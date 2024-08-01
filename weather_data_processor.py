import logging
import re
from data_ingestion import read_from_web_CSV
import numpy as np
import pandas as pd


class WeatherDataProcessor:
    """
    Process weather data.

    Attributes:
        weather_station_data (str): Path to the weather station data CSV file.
        patterns (dict): Regular expression patterns for extracting measurements.
        weather_df (DataFrame): DataFrame to store weather data.
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize WeatherDataProcessor.

        Args:
            config_params (dict): Configuration parameters.
            logging_level (str, optional): Logging level. Defaults to "INFO".
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Initialize logging for WeatherDataProcessor.

        Args:
            logging_level (str): Logging level.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Map weather station data.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.")

    def extract_measurement(self, message):
        """
        Extract measurement from message.

        Args:
            message (str): Message containing measurement.

        Returns:
            tuple: Measurement key and value.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Process messages to extract measurements.

        Returns:
            DataFrame: Processed weather data.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
        Calculate means of weather measurements.

        Returns:
            DataFrame: Mean values of weather measurements.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None

    def process(self):
        """
        Process weather data.
        """
        self.weather_station_mapping()
        self.process_messages()
        self.logger.info("Data processing completed.")
