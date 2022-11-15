#!/usr/bin/env python3

import datetime


class Stopwatch:
    def __init__(self):
        self.start_time = datetime.datetime.utcnow()
        self.start_time_str_value = self.start_time.strftime("%Y-%m-%d %H:%M:%S")

    @property
    def duration_seconds(self):
        return (datetime.datetime.utcnow() - self.start_time).total_seconds()

    @property
    def start_time_str(self):
        return self.start_time_str_value
