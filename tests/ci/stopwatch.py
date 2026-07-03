#!/usr/bin/env python3

import datetime


class Stopwatch:
    def __init__(self):
        self.reset()

    @property
    def duration_seconds(self) -> float:
        return (
            datetime.datetime.now(datetime.timezone.utc) - self.start_time
        ).total_seconds()

    @property
    def start_time_str(self) -> str:
        return self.start_time_str_value

    def reset(self) -> None:
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.start_time_str_value = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
