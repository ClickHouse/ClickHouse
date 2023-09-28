#!/usr/bin/env python

import unittest
from dataclasses import dataclass
from typing import Any, List

from app import set_capacity, Queue


@dataclass
class TestCase:
    name: str
    min_size: int
    desired_capacity: int
    max_size: int
    queues: List[Queue]
    expected_capacity: int


class TestSetCapacity(unittest.TestCase):
    class FakeClient:
        def __init__(self):
            self._expected_data = {}  # type: dict
            self._expected_capacity = -1

        @property
        def expected_data(self) -> dict:
            """a one-time property"""
            data, self._expected_data = self._expected_data, {}
            return data

        @expected_data.setter
        def expected_data(self, value: dict) -> None:
            self._expected_data = value

        @property
        def expected_capacity(self) -> int:
            """one-time property"""
            capacity, self._expected_capacity = self._expected_capacity, -1
            return capacity

        def describe_auto_scaling_groups(self, **kwargs: Any) -> dict:
            _ = kwargs
            return self.expected_data

        def set_desired_capacity(self, **kwargs: Any) -> None:
            self._expected_capacity = kwargs["DesiredCapacity"]

        def data_helper(
            self, name: str, min_size: int, desired_capacity: int, max_size: int
        ) -> None:
            self.expected_data = {
                "AutoScalingGroups": [
                    {
                        "AutoScalingGroupName": name,
                        "DesiredCapacity": desired_capacity,
                        "MinSize": min_size,
                        "MaxSize": max_size,
                        "Instances": [],  # necessary for ins["HealthStatus"] check
                    }
                ]
            }

    def setUp(self):
        self.client = self.FakeClient()

    def test_normal_cases(self):
        test_cases = (
            # Do not change capacity
            TestCase("noqueue", 1, 13, 20, [Queue("in_progress", 155, "noqueue")], -1),
            TestCase("w/reserve", 1, 13, 20, [Queue("queued", 17, "w/reserve")], -1),
            # Increase capacity
            TestCase("increase", 1, 13, 20, [Queue("queued", 23, "increase")], 15),
            TestCase(
                "style-checker", 1, 13, 20, [Queue("queued", 33, "style-checker")], 16
            ),
            TestCase("increase", 1, 13, 20, [Queue("queued", 18, "increase")], 14),
            TestCase("increase", 1, 13, 20, [Queue("queued", 183, "increase")], 20),
            TestCase(
                "increase-w/o reserve",
                1,
                13,
                20,
                [
                    Queue("in_progress", 11, "increase-w/o reserve"),
                    Queue("queued", 12, "increase-w/o reserve"),
                ],
                15,
            ),
            TestCase("lower-min", 10, 5, 20, [Queue("queued", 5, "lower-min")], 10),
            # Decrease capacity
            TestCase("w/reserve", 1, 13, 20, [Queue("queued", 5, "w/reserve")], 9),
            TestCase(
                "style-checker", 1, 13, 20, [Queue("queued", 5, "style-checker")], 5
            ),
            TestCase("w/reserve", 1, 23, 20, [Queue("queued", 17, "w/reserve")], 20),
            TestCase("decrease", 1, 13, 20, [Queue("in_progress", 3, "decrease")], 8),
            TestCase(
                "style-checker",
                1,
                13,
                20,
                [Queue("in_progress", 5, "style-checker")],
                5,
            ),
        )
        for t in test_cases:
            self.client.data_helper(t.name, t.min_size, t.desired_capacity, t.max_size)
            set_capacity(t.name, t.queues, self.client, False)
            self.assertEqual(t.expected_capacity, self.client.expected_capacity, t.name)

    def test_exceptions(self):
        test_cases = (
            (
                TestCase(
                    "different names",
                    1,
                    1,
                    1,
                    [Queue("queued", 5, "another name")],
                    -1,
                ),
                AssertionError,
            ),
            (TestCase("wrong queue len", 1, 1, 1, [], -1), AssertionError),
            (
                TestCase(
                    "wrong queue", 1, 1, 1, [Queue("wrong", 1, "wrong queue")], -1  # type: ignore
                ),
                ValueError,
            ),
        )
        for t, error in test_cases:
            with self.assertRaises(error):
                self.client.data_helper(
                    t.name, t.min_size, t.desired_capacity, t.max_size
                )
                set_capacity(t.name, t.queues, self.client, False)

        with self.assertRaises(AssertionError):
            self.client.expected_data = {"AutoScalingGroups": [1, 2]}
            set_capacity(
                "wrong number of ASGs",
                [Queue("queued", 1, "wrong number of ASGs")],
                self.client,
            )
