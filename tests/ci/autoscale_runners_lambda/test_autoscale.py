#!/usr/bin/env python

import unittest
from dataclasses import dataclass
from typing import Any, List

from app import Queue, set_capacity


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
            """a one-time property"""
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
            TestCase("reserve", 1, 13, 20, [Queue("queued", 13, "reserve")], -1),
            # Increase capacity
            TestCase(
                "increase-always",
                1,
                13,
                20,
                [Queue("queued", 14, "increase-always")],
                14,
            ),
            TestCase("increase-1", 1, 13, 20, [Queue("queued", 23, "increase-1")], 17),
            TestCase(
                "style-checker", 1, 13, 20, [Queue("queued", 19, "style-checker")], 19
            ),
            TestCase("increase-2", 1, 13, 20, [Queue("queued", 18, "increase-2")], 15),
            TestCase("increase-3", 1, 13, 20, [Queue("queued", 183, "increase-3")], 20),
            TestCase(
                "increase-w/o reserve",
                1,
                13,
                20,
                [
                    Queue("in_progress", 11, "increase-w/o reserve"),
                    Queue("queued", 12, "increase-w/o reserve"),
                ],
                17,
            ),
            TestCase("lower-min", 10, 5, 20, [Queue("queued", 5, "lower-min")], 10),
            # Decrease capacity
            TestCase("w/reserve", 1, 13, 20, [Queue("queued", 5, "w/reserve")], 5),
            TestCase(
                "style-checker", 1, 13, 20, [Queue("queued", 5, "style-checker")], 5
            ),
            TestCase("w/reserve", 1, 23, 20, [Queue("queued", 17, "w/reserve")], 17),
            TestCase("decrease", 1, 13, 20, [Queue("in_progress", 3, "decrease")], 3),
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

    def test_effective_capacity(self):
        """Normal cases test increasing w/o considering
        effective_capacity much lower than DesiredCapacity"""
        test_cases = (
            TestCase(
                "desired-overwritten",
                1,
                20,  # DesiredCapacity, overwritten by effective_capacity
                50,
                [
                    Queue("in_progress", 30, "desired-overwritten"),
                    Queue("queued", 60, "desired-overwritten"),
                ],
                40,
            ),
        )
        for t in test_cases:
            self.client.data_helper(t.name, t.min_size, t.desired_capacity, t.max_size)
            # we test that effective_capacity is 30 (a half of 60)
            data_with_instances = self.client.expected_data
            data_with_instances["AutoScalingGroups"][0]["Instances"] = [
                {"HealthStatus": "Healthy" if i % 2 else "Unhealthy"} for i in range(60)
            ]
            self.client.expected_data = data_with_instances
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
