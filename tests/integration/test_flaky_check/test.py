import random


def test_flaky():
    assert random.randint(0, 2) >= 1
