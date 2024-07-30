from datetime import datetime

from test_refreshable_mat_view.schedule_model import get_next_refresh_time


def test_refresh_schedules():
    time_ = datetime(2000, 1, 1, 1, 1, 1)

    assert get_next_refresh_time(
        "AFTER 2 MONTH 3 DAY RANDOMIZE FOR 1 DAY", time_
    ) == datetime(2000, 1, 1, 1, 1, 1)

    assert get_next_refresh_time("EVERY 1 SECOND", time_) == datetime(
        2000, 1, 1, 1, 1, 2
    )
    assert get_next_refresh_time("EVERY 1 MINUTE", time_) == datetime(
        2000,
        1,
        1,
        1,
        2,
    )
    assert get_next_refresh_time("EVERY 1 HOUR", time_) == datetime(
        2000,
        1,
        1,
        2,
    )
    assert get_next_refresh_time("EVERY 1 DAY", time_) == datetime(2000, 1, 2)
    assert get_next_refresh_time("EVERY 1 WEEK", time_) == datetime(2000, 1, 10)
    assert get_next_refresh_time("EVERY 2 WEEK", time_) == datetime(2000, 1, 17)
    assert get_next_refresh_time("EVERY 1 MONTH", time_) == datetime(2000, 2, 1)
    assert get_next_refresh_time("EVERY 1 YEAR", time_) == datetime(2001, 1, 1)

    assert get_next_refresh_time("EVERY 3 YEAR 4 MONTH 10 DAY", time_) == datetime(
        2003, 5, 11
    )

    # OFFSET
    assert get_next_refresh_time(
        "EVERY 1 MONTH OFFSET 5 DAY 2 HOUR 30 MINUTE 15 SECOND", time_
    ) == datetime(2000, 2, 6, 2, 30, 15)
    assert get_next_refresh_time(
        "EVERY 1 YEAR 2 MONTH OFFSET 5 DAY 2 HOUR 30 MINUTE 15 SECOND", time_
    ) == datetime(2001, 3, 6, 2, 30, 15)

    assert get_next_refresh_time(
        "EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE", time_
    ) == datetime(2000, 1, 22, 15, 10)

    # AFTER
    assert get_next_refresh_time("AFTER 30 SECOND", time_) == datetime(
        2000, 1, 1, 1, 1, 31
    )
    assert get_next_refresh_time("AFTER 30 MINUTE", time_) == datetime(
        2000, 1, 1, 1, 31, 1
    )
    assert get_next_refresh_time("AFTER 2 HOUR", time_) == datetime(2000, 1, 1, 3, 1, 1)
    assert get_next_refresh_time("AFTER 2 DAY", time_) == datetime(2000, 1, 3, 1, 1, 1)
    assert get_next_refresh_time("AFTER 2 WEEK", time_) == datetime(
        2000, 1, 15, 1, 1, 1
    )
    assert get_next_refresh_time("AFTER 2 MONTH", time_) == datetime(
        2000, 3, 1, 1, 1, 1
    )
    assert get_next_refresh_time("AFTER 2 YEAR", time_) == datetime(2002, 1, 1, 1, 1, 1)

    assert get_next_refresh_time("AFTER 2 YEAR 1 MONTH", time_) == datetime(
        2002, 2, 1, 1, 1, 1
    )

    assert get_next_refresh_time("AFTER 1 MONTH 2 YEAR", time_) == datetime(
        2002, 2, 1, 1, 1, 1
    )

    # RANDOMIZE
    next_refresh = get_next_refresh_time(
        "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR", time_
    )

    assert next_refresh == (datetime(2000, 1, 2, 2, 0), datetime(2000, 1, 2, 3, 0))

    next_refresh = get_next_refresh_time(
        "EVERY 2 MONTH 3 DAY 5 HOUR OFFSET 3 HOUR 20 SECOND RANDOMIZE FOR 3 DAY 1 HOUR",
        time_,
    )
    assert next_refresh == (
        datetime(2000, 3, 4, 11, 0, 20),
        datetime(2000, 3, 7, 12, 0, 20),
    )
