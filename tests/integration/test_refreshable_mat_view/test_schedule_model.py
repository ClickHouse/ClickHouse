from datetime import datetime

from test_refreshable_mat_view.schedule_model import get_next_refresh_time


def get_next_refresh_time_(*args, **kwargs):
    return get_next_refresh_time(*args, **kwargs)["time"]


def test_refresh_schedules():
    t = datetime(2000, 1, 1, 1, 1, 1)

    assert get_next_refresh_time_("EVERY 1 SECOND", t) == datetime(2000, 1, 1, 1, 1, 2)
    assert get_next_refresh_time_("EVERY 1 MINUTE", t) == datetime(
        2000,
        1,
        1,
        1,
        2,
    )
    assert get_next_refresh_time_("EVERY 1 HOUR", t) == datetime(
        2000,
        1,
        1,
        2,
    )
    assert get_next_refresh_time_("EVERY 1 DAY", t) == datetime(2000, 1, 2)
    assert get_next_refresh_time_("EVERY 1 WEEK", t) == datetime(2000, 1, 10)
    assert get_next_refresh_time_("EVERY 2 WEEK", t) == datetime(2000, 1, 17)
    assert get_next_refresh_time_("EVERY 1 MONTH", t) == datetime(2000, 2, 1)
    assert get_next_refresh_time_("EVERY 1 YEAR", t) == datetime(2001, 1, 1)

    assert get_next_refresh_time_("EVERY 3 YEAR 4 MONTH 10 DAY", t) == datetime(
        2003, 5, 11
    )

    # OFFSET
    assert get_next_refresh_time_(
        "EVERY 1 MONTH OFFSET 5 DAY 2 HOUR 30 MINUTE 15 SECOND", t
    ) == datetime(2000, 2, 6, 2, 30, 15)
    assert get_next_refresh_time_(
        "EVERY 1 YEAR 2 MONTH OFFSET 5 DAY 2 HOUR 30 MINUTE 15 SECOND", t
    ) == datetime(2001, 3, 6, 2, 30, 15)

    assert get_next_refresh_time_(
        "EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE", t
    ) == datetime(2000, 1, 22, 15, 10)

    # AFTER
    assert get_next_refresh_time_("AFTER 30 SECOND", t) == datetime(
        2000, 1, 1, 1, 1, 31
    )
    assert get_next_refresh_time_("AFTER 30 MINUTE", t) == datetime(
        2000, 1, 1, 1, 31, 1
    )
    assert get_next_refresh_time_("AFTER 2 HOUR", t) == datetime(2000, 1, 1, 3, 1, 1)
    assert get_next_refresh_time_("AFTER 2 DAY", t) == datetime(2000, 1, 3, 1, 1, 1)
    assert get_next_refresh_time_("AFTER 2 WEEK", t) == datetime(2000, 1, 15, 1, 1, 1)
    assert get_next_refresh_time_("AFTER 2 MONTH", t) == datetime(2000, 3, 1, 1, 1, 1)
    assert get_next_refresh_time_("AFTER 2 YEAR", t) == datetime(2002, 1, 1, 1, 1, 1)

    assert get_next_refresh_time_("AFTER 2 YEAR 1 MONTH", t) == datetime(
        2002, 2, 1, 1, 1, 1
    )

    assert get_next_refresh_time_("AFTER 1 MONTH 2 YEAR", t) == datetime(
        2002, 2, 1, 1, 1, 1
    )

    # RANDOMIZE
    next_refresh = get_next_refresh_time_(
        "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR", t
    )

    assert next_refresh == (datetime(2000, 1, 2, 1, 30), datetime(2000, 1, 2, 2, 30))

    next_refresh = get_next_refresh_time_(
        "EVERY 2 MONTH 3 DAY 5 HOUR OFFSET 3 HOUR 20 SECOND RANDOMIZE FOR 3 DAY 1 HOUR",
        t,
    )
    assert next_refresh == (
        datetime(2000, 3, 2, 19, 30, 20),
        datetime(2000, 3, 5, 20, 30, 20),
    )

    assert get_next_refresh_time_("AFTER 2 MONTH 3 DAY RANDOMIZE FOR 1 DAY", t) == (
        datetime(2000, 3, 3, 13, 1, 1),
        datetime(2000, 3, 4, 13, 1, 1),
    )
