from datetime import datetime, timedelta

import dateutil.relativedelta as rd

"""
It is the model to test the scheduling of refreshable mat view
"""


def relative_offset(unit, value):
    if unit == "SECOND":
        return rd.relativedelta(seconds=value)
    elif unit == "MINUTE":
        return rd.relativedelta(minutes=value)
    elif unit == "HOUR":
        return rd.relativedelta(hours=value)
    elif unit == "DAY":
        return rd.relativedelta(days=value)
    elif unit == "WEEK":
        return rd.relativedelta(weeks=7 * value)
    elif unit == "MONTH":
        return rd.relativedelta(months=value)
    elif unit == "YEAR":
        return rd.relativedelta(years=value)

    raise Exception("Can't parse unit: {}".format(unit))


def group_and_sort(parts, reverse=False):
    order = ["YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"]
    grouped_parts = []

    for i in range(0, len(parts), 2):
        grouped_parts.append((parts[i], parts[i + 1]))

    sorted_parts = sorted(
        grouped_parts, key=lambda x: order.index(x[1]), reverse=reverse
    )
    return sorted_parts


def get_next_refresh_time(schedule, current_time: datetime, first_week=False):
    parts = schedule.split()
    randomize_offset = timedelta()

    if "RANDOMIZE" in parts:
        randomize_index = parts.index("RANDOMIZE")
        randomize_parts = parts[randomize_index + 2 :]

        for i in range(0, len(randomize_parts), 2):
            value = int(randomize_parts[i])
            randomize_offset += relative_offset(randomize_parts[i + 1], value)

        parts = parts[:randomize_index]

    offset = timedelta()
    if "OFFSET" in parts:
        offset_index = parts.index("OFFSET")
        for i in range(offset_index + 1, len(parts), 2):
            value = int(parts[i])
            offset += relative_offset(parts[i + 1], value)

        parts = parts[:offset_index]

    week_in_primary = False
    if parts[0] == "EVERY":
        parts = group_and_sort(parts[1:])
        for part in parts:
            value = int(part[0])
            unit = part[1]

            if unit == "SECOND":
                current_time = current_time.replace(microsecond=0) + rd.relativedelta(
                    seconds=value
                )
            elif unit == "MINUTE":
                current_time = current_time.replace(
                    second=0, microsecond=0
                ) + rd.relativedelta(minutes=value)
            elif unit == "HOUR":
                current_time = current_time.replace(
                    minute=0, second=0, microsecond=0
                ) + rd.relativedelta(hours=value)
            elif unit == "DAY":
                current_time = current_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) + rd.relativedelta(days=value)
            elif unit == "WEEK":
                week_in_primary = True
                current_time = current_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) + rd.relativedelta(weekday=0, weeks=0 if first_week else value)
            elif unit == "MONTH":
                current_time = current_time.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                ) + rd.relativedelta(months=value)
            elif unit == "YEAR":
                current_time = current_time.replace(
                    month=1, day=1, hour=0, minute=0, second=0, microsecond=0
                ) + rd.relativedelta(years=value)

        current_time += offset
        if randomize_offset:
            half_offset = (current_time + randomize_offset - current_time) / 2
            return {
                "type": "randomize",
                "time": (
                    current_time - half_offset,
                    current_time + half_offset,
                ),
                "week": week_in_primary,
            }

        return {"type": "regular", "time": current_time, "week": week_in_primary}

    elif parts[0] == "AFTER":
        parts = group_and_sort(parts[1:], reverse=True)
        interval = rd.relativedelta()
        for part in parts:
            value = int(part[0])
            unit = part[1]

            if unit == "SECOND":
                interval += rd.relativedelta(seconds=value)
            elif unit == "MINUTE":
                interval += rd.relativedelta(minutes=value)
            elif unit == "HOUR":
                interval += rd.relativedelta(hours=value)
            elif unit == "DAY":
                interval += rd.relativedelta(days=value)
            elif unit == "WEEK":
                week_in_primary = True
                interval += rd.relativedelta(weeks=value)
            elif unit == "MONTH":
                interval += rd.relativedelta(months=value)
            elif unit == "YEAR":
                interval += rd.relativedelta(years=value)

        current_time += interval
        if randomize_offset:
            half_offset = (current_time + randomize_offset - current_time) / 2
            return {
                "type": "randomize",
                "time": (
                    current_time - half_offset,
                    current_time + half_offset,
                ),
                "week": week_in_primary,
            }

        return {"type": "regular", "time": current_time, "week": week_in_primary}
    raise ValueError("Invalid refresh schedule")


def compare_dates(
    date1: str | datetime,
    date2: dict,
    first_week=False,
):
    """
    Special logic for weeks for first refresh:
    The desired behavior for EVERY 1 WEEK is "every Monday". This has the properties: (a) it doesn't depend on when the materialized view was created, (b) consecutive refreshes are exactly 1 week apart. And non-properties: (c) the first refresh doesn't happen exactly 1 week after view creation, it can be anywhere between 0 and 1 week, (d) the schedule is not aligned with months or years.
    I would expect EVERY 2 WEEK to have the same two properties and two non-properties, and also to fall on Mondays. There are exactly two possible ways to achieve that: all even-numbered Mondays or all odd-numbered Mondays. I just picked one.
    """
    weeks = []
    if date2["week"] and first_week:
        for i in [0, 1, 2]:
            if date2["type"] == "randomize":
                weeks.append(
                    (
                        date2["time"][0] + rd.relativedelta(weeks=i),
                        date2["time"][1] + rd.relativedelta(weeks=i),
                    )
                )
            else:
                weeks.append(date2["time"] + rd.relativedelta(weeks=i))

        for week in weeks:
            if compare_dates_(date1, week):
                return True
        raise ValueError("Invalid week")
    else:
        assert compare_dates_(date1, date2["time"])


def compare_dates_(
    date1: str | datetime,
    date2: str | datetime | tuple[datetime],
    inaccuracy=timedelta(minutes=10),
    format_str="%Y-%m-%d %H:%M:%S",
) -> bool:
    """
    Compares two dates with small inaccuracy.
    """
    if isinstance(date1, str):
        date1 = datetime.strptime(date1, format_str)
    if isinstance(date2, str):
        date2 = datetime.strptime(date2, format_str)

    if isinstance(date2, datetime):
        return abs(date1 - date2) <= inaccuracy
    else:
        return date2[0] - inaccuracy <= date1 <= date2[1] + inaccuracy
