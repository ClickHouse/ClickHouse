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
    # elif unit == "WEEK":
    #     return rd.relativedelta(days=7 * value)
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


def get_next_refresh_time(schedule, current_time: datetime):
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
                current_time = current_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) + rd.relativedelta(weekday=0, weeks=value)
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
            return (
                current_time - half_offset,
                current_time + half_offset,
            )

        return current_time

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
                interval += rd.relativedelta(weeks=value)
            elif unit == "MONTH":
                interval += rd.relativedelta(months=value)
            elif unit == "YEAR":
                interval += rd.relativedelta(years=value)

        current_time += interval
        if randomize_offset:
            half_offset = (current_time + randomize_offset - current_time) / 2
            return (
                current_time - half_offset,
                # current_time,
                current_time + half_offset,
            )

        return current_time
    raise ValueError("Invalid refresh schedule")
