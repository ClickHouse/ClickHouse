from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

"""
It is the model to test the scheduling of refreshable mat view
"""


def relative_offset(unit, value):
    if unit == "SECONDS":
        return relativedelta(seconds=value)
    elif unit == "MINUTE":
        return relativedelta(minutes=value)
    elif unit == "HOUR":
        return relativedelta(hours=value)
    elif unit == "DAY":
        return relativedelta(days=value)
    elif unit == "WEEK":
        return relativedelta(days=7 * value)
    elif unit == "MONTH":
        return relativedelta(months=value)
    elif unit == "YEAR":
        return relativedelta(years=value)

    raise Exception("Cant parse unit: {}".format(unit))


def group_and_sort(parts, reverse=False):
    order = ["YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECONDS"]
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

            if unit == "SECONDS":
                current_time = current_time.replace(microsecond=0) + relativedelta(
                    seconds=value
                )
            elif unit == "MINUTE":
                current_time = current_time.replace(
                    second=0, microsecond=0
                ) + relativedelta(minutes=value)
            elif unit == "HOUR":
                current_time = current_time.replace(
                    minute=0, second=0, microsecond=0
                ) + relativedelta(hours=value)
            elif unit == "DAY":
                current_time = current_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) + relativedelta(days=value)
            elif unit == "WEEK":
                current_time = current_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) + relativedelta(weekday=0, weeks=value)
            elif unit == "MONTH":
                current_time = current_time.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                ) + relativedelta(months=value)
            elif unit == "YEAR":
                current_time = current_time.replace(
                    month=1, day=1, hour=0, minute=0, second=0, microsecond=0
                ) + relativedelta(years=value)

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
        interval = relativedelta()
        for part in parts:
            value = int(part[0])
            unit = part[1]

            if unit == "SECONDS":
                interval += relativedelta(seconds=value)
            elif unit == "MINUTE":
                interval += relativedelta(minutes=value)
            elif unit == "HOUR":
                interval += relativedelta(hours=value)
            elif unit == "DAY":
                interval += relativedelta(days=value)
            elif unit == "WEEK":
                interval += relativedelta(weeks=value)
            elif unit == "MONTH":
                interval += relativedelta(months=value)
            elif unit == "YEAR":
                interval += relativedelta(years=value)

        current_time += interval
        if randomize_offset:
            half_offset = (current_time + randomize_offset - current_time) / 2
            return (
                current_time - half_offset,
                current_time + half_offset,
            )

        return current_time
    raise ValueError("Invalid refresh schedule")
