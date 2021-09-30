SELECT toDate(toDateTime64(today(), 0, 'UTC')) = toDate(toDateTime(today(), 'UTC'));
