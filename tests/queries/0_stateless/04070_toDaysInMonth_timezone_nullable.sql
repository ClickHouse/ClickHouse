-- Tags: no-msan

-- Timezone argument: UTC Jan 31 23:30 is Feb 1 in Asia/Tokyo (2024 is leap year, Feb has 29 days)
SELECT toDaysInMonth(toDateTime('2024-01-31 23:30:00', 'UTC'), 'Asia/Tokyo');

-- Without timezone override: stays in January (31 days)
SELECT toDaysInMonth(toDateTime('2024-01-31 23:30:00', 'UTC'));

-- Month boundary: UTC Feb 29 15:00 is Mar 1 in Pacific/Auckland (+13)
SELECT toDaysInMonth(toDateTime('2024-02-29 15:00:00', 'UTC'), 'Pacific/Auckland');
SELECT toDaysInMonth(toDateTime('2024-02-29 15:00:00', 'UTC'));

-- Nullable input
SELECT toDaysInMonth(CAST(NULL AS Nullable(Date)));
SELECT toDaysInMonth(CAST(NULL AS Nullable(DateTime)));
SELECT toDaysInMonth(toNullable(toDate('2024-02-15')));

-- LowCardinality input
SELECT toDaysInMonth(toLowCardinality(toDate('2024-02-15')));
