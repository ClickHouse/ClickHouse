
-- Timezone argument: UTC Jan 31 23:30 is Feb 1 in Asia/Tokyo (2024 is leap year, Feb has 29 days)
SELECT toDaysInMonth(toDateTime('2024-01-31 23:30:00', 'UTC'), 'Asia/Tokyo') ORDER BY 1;

-- Without timezone override: stays in January (31 days)
SELECT toDaysInMonth(toDateTime('2024-01-31 23:30:00', 'UTC')) ORDER BY 1;

-- Month boundary: UTC Feb 29 15:00 is Mar 1 in Pacific/Auckland (+13)
SELECT toDaysInMonth(toDateTime('2024-02-29 15:00:00', 'UTC'), 'Pacific/Auckland') ORDER BY 1;
SELECT toDaysInMonth(toDateTime('2024-02-29 15:00:00', 'UTC')) ORDER BY 1;

-- Nullable input
SELECT toDaysInMonth(CAST(NULL AS Nullable(Date))) ORDER BY 1;
SELECT toDaysInMonth(CAST(NULL AS Nullable(DateTime))) ORDER BY 1;
SELECT toDaysInMonth(toNullable(toDate('2024-02-15'))) ORDER BY 1;

-- LowCardinality input
SELECT toDaysInMonth(toLowCardinality(toDate('2024-02-15'))) ORDER BY 1;
