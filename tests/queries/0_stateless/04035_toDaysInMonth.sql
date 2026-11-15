-- Basic usage with Date
SELECT toDaysInMonth(toDate('2023-01-15'));  -- January: 31
SELECT toDaysInMonth(toDate('2023-02-01'));  -- February (non-leap): 28
SELECT toDaysInMonth(toDate('2024-02-01'));  -- February (leap year): 29
SELECT toDaysInMonth(toDate('2023-04-30'));  -- April: 30
SELECT toDaysInMonth(toDate('2023-07-04'));  -- July: 31

-- With DateTime
SELECT toDaysInMonth(toDateTime('2023-01-31 12:00:00'));
SELECT toDaysInMonth(toDateTime('2023-02-28 23:59:59'));

-- With DateTime64
SELECT toDaysInMonth(toDateTime64('2024-02-29 00:00:00', 3));

-- With Date32
SELECT toDaysInMonth(toDate32('1900-02-01'));  -- February 1900 (not a leap year): 28
SELECT toDaysInMonth(toDate32('2000-02-01'));  -- February 2000 (leap year): 29

-- Every month has at least 28 days
SELECT toDaysInMonth(today()) > 27;
