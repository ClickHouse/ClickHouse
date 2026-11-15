SET session_timezone = 'UTC';

-- Validates that parseDateTimeBestEffort rejects words that happen to start
-- with month/weekday abbreviations but are not valid date components.
-- For example, "Married" starts with "Mar" (March) but should not be parsed.

-- Invalid words starting with month abbreviations
SELECT parseDateTimeBestEffort('Married'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Marchioness'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Juniper'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Augusto'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Decimal'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Surprise'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Octopus'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Novel'); -- { serverError CANNOT_PARSE_DATETIME }

-- Invalid words starting with weekday abbreviations
SELECT parseDateTimeBestEffort('Monkey'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Fundamental'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Weather'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Thunder'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Fridge'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Satisfy'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffort('Sunflower'); -- { serverError CANNOT_PARSE_DATETIME }

-- Valid abbreviated month names still work
SELECT parseDateTimeBestEffort('Mar 1 2025 00:00:00');
SELECT parseDateTimeBestEffort('Sep 15 2025 00:00:00');
SELECT parseDateTimeBestEffort('Jun 30 2025 00:00:00');

-- Valid full month names still work
SELECT parseDateTimeBestEffort('March 1 2025 00:00:00');
SELECT parseDateTimeBestEffort('September 15 2025 00:00:00');
SELECT parseDateTimeBestEffort('January 1 2025 00:00:00');
SELECT parseDateTimeBestEffort('February 28 2025 00:00:00');

-- Valid abbreviated weekday names still work
SELECT parseDateTimeBestEffort('Mon, 3 Mar 2025 12:00:00');
SELECT parseDateTimeBestEffort('Wed, 5 Mar 2025 12:00:00');

-- Valid full weekday names still work
SELECT parseDateTimeBestEffort('Monday, 3 Mar 2025 12:00:00');
SELECT parseDateTimeBestEffort('Wednesday, 5 Mar 2025 12:00:00');

-- parseDateTimeBestEffortOrNull returns NULL for invalid words
SELECT parseDateTimeBestEffortOrNull('Married');
SELECT parseDateTimeBestEffortOrNull('Monkey');

-- parseDateTimeBestEffortUS also rejects invalid words
SELECT parseDateTimeBestEffortUS('Married'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTimeBestEffortUS('Monkey'); -- { serverError CANNOT_PARSE_DATETIME }
