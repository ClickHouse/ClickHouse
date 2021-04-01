SELECT parseDateTimeBestEffort('<Empty>'); -- { serverError 41 }
SELECT parseDateTimeBestEffortOrNull('<Empty>');
SELECT parseDateTimeBestEffortOrZero('<Empty>', 'UTC');

SELECT parseDateTime64BestEffort('<Empty>'); -- { serverError 41 }
SELECT parseDateTime64BestEffortOrNull('<Empty>');
SELECT parseDateTime64BestEffortOrZero('<Empty>', 0, 'UTC');

SET date_time_input_format = 'best_effort';
SELECT toDateTime('<Empty>'); -- { serverError 41 }
SELECT toDateTimeOrNull('<Empty>');
SELECT toDateTimeOrZero('<Empty>', 'UTC');
