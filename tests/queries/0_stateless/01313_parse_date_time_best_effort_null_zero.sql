SELECT parseDateTimeBestEffort('<Empty>'); -- { serverError 6 }
SELECT parseDateTimeBestEffortOrNull('<Empty>');
SELECT parseDateTimeBestEffortOrZero('<Empty>');

SELECT parseDateTime64BestEffort('<Empty>'); -- { serverError 6 }
SELECT parseDateTime64BestEffortOrNull('<Empty>');
SELECT parseDateTime64BestEffortOrZero('<Empty>');

SET date_time_input_format = 'best_effort';
SELECT toDateTime('<Empty>'); -- { serverError 41 }
SELECT toDateTimeOrNull('<Empty>');
SELECT toDateTimeOrZero('<Empty>');
