select parseDateTime64BestEffort('2.55'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64BestEffortOrNull('2.55');
