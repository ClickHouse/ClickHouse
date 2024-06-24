select parseDateTime64BestEffort('2.55'); -- { serverError 41 }
select parseDateTime64BestEffortOrNull('2.55');
