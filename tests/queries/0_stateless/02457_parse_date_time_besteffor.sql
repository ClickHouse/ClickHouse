select parseDateTimeBestEffort('01/12/2017, 18:31:44');
select parseDateTimeBestEffortUS('01/12/2017, 18:31:44');
select parseDateTimeBestEffort('01/12/2017,18:31:44');
select parseDateTimeBestEffortUS('01/12/2017,18:31:44');
select parseDateTimeBestEffort('01/12/2017,'); -- { serverError CANNOT_PARSE_DATETIME}
