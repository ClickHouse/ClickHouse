select parseDateTimeBestEffort('01/12/2017, 18:31:44');
select parseDateTimeBestEffortUS('01/12/2017, 18:31:44');
select parseDateTimeBestEffort('01/12/2017,18:31:44');
select parseDateTimeBestEffortUS('01/12/2017,18:31:44');
select parseDateTimeBestEffort('01/12/2017 ,   18:31:44');
select parseDateTimeBestEffortUS('01/12/2017    ,18:31:44');
select parseDateTimeBestEffortUS('18:31:44, 31/12/2015');
select parseDateTimeBestEffortUS('18:31:44  , 31/12/2015');
select parseDateTimeBestEffort('18:31:44, 31/12/2015');
select parseDateTimeBestEffort('18:31:44  , 31/12/2015');
select parseDateTimeBestEffort('01/12/2017,'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeBestEffortUS('18:31:44,,,, 31/12/2015'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeBestEffortUS('18:31:44, 31/12/2015,'); -- { serverError CANNOT_PARSE_TEXT }
select parseDateTimeBestEffort('01/12/2017, 18:31:44,'); -- { serverError CANNOT_PARSE_TEXT }
select parseDateTimeBestEffort('01/12/2017, ,,,18:31:44'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTimeBestEffort('18:31:44  ,,,,, 31/12/2015'); -- { serverError CANNOT_PARSE_DATETIME }
