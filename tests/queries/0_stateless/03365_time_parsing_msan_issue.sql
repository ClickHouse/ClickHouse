SET use_legacy_to_time = false;

SELECT toTime('default', 1); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toTime('-default', 1); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toTime('-1:11:1', 1); -- { serverError CANNOT_PARSE_TEXT }
SELECT toTime('-1:1:11', 1); -- { serverError CANNOT_PARSE_TEXT }
SELECT toTime('111:11:1', 1); -- { serverError CANNOT_PARSE_TEXT }
