SELECT toTime('default', 1) SETTINGS use_legacy_to_time = false; -- { serverError CANNOT_PARSE_TEXT }
SELECT toTime('-default', 1) SETTINGS use_legacy_to_time = false; -- { serverError CANNOT_PARSE_TEXT }
