INSERT INTO TABLE FUNCTION file(currentDatabase(), 'JSONColumns', 'c0 Enum(x\'e2\' = 1)') SELECT -1 SETTINGS output_format_json_validate_utf8 = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
