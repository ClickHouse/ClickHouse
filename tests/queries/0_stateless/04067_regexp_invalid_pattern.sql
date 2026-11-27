SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04067_regexp.data', 'CSV', 'x UInt64') VALUES (1);

SELECT * FROM file(currentDatabase() || '_04067_regexp.data', 'Regexp') SETTINGS format_regexp = '\\'; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM file(currentDatabase() || '_04067_regexp.data', 'Regexp', 'x String') SETTINGS format_regexp = '\\'; -- { serverError BAD_ARGUMENTS }
