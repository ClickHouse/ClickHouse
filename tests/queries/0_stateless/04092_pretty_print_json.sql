-- Tags: no-fasttest

SELECT prettyPrintJSON('{"a":1,"b":"hello"}');
SELECT prettyPrintJSON('{"nested":{"key":"value"}}');
SELECT prettyPrintJSON('[1,2,3]');
SELECT prettyPrintJSON('42');
SELECT prettyPrintJSON('"just a string"');
SELECT prettyPrintJSON('null');
SELECT prettyPrintJSON(NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT prettyPrintJSON('true');
SELECT prettyPrintJSON('{"a":1}', 0);
SELECT prettyPrintJSON('{"a":1,"b":2}', 1);
SELECT prettyPrintJSON('{"a":1}', 32) IS NOT NULL;
-- max indent is 32 to avoid OOM
SELECT prettyPrintJSON('{"a":1}', 33); -- { serverError BAD_ARGUMENTS }
SELECT prettyPrintJSON('not valid json'); -- { serverError BAD_ARGUMENTS }
SELECT prettyPrintJSON('{invalid}'); -- { serverError BAD_ARGUMENTS }
SELECT prettyPrintJSON(''); -- { serverError BAD_ARGUMENTS }
-- Embedded NUL bytes must not silently truncate the input
SELECT prettyPrintJSON(concat('{"a":1}', char(0), 'garbage')); -- { serverError BAD_ARGUMENTS }
SELECT length(prettyPrintJSON(concat(repeat('{"a":', 1000), '1', repeat('}', 1000)))) > 0;