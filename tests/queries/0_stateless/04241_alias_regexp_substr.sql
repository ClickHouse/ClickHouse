-- REGEXP_SUBSTR is an Oracle/MySQL/Snowflake alias of regexpExtract.
SELECT REGEXP_SUBSTR('100-200', '(\\d+)-(\\d+)');
SELECT REGEXP_SUBSTR('100-200', '(\\d+)-(\\d+)', 1);
SELECT REGEXP_SUBSTR('100-200', '(\\d+)-(\\d+)', 2);
SELECT regexp_substr('abc123def', '([0-9]+)', 1);
