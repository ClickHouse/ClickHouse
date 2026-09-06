-- `optimize_rewrite_regexp_functions` strips a trailing `.*$` from the pattern of `extract`, which is
-- sound only when the pattern has a capture group - `extract` returns group 1 then, and the group
-- precedes the stripped tail. The analyzer counted a flag group as a capture group, so for a pattern
-- whose only parentheses are `(?i)` or `(?i:...)` the strip fired while `extract` was returning the
-- whole match, and the result was silently truncated.

SELECT 'a pure flag group';
SELECT extract(materialize('aBcd'), '(?i)b.*$'), extract(materialize('aBcd'), '(?i)b.*$') SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT extract(materialize('abcd'), '(?s)b.*$'), extract(materialize('abcd'), '(?s)b.*$') SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT regexpExtract(materialize('aBcd'), '(?i)b.*$', 0);

SELECT 'a scoped flag group';
SELECT extract(materialize('aBcd'), '(?i:b).*$'), extract(materialize('aBcd'), '(?i:b).*$') SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT regexpExtract(materialize('aBcd'), '(?i:b).*$', 0);

SELECT 'a real capture group still allows the rewrite';
SELECT extract(materialize('abcd'), '(b).*$'), extract(materialize('abcd'), '(b).*$') SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT extract(materialize('abcd'), '(?i)(b).*$'), extract(materialize('abcd'), '(?i)(b).*$') SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT 'and the pattern without the tail is unchanged';
SELECT extract(materialize('aBcd'), '(?i)b'), extract(materialize('abcd'), '(b)c');

SELECT 'the same over a table';
DROP TABLE IF EXISTS t_extract_flag_group;
CREATE TABLE t_extract_flag_group (h String) ENGINE = MergeTree ORDER BY h;
INSERT INTO t_extract_flag_group VALUES ('aBcd'), ('abcd'), ('zz');

SELECT h, extract(h, '(?i)b.*$') FROM t_extract_flag_group ORDER BY h;
SELECT h, extract(h, '(?i)b.*$') FROM t_extract_flag_group ORDER BY h SETTINGS optimize_rewrite_regexp_functions = 0;

DROP TABLE t_extract_flag_group;
