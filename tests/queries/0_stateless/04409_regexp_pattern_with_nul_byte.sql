-- A NUL byte (\0) in a regular expression must be treated as an ordinary literal byte
-- (RE2 is binary-safe), not as the end of the pattern. Regression for the analyzer in
-- OptimizedRegularExpression, which used to stop at the first NUL and then let the strstr
-- pre-filter drop every row.

-- Leading `\0*` (zero or more NUL bytes) is optional, so the rest of the pattern must still match.
SELECT extract('test@clickhouse.com', '\0*@(.*)$');
SELECT match('test@clickhouse.com', '\0*@.*');
SELECT replaceRegexpAll('a@b@c', '\0*@', '#');
SELECT arrayStringConcat(extractAll('test@x.com', '\0*([a-z]+)'), ',');

-- A literal NUL in the pattern matches a literal NUL in the data, and only that.
SELECT match('xa\0by', 'a\0b');
SELECT match('xayb', 'a\0b');
SELECT hex(extract('za\0bz', '(a\0b)'));
