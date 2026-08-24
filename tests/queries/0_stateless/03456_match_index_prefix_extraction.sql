SET parallel_replicas_local_plan=1;

drop table if exists foo;

CREATE TABLE foo (id UInt8, path String) engine = MergeTree ORDER BY (path) SETTINGS index_granularity=1;

INSERT INTO foo VALUES (1, 'xxx|yyy'),
(2, 'xxx(zzz'),
(3, 'xxx)zzz'),
(4, 'xxx^zzz'),
(5, 'xxx$zzz'),
(6, 'xxx.zzz'),
(7, 'xxx[zzz'),
(8, 'xxx]zzz'),
(9, 'xxx?zzz'),
(10, 'xxx*zzz'),
(11, 'xxx+zzz'),
(12, 'xxx\\zzz'),
(13, 'xxx{zzz'),
(14, 'xxx}zzz'),
(15, 'xxx-zzz');


-- Escaped literal '(' — prefix "xxx(zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\(zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\(zzz') SETTINGS force_primary_key = 1;

-- Escaped literal ')' — prefix "xxx)zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\)zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\)zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '^' — prefix "xxx^zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\^zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\^zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '$' — prefix "xxx$zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\$zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\$zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '.' — prefix "xxx.zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\.zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\.zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '[' — prefix "xxx[zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\[zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\[zzz') SETTINGS force_primary_key = 1;

-- Escaped literal ']' — prefix "xxx]zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\]zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\]zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '?' — prefix "xxx?zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\?zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\?zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '*' — prefix "xxx*zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\*zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\*zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '+' — prefix "xxx+zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\+zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\+zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '\' — prefix "xxx\zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\\\zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\\\zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '{' — prefix "xxx{zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\{zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\{zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '}' — prefix "xxx}zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\}zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\}zzz') SETTINGS force_primary_key = 1;

-- Escaped literal '-' — prefix "xxx-zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\-zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\-zzz') SETTINGS force_primary_key = 1;

-- No optimization: NUL bytes in regex are not supported
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\0bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\0bla') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

-- TODO: Support transparent plain groups — could extract prefix "xxxbla"
-- '(' stops extraction, prefix is "xxx"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx(bla)')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx(bla)') SETTINGS force_primary_key = 1;

-- No optimization beyond "xxx": '[bla]' matches 'b', 'l', or 'a' — not a fixed character
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx[bla]')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx[bla]') SETTINGS force_primary_key = 1;

-- No optimization beyond "xxx": '^' mid-pattern cannot match inside a string
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx^bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx^bla') SETTINGS force_primary_key = 1;

-- No optimization beyond "xxx": '.' matches any character
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx.bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx.bla') SETTINGS force_primary_key = 1;

-- Prefix "xxx": '+' means one-or-more, so the last 'x' is guaranteed
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx+bla')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx+bla') SETTINGS force_primary_key = 1;


-- Prefix "xxx": '{0,1}' allows zero occurrences, so the last 'x' is removed
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx{0,1}')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx{0,1}') SETTINGS force_primary_key = 1;

-- Prefix "xxx": '?' allows zero occurrences, so the last 'x' is removed
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx?')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx?') SETTINGS force_primary_key = 1;

-- Prefix "xxx": '*' allows zero occurrences, so the last 'x' is removed
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx*')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx*') SETTINGS force_primary_key = 1;

-- Prefix "xxx": '\d' is a digit class (not a literal), extraction stops
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\d+')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\d+') SETTINGS force_primary_key = 1;

-- Prefix "xxx": '\w' is a word class (not a literal), extraction stops
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\w+')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\w+') SETTINGS force_primary_key = 1;


-- TODO: Support transparent plain groups — could extract prefix "xxx"
-- No optimization: '(' at start stops extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^(xxx)')) WHERE explain like '%Condition%';

-- TODO: Support transparent plain groups — could extract prefix "xx"
-- No optimization: '(' at start stops extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^(xxx*)')) WHERE explain like '%Condition%';

-- Prefix "xxx": extraction stops at '(', the group is optional but the prefix before it is safe
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx(bla)?')) WHERE explain like '%Condition%';

-- TODO: Support nested transparent groups — could extract prefix "xxx"
-- No optimization: '(' at start stops extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^((xxx))')) WHERE explain like '%Condition%';

-- No optimization: '(?:' is a non-capturing group, extraction stops at '('
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^(?:xxx)')) WHERE explain like '%Condition%';

-- Escaped literal '|' — prefix "xxx|zzz"
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxx\\|zzz')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxx\\|zzz') SETTINGS force_primary_key = 1;

-- No optimization: top-level unescaped '|' means the second branch is unanchored
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM foo WHERE match(path, '^xxxx|foo')) WHERE explain like '%Condition%';
SELECT count() FROM foo WHERE match(path, '^xxxx|foo') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
