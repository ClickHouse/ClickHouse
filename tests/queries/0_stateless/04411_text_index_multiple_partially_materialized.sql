-- Direct read from text index must work when a query uses several text indexes that are
-- only partially materialized (added by ALTER ... ADD INDEX after some data was inserted,
-- so the older parts have no index files).
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/108530:
-- the physical fallback column injected for a not-yet-materialized index is shared between
-- the per-index read steps and deduplicated away from all but the first one, which routed the
-- remaining steps to a text index reader that threw `FILE_DOESNT_EXIST` for the missing file.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS users;

-- 'John'/'1231' goes into a part written before the indexes exist (not materialized),
-- 'Alice'/'1232' goes into a part written after (materialized).
CREATE TABLE users (user String, host String) ENGINE = MergeTree ORDER BY tuple()
AS SELECT 'John', '1231';

ALTER TABLE users
    ADD INDEX user_idx user TYPE text(tokenizer = sparseGrams(3)),
    ADD INDEX host_idx host TYPE text(tokenizer = sparseGrams(3));

SYSTEM STOP MERGES users;
INSERT INTO users VALUES ('Alice', '1232');

SELECT '-- two indexes, both partially materialized';
SELECT 'AND, direct read off', count() FROM users WHERE user = 'John' AND host = '1231' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'AND, direct read on ', count() FROM users WHERE user = 'John' AND host = '1231';
SELECT 'OR,  direct read off', count() FROM users WHERE user = 'John' OR host = '1231' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'OR,  direct read on ', count() FROM users WHERE user = 'John' OR host = '1231';

DROP TABLE users;

SELECT '-- two indexes fully materialized (defined in DDL) give the same results';

DROP TABLE IF EXISTS users_full;

CREATE TABLE users_full
(
    user String,
    host String,
    INDEX user_idx user TYPE text(tokenizer = sparseGrams(3)),
    INDEX host_idx host TYPE text(tokenizer = sparseGrams(3))
)
ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES users_full;
INSERT INTO users_full VALUES ('John', '1231');
INSERT INTO users_full VALUES ('Alice', '1232');

SELECT 'AND', count() FROM users_full WHERE user = 'John' AND host = '1231';
SELECT 'OR ', count() FROM users_full WHERE user = 'John' OR host = '1231';

DROP TABLE users_full;

SELECT '-- three indexes, all partially materialized';

DROP TABLE IF EXISTS triple;

CREATE TABLE triple (a String, b String, c String) ENGINE = MergeTree ORDER BY tuple()
AS SELECT 'alpha', 'beta', 'gamma';

ALTER TABLE triple
    ADD INDEX a_idx a TYPE text(tokenizer = sparseGrams(3)),
    ADD INDEX b_idx b TYPE text(tokenizer = sparseGrams(3)),
    ADD INDEX c_idx c TYPE text(tokenizer = sparseGrams(3));

SYSTEM STOP MERGES triple;
INSERT INTO triple VALUES ('delta', 'epsilon', 'zeta');

SELECT 'AND all three', count() FROM triple WHERE a = 'alpha' AND b = 'beta' AND c = 'gamma';
SELECT 'AND two',       count() FROM triple WHERE a = 'alpha' AND c = 'gamma';
SELECT 'OR all three',  count() FROM triple WHERE a = 'alpha' OR b = 'beta' OR c = 'gamma';

DROP TABLE triple;

SELECT '-- two indexes with hasToken (splitByNonAlpha tokenizer), partially materialized';

DROP TABLE IF EXISTS logs;

CREATE TABLE logs (msg String, tag String) ENGINE = MergeTree ORDER BY tuple()
AS SELECT 'error connection refused', 'net';

ALTER TABLE logs
    ADD INDEX msg_idx msg TYPE text(tokenizer = splitByNonAlpha),
    ADD INDEX tag_idx tag TYPE text(tokenizer = splitByNonAlpha);

SYSTEM STOP MERGES logs;
INSERT INTO logs VALUES ('warning disk full', 'io');

SELECT 'AND', count() FROM logs WHERE hasToken(msg, 'connection') AND hasToken(tag, 'net');
SELECT 'OR ', count() FROM logs WHERE hasToken(msg, 'connection') OR hasToken(tag, 'io');

DROP TABLE logs;
