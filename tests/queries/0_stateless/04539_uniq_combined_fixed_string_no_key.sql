-- Test uniqCombined/uniqCombined64 batched no-key path over FixedString columns.
DROP TABLE IF EXISTS t_uniq_fs;
CREATE TABLE t_uniq_fs (s FixedString(8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uniq_fs SELECT toFixedString(concat('s', toString(number % 100)), 8) FROM numbers(5000);

SELECT 'fixedstring cardinality, no key';
SELECT uniqCombined(s), uniqCombined64(s), uniqCombined(12)(s), uniqCombined64(12)(s) FROM t_uniq_fs;

SELECT 'fixedstring matches uniqExact';
SELECT uniqCombined(s) = uniqExact(s), uniqCombined64(s) = uniqExact(s) FROM t_uniq_fs;

DROP TABLE t_uniq_fs;

-- Values that share the same prefix up to the first \0 padding byte and differ only afterwards
-- (bytes are 'p' \0 <distinct> \0 \0 ...). A regression that hashes FixedString as a NUL-terminated
-- string, or ignores bytes past the first zero, would collapse all rows to one value here.
DROP TABLE IF EXISTS t_uniq_fs_nul;
CREATE TABLE t_uniq_fs_nul (s FixedString(8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uniq_fs_nul SELECT toFixedString(char(112, 0, number % 100 + 1), 8) FROM numbers(5000);

SELECT 'fixedstring with embedded null cardinality, no key';
SELECT uniqCombined(s), uniqCombined64(s), uniqCombined(12)(s), uniqCombined64(12)(s) FROM t_uniq_fs_nul;

SELECT 'fixedstring with embedded null matches uniqExact';
SELECT uniqCombined(s) = uniqExact(s), uniqCombined64(s) = uniqExact(s) FROM t_uniq_fs_nul;

DROP TABLE t_uniq_fs_nul;
