-- A `FixedString` value is stored NUL padded, and comparisons ignore that padding while the tokenizer does not, so a
-- probe filter can be built over tokens the granule never saw. Each correctness row prints the count from an unindexed
-- `Log` twin next to the count from the indexed table: they must agree.

SET use_skip_indexes = 1;
-- Without this the map predicates are rewritten into subcolumn reads, which do not reach the index code under test.
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS fs_idx;
DROP TABLE IF EXISTS fs_log;
CREATE TABLE fs_idx (id UInt64, v Array(FixedString(8)), s FixedString(8), mk Map(FixedString(8), String), mv Map(String, FixedString(8)),
    INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_mk mapKeys(mk) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_mv mapValues(mv) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE fs_log (id UInt64, v Array(FixedString(8)), s FixedString(8), mk Map(FixedString(8), String), mv Map(String, FixedString(8))) ENGINE = Log;
INSERT INTO fs_idx SELECT number, [w], w, map(w, 'x'), map('k', w) FROM (SELECT number, if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) AS w FROM numbers(64));
INSERT INTO fs_log SELECT number, [w], w, map(w, 'x'), map('k', w) FROM (SELECT number, if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) AS w FROM numbers(64));

-- A constant wider than the column carries padding the comparison drops. `equals`, `hasAny` and `hasAll` were already
-- handled; the membership arms are the ones that pruned the matching granule.
SELECT 'has wide const', (SELECT count() FROM fs_log WHERE has(v, toFixedString('VALUE0', 10))), (SELECT count() FROM fs_idx WHERE has(v, toFixedString('VALUE0', 10)));
SELECT 'hasAny wide const', (SELECT count() FROM fs_log WHERE hasAny(v, [toFixedString('VALUE0', 10)])), (SELECT count() FROM fs_idx WHERE hasAny(v, [toFixedString('VALUE0', 10)]));
SELECT 'hasAll wide const', (SELECT count() FROM fs_log WHERE hasAll(v, [toFixedString('VALUE0', 10)])), (SELECT count() FROM fs_idx WHERE hasAll(v, [toFixedString('VALUE0', 10)]));
SELECT 'mapContainsKey wide const', (SELECT count() FROM fs_log WHERE mapContainsKey(mk, toFixedString('VALUE0', 10))), (SELECT count() FROM fs_idx WHERE mapContainsKey(mk, toFixedString('VALUE0', 10)));
SELECT 'mapContains wide const', (SELECT count() FROM fs_log WHERE mapContains(mk, toFixedString('VALUE0', 10))), (SELECT count() FROM fs_idx WHERE mapContains(mk, toFixedString('VALUE0', 10)));
SELECT 'has mapKeys wide const', (SELECT count() FROM fs_log WHERE has(mapKeys(mk), toFixedString('VALUE0', 10))), (SELECT count() FROM fs_idx WHERE has(mapKeys(mk), toFixedString('VALUE0', 10)));
SELECT 'mapContainsValue wide const', (SELECT count() FROM fs_log WHERE mapContainsValue(mv, toFixedString('VALUE0', 10))), (SELECT count() FROM fs_idx WHERE mapContainsValue(mv, toFixedString('VALUE0', 10)));
SELECT 'equals wide const', (SELECT count() FROM fs_log WHERE s = toFixedString('VALUE0', 10)), (SELECT count() FROM fs_idx WHERE s = toFixedString('VALUE0', 10));

-- The same defect with a `String` constant that carries the NUL tail itself, which a guard keyed on the constant's type
-- would miss. `notEquals` is spelled under a `NOT` because a positive `!=` atom never prunes.
SELECT 'equals String const', (SELECT count() FROM fs_log WHERE s = 'VALUE0\0\0\0\0'), (SELECT count() FROM fs_idx WHERE s = 'VALUE0\0\0\0\0');
SELECT 'notEquals String const', (SELECT count() FROM fs_log WHERE NOT (s != 'VALUE0\0\0\0\0')), (SELECT count() FROM fs_idx WHERE NOT (s != 'VALUE0\0\0\0\0'));

-- The stored value ends `C2 00`, so trimming the probe leaves it on a UTF-8 lead byte. The gram tokenizers step by
-- `seqLength` and clamp the last gram to the buffer end, which turns `5545C200` in the granule into a probe `5545C2`.
DROP TABLE IF EXISTS u_idx;
DROP TABLE IF EXISTS u_log;
CREATE TABLE u_idx (id UInt64, v Array(FixedString(8)), s FixedString(8), mk Map(FixedString(8), String),
    INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_mk mapKeys(mk) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE u_log (id UInt64, v Array(FixedString(8)), s FixedString(8), mk Map(FixedString(8), String)) ENGINE = Log;
INSERT INTO u_idx SELECT number, [w], w, map(w, 'x') FROM (SELECT number, if(number = 7, toFixedString(concat('VALUE', unhex('C2')), 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) AS w FROM numbers(64));
INSERT INTO u_log SELECT number, [w], w, map(w, 'x') FROM (SELECT number, if(number = 7, toFixedString(concat('VALUE', unhex('C2')), 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) AS w FROM numbers(64));

SELECT 'equals FixedString const, UTF-8 tail', (SELECT count() FROM u_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)), (SELECT count() FROM u_idx WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8));
SELECT 'equals String const, UTF-8 tail', (SELECT count() FROM u_log WHERE s = concat('VALUE', unhex('C2'))), (SELECT count() FROM u_idx WHERE s = concat('VALUE', unhex('C2')));
SELECT 'has String const, UTF-8 tail', (SELECT count() FROM u_log WHERE has(v, concat('VALUE', unhex('C2')))), (SELECT count() FROM u_idx WHERE has(v, concat('VALUE', unhex('C2'))));
SELECT 'hasAny String const, UTF-8 tail', (SELECT count() FROM u_log WHERE hasAny(v, [concat('VALUE', unhex('C2'))])), (SELECT count() FROM u_idx WHERE hasAny(v, [concat('VALUE', unhex('C2'))]));
SELECT 'hasAll String const, UTF-8 tail', (SELECT count() FROM u_log WHERE hasAll(v, [concat('VALUE', unhex('C2'))])), (SELECT count() FROM u_idx WHERE hasAll(v, [concat('VALUE', unhex('C2'))]));
SELECT 'mapContainsKey String const, UTF-8 tail', (SELECT count() FROM u_log WHERE mapContainsKey(mk, concat('VALUE', unhex('C2')))), (SELECT count() FROM u_idx WHERE mapContainsKey(mk, concat('VALUE', unhex('C2'))));

-- A tokenizer whose tokens can span the end of the value cannot answer for a trimmed constant, so the index is
-- declined there. A constant with nothing to trim is unaffected and keeps pruning.
DROP TABLE IF EXISTS s_array;
DROP TABLE IF EXISTS s_split;
DROP TABLE IF EXISTS s_log;
CREATE TABLE s_array (id UInt64, s String, INDEX idx_s s TYPE text(tokenizer = array)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE s_split (id UInt64, s String, INDEX idx_s s TYPE text(tokenizer = splitByString([',']))) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE s_log (id UInt64, s String) ENGINE = Log;
INSERT INTO s_array SELECT number, if(number = 7, concat('VALUE0', unhex('0000')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO s_split SELECT number, if(number = 7, concat('VALUE0', unhex('0000')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO s_log   SELECT number, if(number = 7, concat('VALUE0', unhex('0000')), 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'text array, NUL tail in the data', (SELECT count() FROM s_log WHERE s = toFixedString('VALUE0', 8)), (SELECT count() FROM s_array WHERE s = toFixedString('VALUE0', 8));
SELECT 'text splitByString, NUL tail in the data', (SELECT count() FROM s_log WHERE s = toFixedString('VALUE0', 8)), (SELECT count() FROM s_split WHERE s = toFixedString('VALUE0', 8));
-- A `String` constant has nothing to trim, so these two keep reading through their index (the `array` plan turns
-- into a trivial count over it, which prints no granule line).
SELECT 'text array uses its index', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM s_array WHERE s = 'FILLER13') WHERE explain ILIKE '%idx_s%';
SELECT 'text splitByString uses its index', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM s_split WHERE s = 'FILLER13') WHERE explain ILIKE '%idx_s%';
SELECT 'text array answer, String constant', (SELECT count() FROM s_log WHERE s = 'FILLER13'), (SELECT count() FROM s_array WHERE s = 'FILLER13');

-- A column type without a byte width cannot pad the probe back, so there the trimmed constant is declined instead.
DROP TABLE IF EXISTS w_str;
DROP TABLE IF EXISTS w_lc;
DROP TABLE IF EXISTS w_sparse;
DROP TABLE IF EXISTS w_token;
DROP TABLE IF EXISTS w_log;
CREATE TABLE w_str (id UInt64, s String, INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE w_lc (id UInt64, s LowCardinality(String), INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE w_sparse (id UInt64, s String, INDEX idx_s s TYPE sparse_grams(3, 100, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE w_token (id UInt64, s String, INDEX idx_s s TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE w_log (id UInt64, s String) ENGINE = Log;
INSERT INTO w_str     SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO w_lc      SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO w_sparse  SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO w_token   SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO w_log     SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);

SELECT 'String column, UTF-8 tail', (SELECT count() FROM w_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)), (SELECT count() FROM w_str WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8));
SELECT 'LowCardinality column, UTF-8 tail', (SELECT count() FROM w_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)), (SELECT count() FROM w_lc WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8));
SELECT 'sparse_grams column, UTF-8 tail', (SELECT count() FROM w_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)), (SELECT count() FROM w_sparse WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8));

-- The `text` index has the same trimming step behind its own guard, and the same incomplete sequence reaches it.
DROP TABLE IF EXISTS t_ngrams;
DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_token;
CREATE TABLE t_ngrams (id UInt64, s String, INDEX idx_s s TYPE text(tokenizer = ngrams(3))) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_sparse (id UInt64, s String, INDEX idx_s s TYPE text(tokenizer = sparseGrams)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_token (id UInt64, s String, INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_ngrams SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO t_sparse SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO t_token  SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'text ngrams, UTF-8 tail', (SELECT count() FROM w_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)), (SELECT count() FROM t_ngrams WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8));
SELECT 'text sparseGrams, UTF-8 tail', (SELECT count() FROM w_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)), (SELECT count() FROM t_sparse WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8));
SELECT 'text splitByNonAlpha, UTF-8 tail', (SELECT count() FROM w_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)), (SELECT count() FROM t_token WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8));
-- The trimmed constant must keep pruning where it ends on a sequence boundary, and the `splitByNonAlpha` tokenizer,
-- which has no final-term clamp, must keep pruning in both cases.
SELECT 'text ngrams prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrams WHERE s = toFixedString('FILLER13', 12)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'text sparseGrams prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE s = toFixedString('FILLER13', 12)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'text splitByNonAlpha keeps pruning', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_token WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'text ngrams declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngrams WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%idx_s%';

-- `IPv6` is stored and tokenized as 16 raw bytes, so a trailing zero group is padding that has to be probed. The
-- fillers carry no long zero run, so they share the trimmed probe's leading grams but not its zero-run grams.
DROP TABLE IF EXISTS ip_idx;
DROP TABLE IF EXISTS ip_log;
CREATE TABLE ip_idx (id UInt64, ip IPv6, INDEX idx_ip ip TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE ip_log (id UInt64, ip IPv6) ENGINE = Log;
INSERT INTO ip_idx SELECT number, multiIf(number = 7, toIPv6('2001:db8::'), number = 13, toIPv6('::ffff:1.2.3.4'), toIPv6('2001:db8:1:2:3:4:5:' || hex(number + 256))) FROM numbers(64);
INSERT INTO ip_log SELECT number, multiIf(number = 7, toIPv6('2001:db8::'), number = 13, toIPv6('::ffff:1.2.3.4'), toIPv6('2001:db8:1:2:3:4:5:' || hex(number + 256))) FROM numbers(64);
SELECT 'IPv6 equals', (SELECT count() FROM ip_log WHERE ip = toFixedString(reinterpretAsFixedString(toIPv6('2001:db8::')), 16)), (SELECT count() FROM ip_idx WHERE ip = toFixedString(reinterpretAsFixedString(toIPv6('2001:db8::')), 16));
-- A textual constant is compared against the parsed address, not against its own characters.
SELECT 'IPv6 equals text', (SELECT count() FROM ip_log WHERE ip = '2001:db8::'), (SELECT count() FROM ip_idx WHERE ip = '2001:db8::');
SELECT 'IPv6 equals text zero-filled', (SELECT count() FROM ip_log WHERE ip = '2001:0db8:0000:0000:0000:0000:0000:0000'), (SELECT count() FROM ip_idx WHERE ip = '2001:0db8:0000:0000:0000:0000:0000:0000');
SELECT 'IPv6 equals text uppercase', (SELECT count() FROM ip_log WHERE ip = '2001:DB8::'), (SELECT count() FROM ip_idx WHERE ip = '2001:DB8::');
SELECT 'IPv6 equals text v4 mapped', (SELECT count() FROM ip_log WHERE ip = '::ffff:1.2.3.4'), (SELECT count() FROM ip_idx WHERE ip = '::ffff:1.2.3.4');

-- A `LowCardinality(FixedString)` dictionary has the same byte width as the type it wraps.
DROP TABLE IF EXISTS lcfs_idx;
DROP TABLE IF EXISTS lcfs_log;
CREATE TABLE lcfs_idx (id UInt64, s LowCardinality(FixedString(8)), INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE lcfs_log (id UInt64, s LowCardinality(FixedString(8))) ENGINE = Log;
INSERT INTO lcfs_idx SELECT number, if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) FROM numbers(64);
INSERT INTO lcfs_log SELECT number, if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) FROM numbers(64);
SELECT 'LowCardinality(FixedString) wide const', (SELECT count() FROM lcfs_log WHERE s = toFixedString('VALUE0', 10)), (SELECT count() FROM lcfs_idx WHERE s = toFixedString('VALUE0', 10));

-- The answers above are also correct when the probe is merely a subset of the granule's tokens, so the rows below
-- assert the probe is the stored value and not a prefix of it. Every filler shares all grams of the trimmed probe,
-- which leaves the granule count as the only witness.
DROP TABLE IF EXISTS c_fs;
CREATE TABLE c_fs (id UInt64, v Array(FixedString(8)), s FixedString(8), mk Map(FixedString(8), String),
    INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_mk mapKeys(mk) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO c_fs SELECT number, [w], w, map(w, 'x') FROM (SELECT number, if(number = 7, toFixedString('VALUE0', 8), toFixedString('VALUE0' || substring(hex(number), 1, 2), 8)) AS w FROM numbers(64));

SELECT 'exact prune, equals', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE s = toFixedString('VALUE0', 10)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, has', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE has(v, toFixedString('VALUE0', 10))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, hasAny', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE hasAny(v, [toFixedString('VALUE0', 10)])) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, mapContainsKey', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE mapContainsKey(mk, toFixedString('VALUE0', 10))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, IPv6', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_idx WHERE ip = toFixedString(reinterpretAsFixedString(toIPv6('2001:db8::')), 16)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, IPv6 text', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM ip_idx WHERE ip = '2001:db8::') WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, hasAll', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE hasAll(v, [toFixedString('VALUE0', 10)])) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, mapContainsValue', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM fs_idx WHERE mapContainsValue(mv, toFixedString('VALUE0', 10))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'exact prune, LowCardinality(FixedString)', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM lcfs_idx WHERE s = toFixedString('VALUE0', 10)) WHERE explain ILIKE '%Granules: 1/64%';
-- A widthless domain still prunes when the trimmed constant ends on a sequence boundary, on both index types.
SELECT 'widthless prunes, ngrambf_v1', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM w_str WHERE s = toFixedString('FILLER13', 12)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'widthless prunes, LowCardinality', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM w_lc WHERE s = toFixedString('FILLER13', 12)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'widthless prunes, sparse_grams', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM w_sparse WHERE s = toFixedString('FILLER13', 12)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'widthless prunes, tokenbf_v1', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM w_token WHERE s = toFixedString('FILLER13', 12)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'widthless answers, ngrambf_v1', (SELECT count() FROM w_log WHERE s = toFixedString('FILLER13', 12)), (SELECT count() FROM w_str WHERE s = toFixedString('FILLER13', 12));

-- A membership predicate over a widthless domain compares the padding too, so the padded constant stays the probe.
DROP TABLE IF EXISTS lit_arr;
DROP TABLE IF EXISTS lit_log;
CREATE TABLE lit_arr (id UInt64, v Array(String), INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE lit_log (id UInt64, v Array(String)) ENGINE = Log;
INSERT INTO lit_arr SELECT number, if(number = 7, [concat('VALUE0', unhex('00000000'))], ['VALUE0' || substring(hex(number), 1, 2)]) FROM numbers(64);
INSERT INTO lit_log SELECT number, if(number = 7, [concat('VALUE0', unhex('00000000'))], ['VALUE0' || substring(hex(number), 1, 2)]) FROM numbers(64);
SELECT 'widthless has answer', (SELECT count() FROM lit_log WHERE has(v, toFixedString('VALUE0', 10))), (SELECT count() FROM lit_arr WHERE has(v, toFixedString('VALUE0', 10)));
SELECT 'widthless has prunes exactly', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM lit_arr WHERE has(v, toFixedString('VALUE0', 10))) WHERE explain ILIKE '%Granules: 1/64%';

-- A constant longer than the column equals no stored value, so pruning every granule is right and stays.
SELECT 'wider than column prunes all', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM fs_idx WHERE s = toFixedString('VALUE0LONGER', 12)) WHERE explain ILIKE '%Granules: 0/64%';
-- `splitByNonAlpha` has no final-token clamp, so the decline above must not reach it and it keeps pruning.
SELECT 'token index keeps pruning', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM w_token WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%Granules: 1/64%';
-- Where the probe was declined the index must be silent rather than prune everything.
SELECT 'declined index absent, String', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w_str WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%idx_s%';
SELECT 'declined index absent, LowCardinality', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w_lc WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%idx_s%';
SELECT 'declined index absent, sparse_grams', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w_sparse WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%idx_s%';
-- `has(<constant array>, <indexed scalar>)` does not ignore the padding: this predicate matches no row, so the probe
-- stays the padded constant and every granule is pruned.
SELECT 'reversed has unchanged', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM fs_idx WHERE has([toFixedString('VALUE0', 10)], s)) WHERE explain ILIKE '%Granules: 0/64%';

-- Through the `mapKeys` redirect the probe is the map key, while the constant type still describes the map value, so
-- the key must be probed as the map stores it.
DROP TABLE IF EXISTS r_mk;
CREATE TABLE r_mk (id UInt64, mk Map(String, FixedString(3)), INDEX idx_mk mapKeys(mk) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO r_mk SELECT number, if(number = 7, map(concat('VALUE', unhex('C2')), toFixedString('V', 3)), map('FILLER' || toString(number), toFixedString('F', 3))) FROM numbers(64);
SELECT 'map element redirect', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM r_mk WHERE mk[concat('VALUE', unhex('C2'))] = toFixedString('V', 3)) WHERE explain ILIKE '%Granules: 1/64%';

-- A map subcolumn read carries the key as its text, while a key type can store other bytes than that text.
DROP TABLE IF EXISTS ip_mk_idx;
DROP TABLE IF EXISTS ip_mk_log;
CREATE TABLE ip_mk_idx (id UInt64, m Map(IPv6, String), INDEX idx_mk mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE ip_mk_log (id UInt64, m Map(IPv6, String)) ENGINE = Log;
INSERT INTO ip_mk_idx SELECT number, map(if(number = 7, toIPv6('2001:db8::'), toIPv6('::' || toString(number + 100))), 'x') FROM numbers(64);
INSERT INTO ip_mk_log SELECT number, map(if(number = 7, toIPv6('2001:db8::'), toIPv6('::' || toString(number + 100))), 'x') FROM numbers(64);
SET optimize_functions_to_subcolumns = 1;
SELECT 'ipv6 map key subcolumn', (SELECT count() FROM ip_mk_log WHERE m[toIPv6('2001:db8::')] = 'x'), (SELECT count() FROM ip_mk_idx WHERE m[toIPv6('2001:db8::')] = 'x');
SET optimize_functions_to_subcolumns = 0;
DROP TABLE ip_mk_idx;
DROP TABLE ip_mk_log;

DROP TABLE fs_idx;
DROP TABLE fs_log;
DROP TABLE u_idx;
DROP TABLE u_log;
DROP TABLE w_str;
DROP TABLE w_lc;
DROP TABLE w_sparse;
DROP TABLE w_token;
DROP TABLE w_log;
DROP TABLE ip_idx;
DROP TABLE ip_log;
DROP TABLE c_fs;
DROP TABLE r_mk;
DROP TABLE lcfs_idx;
DROP TABLE lcfs_log;
DROP TABLE t_ngrams;
DROP TABLE t_sparse;
DROP TABLE t_token;
DROP TABLE lit_arr;
DROP TABLE lit_log;
DROP TABLE s_array;
DROP TABLE s_split;
DROP TABLE s_log;
