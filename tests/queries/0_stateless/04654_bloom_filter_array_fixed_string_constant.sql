-- { echo }

-- The index must hash the value the array-search function actually compares. Every cell asserts
-- keyed == oracle, so no expected answer is baked into the reference.

DROP TABLE IF EXISTS o_str;
DROP TABLE IF EXISTS k_str;
DROP TABLE IF EXISTS o_fs3;
DROP TABLE IF EXISTS k_fs3;
DROP TABLE IF EXISTS o_lcstr;
DROP TABLE IF EXISTS k_lcstr;
DROP TABLE IF EXISTS o_lcfs3;
DROP TABLE IF EXISTS k_lcfs3;
DROP TABLE IF EXISTS p_str;
DROP TABLE IF EXISTS p_fs3;
DROP TABLE IF EXISTS p_lcstr;
DROP TABLE IF EXISTS p_num;
DROP TABLE IF EXISTS q_str;
DROP TABLE IF EXISTS o_sc;
DROP TABLE IF EXISTS k_sc;

CREATE TABLE o_str (id UInt64, v Array(String)) ENGINE = Log;
CREATE TABLE k_str (id UInt64, v Array(String), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_str VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);
INSERT INTO k_str VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);

CREATE TABLE o_fs3 (id UInt64, v Array(FixedString(3))) ENGINE = Log;
CREATE TABLE k_fs3 (id UInt64, v Array(FixedString(3)), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_fs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);
INSERT INTO k_fs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);

CREATE TABLE o_lcstr (id UInt64, v Array(LowCardinality(String))) ENGINE = Log;
CREATE TABLE k_lcstr (id UInt64, v Array(LowCardinality(String)), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lcstr VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);
INSERT INTO k_lcstr VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);

CREATE TABLE o_lcfs3 (id UInt64, v Array(LowCardinality(FixedString(3)))) ENGINE = Log;
CREATE TABLE k_lcfs3 (id UInt64, v Array(LowCardinality(FixedString(3))), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lcfs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);
INSERT INTO k_lcfs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);

-- Array(String): hasAny/hasAll cast both arrays to the common type, so they compare the unpadded value.
SELECT 'str hasAny FS3', (SELECT count() FROM o_str WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_str WHERE hasAny(v,[toFixedString('V0',3)]));
SELECT 'str hasAny FS5', (SELECT count() FROM o_str WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_str WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'str hasAll FS3', (SELECT count() FROM o_str WHERE hasAll(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_str WHERE hasAll(v,[toFixedString('V0',3)]));
SELECT 'str hasAll FS5', (SELECT count() FROM o_str WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_str WHERE hasAll(v,[toFixedString('V0',5)]));

-- Array(String): has/indexOf take executeString, which compares the raw padded bytes. Must not change.
SELECT 'str has Str', (SELECT count() FROM o_str WHERE has(v,'V0')) = (SELECT count() FROM k_str WHERE has(v,'V0'));
SELECT 'str has FS2', (SELECT count() FROM o_str WHERE has(v,toFixedString('V0',2))) = (SELECT count() FROM k_str WHERE has(v,toFixedString('V0',2)));
SELECT 'str has FS3', (SELECT count() FROM o_str WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_str WHERE has(v,toFixedString('V0',3)));
SELECT 'str has FS5', (SELECT count() FROM o_str WHERE has(v,toFixedString('V0',5))) = (SELECT count() FROM k_str WHERE has(v,toFixedString('V0',5)));
SELECT 'str indexOf Str', (SELECT count() FROM o_str WHERE indexOf(v,'V0') = 1) = (SELECT count() FROM k_str WHERE indexOf(v,'V0') = 1);
SELECT 'str indexOf FS3', (SELECT count() FROM o_str WHERE indexOf(v,toFixedString('V0',3)) = 1) = (SELECT count() FROM k_str WHERE indexOf(v,toFixedString('V0',3)) = 1);
SELECT 'str hasAny Str', (SELECT count() FROM o_str WHERE hasAny(v,['V0'])) = (SELECT count() FROM k_str WHERE hasAny(v,['V0']));
SELECT 'str hasAny FS2', (SELECT count() FROM o_str WHERE hasAny(v,[toFixedString('V0',2)])) = (SELECT count() FROM k_str WHERE hasAny(v,[toFixedString('V0',2)]));
SELECT 'str hasAll Str', (SELECT count() FROM o_str WHERE hasAll(v,['V0'])) = (SELECT count() FROM k_str WHERE hasAll(v,['V0']));

-- Array(FixedString(3)): a wider constant is stripped then re-encoded into the element width, so it
-- matches exactly. Before the fix has/indexOf answered 0 and hasAny/hasAll raised Code 131.
SELECT 'fs3 has FS5', (SELECT count() FROM o_fs3 WHERE has(v,toFixedString('V0',5))) = (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('V0',5)));
SELECT 'fs3 indexOf FS5', (SELECT count() FROM o_fs3 WHERE indexOf(v,toFixedString('V0',5)) = 1) = (SELECT count() FROM k_fs3 WHERE indexOf(v,toFixedString('V0',5)) = 1);
SELECT 'fs3 hasAny FS5', (SELECT count() FROM o_fs3 WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_fs3 WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'fs3 hasAll FS5', (SELECT count() FROM o_fs3 WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_fs3 WHERE hasAll(v,[toFixedString('V0',5)]));
SELECT 'fs3 has Str', (SELECT count() FROM o_fs3 WHERE has(v,'V0')) = (SELECT count() FROM k_fs3 WHERE has(v,'V0'));
SELECT 'fs3 has FS2', (SELECT count() FROM o_fs3 WHERE has(v,toFixedString('V0',2))) = (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('V0',2)));
SELECT 'fs3 has FS3', (SELECT count() FROM o_fs3 WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('V0',3)));
SELECT 'fs3 indexOf FS3', (SELECT count() FROM o_fs3 WHERE indexOf(v,toFixedString('V0',3)) = 1) = (SELECT count() FROM k_fs3 WHERE indexOf(v,toFixedString('V0',3)) = 1);
SELECT 'fs3 hasAny FS3', (SELECT count() FROM o_fs3 WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_fs3 WHERE hasAny(v,[toFixedString('V0',3)]));
SELECT 'fs3 hasAll FS3', (SELECT count() FROM o_fs3 WHERE hasAll(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_fs3 WHERE hasAll(v,[toFixedString('V0',3)]));

-- Array(LowCardinality(String)): every predicate coerces here, so every FixedString constant was wrong.
SELECT 'lcstr has FS3', (SELECT count() FROM o_lcstr WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_lcstr WHERE has(v,toFixedString('V0',3)));
SELECT 'lcstr has FS5', (SELECT count() FROM o_lcstr WHERE has(v,toFixedString('V0',5))) = (SELECT count() FROM k_lcstr WHERE has(v,toFixedString('V0',5)));
SELECT 'lcstr indexOf FS3', (SELECT count() FROM o_lcstr WHERE indexOf(v,toFixedString('V0',3)) = 1) = (SELECT count() FROM k_lcstr WHERE indexOf(v,toFixedString('V0',3)) = 1);
SELECT 'lcstr indexOf FS5', (SELECT count() FROM o_lcstr WHERE indexOf(v,toFixedString('V0',5)) = 1) = (SELECT count() FROM k_lcstr WHERE indexOf(v,toFixedString('V0',5)) = 1);
SELECT 'lcstr hasAny FS3', (SELECT count() FROM o_lcstr WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcstr WHERE hasAny(v,[toFixedString('V0',3)]));
SELECT 'lcstr hasAny FS5', (SELECT count() FROM o_lcstr WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcstr WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'lcstr hasAll FS3', (SELECT count() FROM o_lcstr WHERE hasAll(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcstr WHERE hasAll(v,[toFixedString('V0',3)]));
SELECT 'lcstr hasAll FS5', (SELECT count() FROM o_lcstr WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcstr WHERE hasAll(v,[toFixedString('V0',5)]));
SELECT 'lcstr has Str', (SELECT count() FROM o_lcstr WHERE has(v,'V0')) = (SELECT count() FROM k_lcstr WHERE has(v,'V0'));
SELECT 'lcstr has FS2', (SELECT count() FROM o_lcstr WHERE has(v,toFixedString('V0',2))) = (SELECT count() FROM k_lcstr WHERE has(v,toFixedString('V0',2)));
SELECT 'lcstr hasAny Str', (SELECT count() FROM o_lcstr WHERE hasAny(v,['V0'])) = (SELECT count() FROM k_lcstr WHERE hasAny(v,['V0']));

-- Array(LowCardinality(FixedString(3))): hasAny/hasAll cast to the supertype and legitimately match.
SELECT 'lcfs3 hasAny FS5', (SELECT count() FROM o_lcfs3 WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcfs3 WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'lcfs3 hasAll FS5', (SELECT count() FROM o_lcfs3 WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcfs3 WHERE hasAll(v,[toFixedString('V0',5)]));
SELECT 'lcfs3 has FS3', (SELECT count() FROM o_lcfs3 WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_lcfs3 WHERE has(v,toFixedString('V0',3)));
SELECT 'lcfs3 hasAny FS3', (SELECT count() FROM o_lcfs3 WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcfs3 WHERE hasAny(v,[toFixedString('V0',3)]));

-- has/indexOf over a LowCardinality element cast the constant straight to the dictionary type, which
-- overflows for a wider constant. The engine raises this with no index at all, so the index must
-- decline rather than answer 0.
SELECT count() FROM o_lcfs3 WHERE has(v,toFixedString('V0',5)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM k_lcfs3 WHERE has(v,toFixedString('V0',5)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM o_lcfs3 WHERE indexOf(v,toFixedString('V0',5)) = 1; -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM k_lcfs3 WHERE indexOf(v,toFixedString('V0',5)) = 1; -- { serverError TOO_LARGE_STRING_SIZE }

-- Pruning must not be lost. 64 granules, needle in exactly one, so the reduction is real.
CREATE TABLE p_str (id UInt64, v Array(String), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO p_str SELECT number, [if(number = 7, 'V0', concat('z', toString(number)))] FROM numbers(64);
CREATE TABLE p_fs3 (id UInt64, v Array(FixedString(3)), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO p_fs3 SELECT number, [if(number = 7, toFixedString('V0',3), toFixedString(concat('z', leftPad(toString(number),2,'0')),3))] FROM numbers(64);
CREATE TABLE p_lcstr (id UInt64, v Array(LowCardinality(String)), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO p_lcstr SELECT number, [if(number = 7, 'V0', concat('z', toString(number)))] FROM numbers(64);

SELECT 'prune str has Str', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_str WHERE has(v,'V0')) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune str hasAny FS3', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_str WHERE hasAny(v,[toFixedString('V0',3)])) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));

-- has over a bare String element keeps hashing the padded form, so a FixedString(3) constant selects
-- the granule holding 'V0\0' and not the one holding 'V0'. Asserting the exact granule the index picks
-- pins the representation: a >0-reduction test would also pass if the index pruned everything away.
CREATE TABLE q_str (id UInt64, v Array(String), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO q_str SELECT number, [multiIf(number = 7, 'V0', number = 11, 'V0\0', concat('z', toString(number)))] FROM numbers(64);
SELECT 'padded str has FS3', (SELECT count() FROM q_str WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM q_str WHERE has(v,toFixedString('V0',3)) SETTINGS use_skip_indexes=0);
SELECT 'padded str has FS3 id', (SELECT groupArray(id) FROM (SELECT id FROM q_str WHERE has(v,toFixedString('V0',3)) ORDER BY id)) = (SELECT groupArray(id) FROM (SELECT id FROM q_str WHERE has(v,toFixedString('V0',3)) ORDER BY id SETTINGS use_skip_indexes=0));
-- On byte-identical data has matches the padded row and hasAny matches the unpadded one. Pinning the
-- absolute ids is what proves each predicate keeps its own representation: a keyed-vs-unkeyed
-- comparison alone stays green if a change moves both sides together.
SELECT 'padded str has FS3 is 11', (SELECT groupArray(id) FROM (SELECT id FROM q_str WHERE has(v,toFixedString('V0',3)) ORDER BY id)) = [11];
SELECT 'unpadded str hasAny FS3 is 7', (SELECT groupArray(id) FROM (SELECT id FROM q_str WHERE hasAny(v,[toFixedString('V0',3)]) ORDER BY id)) = [7];
SELECT 'prune str has FS3', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM q_str WHERE has(v,toFixedString('V0',3))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) > 0 AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'padded str hasAny FS3 id', (SELECT groupArray(id) FROM (SELECT id FROM q_str WHERE hasAny(v,[toFixedString('V0',3)]) ORDER BY id)) = (SELECT groupArray(id) FROM (SELECT id FROM q_str WHERE hasAny(v,[toFixedString('V0',3)]) ORDER BY id SETTINGS use_skip_indexes=0));
SELECT 'prune fs3 has FS5', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_fs3 WHERE has(v,toFixedString('V0',5))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune fs3 indexOf FS5', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_fs3 WHERE indexOf(v,toFixedString('V0',5)) = 1) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune fs3 hasAny FS5', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_fs3 WHERE hasAny(v,[toFixedString('V0',5)])) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune fs3 hasAll FS5', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_fs3 WHERE hasAll(v,[toFixedString('V0',5)])) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune fs3 has FS3', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_fs3 WHERE has(v,toFixedString('V0',3))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune lcstr has FS3', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_lcstr WHERE has(v,toFixedString('V0',3))) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune lcstr hasAny FS3', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_lcstr WHERE hasAny(v,[toFixedString('V0',3)])) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));
SELECT 'prune lcstr has Str', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_lcstr WHERE has(v,'V0')) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));

-- A numeric element takes executeIntegral, a coercion the helper does not emulate, so it stays on the
-- original path. Answer and pruning must both be unaffected.
CREATE TABLE p_num (id UInt64, v Array(UInt64), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO p_num SELECT number, [if(number = 7, 999, number)] FROM numbers(64);
SELECT 'num has', (SELECT count() FROM p_num WHERE has(v,999)) = (SELECT count() FROM p_num WHERE has(v,999) SETTINGS use_skip_indexes=0);
SELECT 'num hasAny', (SELECT count() FROM p_num WHERE hasAny(v,[999])) = (SELECT count() FROM p_num WHERE hasAny(v,[999]) SETTINGS use_skip_indexes=0);
SELECT 'prune num has', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM p_num WHERE has(v,999)) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));

-- has(<const array>, <indexed scalar>) shares createColumnFromConstantArray, whose runtime is a
-- Field-level accurateEquals and therefore needs the padded form. Deliberately left untouched.
-- The indexed column must be a SCALAR: with an array column this form does not use the index at all,
-- which would make every assertion below vacuous.
CREATE TABLE o_sc (id UInt64, s String) ENGINE = Log;
CREATE TABLE k_sc (id UInt64, s String, INDEX idx s TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_sc VALUES (0,'V0'),(1,'V0\0'),(2,'V0\0\0'),(3,'X');
INSERT INTO k_sc VALUES (0,'V0'),(1,'V0\0'),(2,'V0\0\0'),(3,'X');
SELECT 'const-array has FS3', (SELECT count() FROM o_sc WHERE has([toFixedString('V0',3)], s)) = (SELECT count() FROM k_sc WHERE has([toFixedString('V0',3)], s));
SELECT 'const-array has FS5', (SELECT count() FROM o_sc WHERE has([toFixedString('V0',5)], s)) = (SELECT count() FROM k_sc WHERE has([toFixedString('V0',5)], s));
SELECT 'const-array has Str', (SELECT count() FROM o_sc WHERE has(['V0'], s)) = (SELECT count() FROM k_sc WHERE has(['V0'], s));
SELECT 'const-array has FS3 id', (SELECT groupArray(id) FROM (SELECT id FROM k_sc WHERE has([toFixedString('V0',3)], s) ORDER BY id)) = [1];
SELECT 'prune const-array has FS3', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM k_sc WHERE has([toFixedString('V0',3)], s)) WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) > 0 AND toUInt64OrZero(extract(explain,'Granules: (\d+)/')) < toUInt64OrZero(extract(explain,'Granules: \d+/(\d+)'));

DROP TABLE o_str;
DROP TABLE k_str;
DROP TABLE o_fs3;
DROP TABLE k_fs3;
DROP TABLE o_lcstr;
DROP TABLE k_lcstr;
DROP TABLE o_lcfs3;
DROP TABLE k_lcfs3;
DROP TABLE p_str;
DROP TABLE p_fs3;
DROP TABLE p_lcstr;
DROP TABLE p_num;
DROP TABLE q_str;
DROP TABLE o_sc;
DROP TABLE k_sc;
