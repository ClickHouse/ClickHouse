-- Text bloom-filter skip indexes (ngrambf_v1, tokenbf_v1, sparse_grams) tokenize a string constant into a probe
-- filter. A FixedString constant carries NUL padding in its Field, and the padding does not always survive to the
-- comparison the executing function performs, so the probe used to be built over tokens the granule never saw and a
-- matching granule was skipped.
--
-- Direction A asserts the answers are now correct; every A row is paired with an unindexed oracle table.
-- Direction B asserts the cells that were already correct keep pruning.
-- Direction C is an adversarial fixture where every filler shares all of the trimmed probe's n-grams, so only the
-- granule count can tell a correct probe from an over-broad one.
-- Direction D pins the one cell that is out of scope as unchanged rather than as correct.

SET use_skip_indexes = 1;

-- ---------------------------------------------------------------------------------------------------
-- Direction A: correctness. Needle in exactly one granule of 64.
-- ---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS a_str;
DROP TABLE IF EXISTS a_str_log;
CREATE TABLE a_str (id UInt64, v Array(String), s String,
    INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_str_log (id UInt64, v Array(String), s String) ENGINE = Log;
INSERT INTO a_str SELECT number, if(number = 7, ['VALUE0'], ['FILLER' || toString(number)]),
    if(number = 7, 'VALUE0', 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO a_str_log SELECT number, if(number = 7, ['VALUE0'], ['FILLER' || toString(number)]),
    if(number = 7, 'VALUE0', 'FILLER' || toString(number)) FROM numbers(64);

SELECT 'A1', count() FROM a_str_log WHERE hasAny(v, [toFixedString('VALUE0', 8)]);
SELECT 'A1', count() FROM a_str     WHERE hasAny(v, [toFixedString('VALUE0', 8)]);
SELECT 'A2', count() FROM a_str_log WHERE hasAny(v, [toFixedString('VALUE0', 10)]);
SELECT 'A2', count() FROM a_str     WHERE hasAny(v, [toFixedString('VALUE0', 10)]);
SELECT 'A3', count() FROM a_str_log WHERE hasAll(v, [toFixedString('VALUE0', 8)]);
SELECT 'A3', count() FROM a_str     WHERE hasAll(v, [toFixedString('VALUE0', 8)]);
SELECT 'A4', count() FROM a_str_log WHERE hasAll(v, [toFixedString('VALUE0', 10)]);
SELECT 'A4', count() FROM a_str     WHERE hasAll(v, [toFixedString('VALUE0', 10)]);

SELECT 'A10', count() FROM a_str_log WHERE s = toFixedString('VALUE0', 8);
SELECT 'A10', count() FROM a_str     WHERE s = toFixedString('VALUE0', 8);
SELECT 'A11', count() FROM a_str_log WHERE s = toFixedString('VALUE0', 10);
SELECT 'A11', count() FROM a_str     WHERE s = toFixedString('VALUE0', 10);

-- A17/A18 must use the `NOT (s != c)` spelling: a positive `s != c` atom never prunes, so the plain form would
-- silently assert nothing. The read path canonicalizes this to `equals`, so these are extra spellings of the
-- `equals` arm and they must redden together with A10/A11.
SELECT 'A17', count() FROM a_str_log WHERE NOT (s != toFixedString('VALUE0', 8));
SELECT 'A17', count() FROM a_str     WHERE NOT (s != toFixedString('VALUE0', 8));
SELECT 'A18', count() FROM a_str_log WHERE NOT (s != toFixedString('VALUE0', 10));
SELECT 'A18', count() FROM a_str     WHERE NOT (s != toFixedString('VALUE0', 10));

-- A25: a widthless index cannot pad the probe back, so it ships the trimmed constant. That is token-preserving only
-- while the trimmed value does not end inside a declared UTF-8 sequence: the n-gram tokenizer steps by seqLength and
-- clamps its final gram to the buffer, so a stored value ending `C2 00` yields the gram `5545C200` while the trimmed
-- probe `..C2` yields `5545C2` - not a subset, so the granule holding the matching row was skipped. The stored value
-- must keep a NUL AFTER the lead byte: with the lead byte last there is nothing to trim and the probe is the stored
-- value itself, so an exact-width fixture would assert nothing here.
DROP TABLE IF EXISTS a_str_utf8;
DROP TABLE IF EXISTS a_str_utf8_log;
CREATE TABLE a_str_utf8 (id UInt64, s String,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_str_utf8_log (id UInt64, s String) ENGINE = Log;
INSERT INTO a_str_utf8     SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO a_str_utf8_log SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'A25', count() FROM a_str_utf8_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);
SELECT 'A25', count() FROM a_str_utf8     WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);

-- A27: the tokenbf_v1 twin of A25. The token tokenizer advances one byte at a time and never consults seqLength, so
-- it has no final-token clamp and trimming is token-preserving for it whatever the trimmed bytes are. The decline
-- A25 needs must therefore not fire here, or this index stops pruning where it used to. The answer is correct either
-- way, so the granule row in direction B is the half that catches it.
DROP TABLE IF EXISTS a_tok_utf8;
DROP TABLE IF EXISTS a_tok_utf8_log;
CREATE TABLE a_tok_utf8 (id UInt64, s String,
    INDEX idx_s s TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_tok_utf8_log (id UInt64, s String) ENGINE = Log;
INSERT INTO a_tok_utf8     SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO a_tok_utf8_log SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'A27', count() FROM a_tok_utf8_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);
SELECT 'A27', count() FROM a_tok_utf8     WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);

-- A28: an IPv6 index. The validator accepts IPv6 alongside String and FixedString, and although IPv6 is not a
-- DataTypeFixedString it is stored and tokenized as exactly 16 raw bytes, which is also what `equals` compares
-- against a FixedString(16) constant (its own special case runs before any common-type cast). So this domain has a
-- width and the probe must be padded back to it, not trimmed. The needle ends in ten zero bytes, so a trimmed probe
-- covers every filler and the index stops pruning; the answer stays correct, so only the granule row in direction B
-- sees it. The fillers deliberately carry no long zero run: they share the needle's leading grams but not its
-- zero-run grams, which is what makes an exact probe selective here.
DROP TABLE IF EXISTS a_ip6;
DROP TABLE IF EXISTS a_ip6_log;
CREATE TABLE a_ip6 (id UInt64, ip IPv6,
    INDEX idx_ip ip TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_ip6_log (id UInt64, ip IPv6) ENGINE = Log;
INSERT INTO a_ip6     SELECT number, if(number = 7, toIPv6('2001:db8::'), toIPv6('2001:db8:1:2:3:4:5:' || hex(number + 256))) FROM numbers(64);
INSERT INTO a_ip6_log SELECT number, if(number = 7, toIPv6('2001:db8::'), toIPv6('2001:db8:1:2:3:4:5:' || hex(number + 256))) FROM numbers(64);
SELECT 'A28', count() FROM a_ip6_log WHERE ip = toFixedString(reinterpretAsFixedString(toIPv6('2001:db8::')), 16);
SELECT 'A28', count() FROM a_ip6     WHERE ip = toFixedString(reinterpretAsFixedString(toIPv6('2001:db8::')), 16);

-- sparse_grams shares the same condition class and is affected identically.
DROP TABLE IF EXISTS a_sparse;
CREATE TABLE a_sparse (id UInt64, v Array(String),
    INDEX idx_v v TYPE sparse_grams(3, 100, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO a_sparse SELECT number, if(number = 7, ['VALUE0'], ['FILLER' || toString(number)]) FROM numbers(64);
SELECT 'A5', count() FROM a_str_log WHERE hasAny(v, [toFixedString('VALUE0', 8)]);
SELECT 'A5', count() FROM a_sparse  WHERE hasAny(v, [toFixedString('VALUE0', 8)]);
SELECT 'A6', count() FROM a_str_log WHERE hasAll(v, [toFixedString('VALUE0', 8)]);
SELECT 'A6', count() FROM a_sparse  WHERE hasAll(v, [toFixedString('VALUE0', 8)]);

-- FixedString(8) carriers. Values are distinct 8-byte strings so each granule holds its own value.
DROP TABLE IF EXISTS a_fs;
DROP TABLE IF EXISTS a_fs_log;
CREATE TABLE a_fs (id UInt64, v Array(FixedString(8)), s FixedString(8),
    INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_fs_log (id UInt64, v Array(FixedString(8)), s FixedString(8)) ENGINE = Log;
INSERT INTO a_fs SELECT number, if(number = 7, [toFixedString('VALUE0', 8)], [toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)]),
    if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) FROM numbers(64);
INSERT INTO a_fs_log SELECT number, if(number = 7, [toFixedString('VALUE0', 8)], [toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)]),
    if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) FROM numbers(64);

SELECT 'A7', count() FROM a_fs_log WHERE hasAny(v, [toFixedString('VALUE0', 10)]);
SELECT 'A7', count() FROM a_fs     WHERE hasAny(v, [toFixedString('VALUE0', 10)]);
SELECT 'A8', count() FROM a_fs_log WHERE hasAll(v, [toFixedString('VALUE0', 10)]);
SELECT 'A8', count() FROM a_fs     WHERE hasAll(v, [toFixedString('VALUE0', 10)]);
-- A9: `has` is correct on Array(String) and wrong on Array(FixedString) — the element type decides, not the
-- predicate. A scalar constant makes value_data_type.isArray() false, so this lands on the scalar `has` arm.
SELECT 'A9', count() FROM a_fs_log WHERE has(v, toFixedString('VALUE0', 10));
SELECT 'A9', count() FROM a_fs     WHERE has(v, toFixedString('VALUE0', 10));

SELECT 'A12', count() FROM a_fs_log WHERE s = toFixedString('VALUE0', 10);
SELECT 'A12', count() FROM a_fs     WHERE s = toFixedString('VALUE0', 10);
-- A13: the constant is a plain String, so a guard keyed only on the constant type would miss this cell.
SELECT 'A13', count() FROM a_fs_log WHERE s = 'VALUE0\0\0\0\0';
SELECT 'A13', count() FROM a_fs     WHERE s = 'VALUE0\0\0\0\0';
SELECT 'A19', count() FROM a_fs_log WHERE NOT (s != toFixedString('VALUE0', 10));
SELECT 'A19', count() FROM a_fs     WHERE NOT (s != toFixedString('VALUE0', 10));
SELECT 'A19b', count() FROM a_fs_log WHERE NOT (s != 'VALUE0\0\0\0\0');
SELECT 'A19b', count() FROM a_fs     WHERE NOT (s != 'VALUE0\0\0\0\0');

-- LowCardinality(String): the dictionary type has no width, so the trimmed probe is the sound one.
DROP TABLE IF EXISTS a_lc;
DROP TABLE IF EXISTS a_lc_log;
CREATE TABLE a_lc (id UInt64, s LowCardinality(String),
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_lc_log (id UInt64, s LowCardinality(String)) ENGINE = Log;
INSERT INTO a_lc SELECT number, if(number = 7, 'VALUE0', 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO a_lc_log SELECT number, if(number = 7, 'VALUE0', 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'A14', count() FROM a_lc_log WHERE s = toFixedString('VALUE0', 8);
SELECT 'A14', count() FROM a_lc     WHERE s = toFixedString('VALUE0', 8);
SELECT 'A15', count() FROM a_lc_log WHERE s = toFixedString('VALUE0', 10);
SELECT 'A15', count() FROM a_lc     WHERE s = toFixedString('VALUE0', 10);

-- A26: the LowCardinality(String) twin of A25. The dictionary type is widthless, so this also ships a trimmed probe
-- and needs the same sequence-completeness guard.
DROP TABLE IF EXISTS a_lc_utf8;
DROP TABLE IF EXISTS a_lc_utf8_log;
CREATE TABLE a_lc_utf8 (id UInt64, s LowCardinality(String),
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_lc_utf8_log (id UInt64, s LowCardinality(String)) ENGINE = Log;
INSERT INTO a_lc_utf8     SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO a_lc_utf8_log SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'A26', count() FROM a_lc_utf8_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);
SELECT 'A26', count() FROM a_lc_utf8     WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);

-- LowCardinality(FixedString(8)): the width comes from the dictionary type, so the probe is re-encoded to 8 bytes.
DROP TABLE IF EXISTS a_lcfs;
DROP TABLE IF EXISTS a_lcfs_log;
CREATE TABLE a_lcfs (id UInt64, s LowCardinality(FixedString(8)),
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_lcfs_log (id UInt64, s LowCardinality(FixedString(8))) ENGINE = Log;
INSERT INTO a_lcfs SELECT number, if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) FROM numbers(64);
INSERT INTO a_lcfs_log SELECT number, if(number = 7, toFixedString('VALUE0', 8), toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8)) FROM numbers(64);
SELECT 'A15b', count() FROM a_lcfs_log WHERE s = toFixedString('VALUE0', 10);
SELECT 'A15b', count() FROM a_lcfs     WHERE s = toFixedString('VALUE0', 10);

-- Map(FixedString(8), String) with a mapKeys index. A Map(String, ...) fixture would make every FixedString-constant
-- map-key cell vacuous, because the constant could never match.
DROP TABLE IF EXISTS a_mk;
DROP TABLE IF EXISTS a_mk_log;
CREATE TABLE a_mk (id UInt64, m Map(FixedString(8), String),
    INDEX idx_k mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_mk_log (id UInt64, m Map(FixedString(8), String)) ENGINE = Log;
INSERT INTO a_mk SELECT number, if(number = 7, map(toFixedString('VALUE0', 8), 'hit'), map(toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8), 'x')) FROM numbers(64);
INSERT INTO a_mk_log SELECT number, if(number = 7, map(toFixedString('VALUE0', 8), 'hit'), map(toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8), 'x')) FROM numbers(64);

-- A20/A21 land on the map branch (the column name is `m`, so only map_key_index resolves) and A22 on the scalar
-- `has` arm (the column name is literally `mapKeys(m)`, so key_index matches directly). Both spellings are reached,
-- so a test with only one covers half the class.
--
-- The pin keeps these on the map branch they are labelled for. optimize_functions_to_subcolumns would otherwise
-- rewrite mapContainsKey(m, x) into has(m.keys, x), which is another spelling of the arm A22 already covers, and
-- that setting is randomized per run. Today the rewrite cannot fire here anyway - the pass treats every column a
-- secondary index needs as a key column and only whitelists arrayElement for those - but that whitelist is an
-- explicit exception list with a TODO next to it, so the pin is what keeps these cells on their labelled branch if
-- mapContainsKey is ever added to it. Pinning the one setting rather than disabling randomization keeps the rest of
-- the run randomized.
SELECT 'A20', count() FROM a_mk_log WHERE mapContainsKey(m, toFixedString('VALUE0', 10));
SELECT 'A20', count() FROM a_mk     WHERE mapContainsKey(m, toFixedString('VALUE0', 10)) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'A21', count() FROM a_mk_log WHERE mapContains(m, toFixedString('VALUE0', 10));
SELECT 'A21', count() FROM a_mk     WHERE mapContains(m, toFixedString('VALUE0', 10)) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'A22', count() FROM a_mk_log WHERE has(mapKeys(m), toFixedString('VALUE0', 10));
SELECT 'A22', count() FROM a_mk     WHERE has(mapKeys(m), toFixedString('VALUE0', 10));

-- Map(String, FixedString(8)) with a mapValues index: A16 plus the mapValues redirect into the equals arm.
DROP TABLE IF EXISTS a_mv;
DROP TABLE IF EXISTS a_mv_log;
CREATE TABLE a_mv (id UInt64, m Map(String, FixedString(8)),
    INDEX idx_v mapValues(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_mv_log (id UInt64, m Map(String, FixedString(8))) ENGINE = Log;
INSERT INTO a_mv SELECT number, if(number = 7, map('k', toFixedString('VALUE0', 8)), map('k', toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8))) FROM numbers(64);
INSERT INTO a_mv_log SELECT number, if(number = 7, map('k', toFixedString('VALUE0', 8)), map('k', toFixedString('FILL' || leftPad(toString(number), 4, '0'), 8))) FROM numbers(64);
SELECT 'A16', count() FROM a_mv_log WHERE mapContainsValue(m, toFixedString('VALUE0', 10));
SELECT 'A16', count() FROM a_mv     WHERE mapContainsValue(m, toFixedString('VALUE0', 10)) SETTINGS optimize_functions_to_subcolumns = 0;
-- The mapValues redirect is a third entry point into the equals arm; there value_type does describe the compared
-- operand, so it is re-encoded like any other FixedString-valued path.
SELECT 'A16b', count() FROM a_mv_log WHERE m['k'] = toFixedString('VALUE0', 10);
SELECT 'A16b', count() FROM a_mv     WHERE m['k'] = toFixedString('VALUE0', 10);

-- A23: an all-NUL FixedString constant against a String index. This reaches the plain equals arm, so it is not the
-- map default-match guard that 2026-07-29-p2-tokenbfv1ngrambfv1-map-default-mat owns.
DROP TABLE IF EXISTS a_empty;
DROP TABLE IF EXISTS a_empty_log;
CREATE TABLE a_empty (id UInt64, s String,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_empty_log (id UInt64, s String) ENGINE = Log;
INSERT INTO a_empty SELECT number, if(number = 7, '', 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO a_empty_log SELECT number, if(number = 7, '', 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'A23', count() FROM a_empty_log WHERE s = toFixedString('', 3);
SELECT 'A23', count() FROM a_empty     WHERE s = toFixedString('', 3);

-- A29: the sparse_grams twin of A25. sparse_grams also walks its input by seqLength (through
-- SparseGramsImpl<true>), so it has the same final-token clamp and the same decline is required. The stored
-- value must keep a NUL AFTER the lead byte, exactly as in A25. B28 is the half that reads the decline.
DROP TABLE IF EXISTS a_sparse_utf8;
DROP TABLE IF EXISTS a_sparse_utf8_log;
CREATE TABLE a_sparse_utf8 (id UInt64, s String,
    INDEX idx_s s TYPE sparse_grams(3, 100, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE a_sparse_utf8_log (id UInt64, s String) ENGINE = Log;
INSERT INTO a_sparse_utf8     SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
INSERT INTO a_sparse_utf8_log SELECT number, if(number = 7, concat('VALUE', unhex('C200')), 'FILLER' || toString(number)) FROM numbers(64);
SELECT 'A29', count() FROM a_sparse_utf8_log WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);
SELECT 'A29', count() FROM a_sparse_utf8     WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8);

-- ---------------------------------------------------------------------------------------------------
-- Direction B: no pruning lost. Asserted on the granule count, because a too-wide probe still answers
-- correctly and shows up only as a full scan.
-- ---------------------------------------------------------------------------------------------------

SELECT 'B1', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str WHERE hasAny(v, ['VALUE0'])) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B2', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_fs WHERE hasAny(v, [toFixedString('VALUE0', 8)])) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B3', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str WHERE has(v, 'VALUE0')) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B4', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_fs WHERE has(v, toFixedString('VALUE0', 8))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B5', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str WHERE s = 'VALUE0') WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B6', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_fs WHERE s = toFixedString('VALUE0', 8)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B7', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_lc WHERE s = 'VALUE0') WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B8', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_mk WHERE mapContainsKey(m, toFixedString('VALUE0', 8)) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B9', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_mv WHERE mapContainsValue(m, toFixedString('VALUE0', 8)) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%Granules: 1/64%';
-- The reversed shape `has(<constant array>, <indexed scalar>)` shares the array arm but compares padded bytes, so
-- its probe is left alone.
SELECT 'B10', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str WHERE has(['VALUE0'], s)) WHERE explain ILIKE '%Granules: 1/64%';
-- mapContainsValueLike parses LIKE wildcards out of the pattern, so it is deliberately not normalized. This row is
-- what makes that exclusion load-bearing.
SELECT 'B11', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_mv WHERE mapContainsValueLike(m, 'VALUE%')) WHERE explain ILIKE '%Granules: 1/64%';
-- A needle that matches nothing must still prune everything away.
SELECT 'B12', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str WHERE hasAny(v, ['NOSUCHVALUE'])) WHERE explain ILIKE '%Granules: 0/64%';
-- Prefix fixture for the startsWith/endsWith rows: a FixedString(8) column whose every value starts with 'VALUE0',
-- so a correct prefix probe reads all 64 granules while a padded one would prune them away.
DROP TABLE IF EXISTS c_str_pfx;
DROP TABLE IF EXISTS c_str_pfx_log;
CREATE TABLE c_str_pfx (id UInt64, s FixedString(8),
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE c_str_pfx_log (id UInt64, s FixedString(8)) ENGINE = Log;
INSERT INTO c_str_pfx SELECT number, toFixedString('VALUE0' || substring('AB', 1 + (number % 2), 1), 8) FROM numbers(64);
INSERT INTO c_str_pfx_log SELECT number, toFixedString('VALUE0' || substring('AB', 1 + (number % 2), 1), 8) FROM numbers(64);

-- tokenbf_v1 splits on non-alphanumeric bytes, so a trailing NUL never survives tokenization and the fix must be a
-- no-op for it. This row pins that.
DROP TABLE IF EXISTS b_token;
CREATE TABLE b_token (id UInt64, v Array(String),
    INDEX idx_v v TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO b_token SELECT number, if(number = 7, ['VALUE0'], ['FILLER' || toString(number)]) FROM numbers(64);
SELECT 'B13', count() FROM a_str_log WHERE hasAny(v, [toFixedString('VALUE0', 8)]);
SELECT 'B13', count() FROM b_token   WHERE hasAny(v, [toFixedString('VALUE0', 8)]);
SELECT 'B13g', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM b_token WHERE hasAny(v, [toFixedString('VALUE0', 8)])) WHERE explain ILIKE '%Granules: 1/64%';
-- startsWith/endsWith build a substring probe, which is already a subset of the stored token set, so they are not
-- normalized either. Their constant is a PREFIX, not a value: padding it would append literal NULs that a longer
-- matching value does not contain, so a shorter-than-the-column constant is the discriminating shape here. On a
-- FixedString(8) column every row starts with 'VALUE0', so all 64 granules must be read.
SELECT 'B14', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_fs WHERE startsWith(s, toFixedString('VALUE0', 8))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B15', count() FROM c_str_pfx_log WHERE startsWith(s, toFixedString('VALUE0', 6));
SELECT 'B15', count() FROM c_str_pfx     WHERE startsWith(s, toFixedString('VALUE0', 6));
SELECT 'B15g', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM c_str_pfx WHERE startsWith(s, toFixedString('VALUE0', 6))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B16', count() FROM c_str_pfx_log WHERE endsWith(s, toFixedString('0', 1));
SELECT 'B16', count() FROM c_str_pfx     WHERE endsWith(s, toFixedString('0', 1));

-- B17/B17g: the two decline paths, asserted as declines rather than left silent. A constant wider than the indexed
-- FixedString(8) (B17) and a trimmed constant that is not UTF-8-sequence-complete on a widthless index (B17g) both
-- make the atom UNKNOWN, so the skip index removes nothing and every granule is read. The answer is 0 either way, so
-- only the granule count can tell a decline from a prune - and it must be asserted as the ABSENCE of the skip index's
-- `Granules: 0/64` line, not the presence of `64/64`: the plan also carries the primary key's own unconditional
-- `Granules: 64/64` line, so matching that would pass under a prune too (measured vacuous). B17g fails without the
-- sequence-completeness guard, which is what makes it the load-bearing half.
SELECT 'B17', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM a_fs WHERE s = toFixedString('VALUE0LONGER', 12)) WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'B17g', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str_utf8 WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%Granules: 0/64%';

-- B18: the tokenbf_v1 half of A27. B17g's decline must NOT extend to this tokenizer, so this reads an exact prune
-- where B17g reads a decline - the same query shape on the same data, one index type apart.
SELECT 'B18', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_tok_utf8 WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%Granules: 1/64%';

-- B19: the IPv6 half of A28. Trimming this probe instead of padding it back to 16 bytes reads all 64 granules.
SELECT 'B19', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_ip6 WHERE ip = toFixedString(reinterpretAsFixedString(toIPv6('2001:db8::')), 16)) WHERE explain ILIKE '%Granules: 1/64%';

-- B20-B23: the WIDE-constant granule rows. B8/B9 above use an exact-width constant, where the padding is the
-- identity, so they pass whether or not the probe is re-encoded; the cells this change actually alters on these arms
-- are the wide-constant ones (A20/A21/A16/A11), and those were asserted on the answer alone. These four assert that
-- each still prunes exactly.
SELECT 'B20', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_mk WHERE mapContainsKey(m, toFixedString('VALUE0', 10)) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B21', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_mk WHERE mapContains(m, toFixedString('VALUE0', 10)) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'B22', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_mv WHERE mapContainsValue(m, toFixedString('VALUE0', 10)) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%Granules: 1/64%';
-- The widthless equals arm needs a fixture whose fillers do NOT share the probe's grams, so c_str cannot serve (there
-- sharing them is the point). a_str's fillers are `FILLERn`, so an exact probe prunes to one granule.
SELECT 'B23', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str WHERE s = toFixedString('VALUE0', 10)) WHERE explain ILIKE '%Granules: 1/64%';

-- B24-B28: the remaining WIDE-constant granule rows, for the same reason as B20-B23. A decline returns
-- false from the arm and the atom becomes UNKNOWN, so the skip index removes nothing and every A row still
-- reads its correct answer - an answer-only cell cannot tell an exact probe from a declined one. These
-- fixtures' fillers are FILLERn/FILLnnnn, so an exact probe prunes to one granule while a trimmed or
-- declined one does not.
-- B24: the widthless-String array arm (A1-A4 are answer-only). B1 uses a plain String constant, for which
-- normalization never runs, so it cannot cover this cell.
SELECT 'B24', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_str WHERE hasAny(v, [toFixedString('VALUE0', 10)])) WHERE explain ILIKE '%Granules: 1/64%';
-- B25: the sparse_grams array arm (A5/A6 are answer-only and a_sparse had no granule row at all).
SELECT 'B25', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_sparse WHERE hasAny(v, [toFixedString('VALUE0', 10)])) WHERE explain ILIKE '%Granules: 1/64%';
-- B26: the widthless equals arm reached through LowCardinality(String) (A14/A15 are answer-only).
SELECT 'B26', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_lc WHERE s = toFixedString('VALUE0', 10)) WHERE explain ILIKE '%Granules: 1/64%';
-- B27: the width equals arm reached through LowCardinality(FixedString(8)) (A15b is answer-only).
SELECT 'B27', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM a_lcfs WHERE s = toFixedString('VALUE0', 10)) WHERE explain ILIKE '%Granules: 1/64%';
-- B28: the sparse_grams half of A29, and the only row that pins the SparseGrams answer of the
-- tokenizer-kind predicate. It reads a DECLINE, like B17g and unlike B18, so it uses the same absence
-- idiom: the plan carries the primary key's own unconditional `Granules: 64/64` line, so a decline must be
-- read as the ABSENCE of the skip index's `Granules: 0/64` line.
SELECT 'B28', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM a_sparse_utf8 WHERE s = toFixedString(concat('VALUE', unhex('C2')), 8)) WHERE explain ILIKE '%Granules: 0/64%';

-- ---------------------------------------------------------------------------------------------------
-- Direction C: the adversarial shared-gram fixture. Every filler shares all of the trimmed probe's 3-grams, so a
-- probe that is merely sound rather than exact reads all 64 granules. None of these rows is visible to a
-- correctness assertion: the answers are right under both probe forms.
-- ---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS c_fs;
CREATE TABLE c_fs (id UInt64, v Array(FixedString(8)), s FixedString(8),
    INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO c_fs SELECT number,
    if(number = 7, [toFixedString('VALUE0', 8)], [toFixedString('VALUE0' || substring('AB', 1 + (number % 2), 1), 8)]),
    if(number = 7, toFixedString('VALUE0', 8), toFixedString('VALUE0' || substring('AB', 1 + (number % 2), 1), 8))
FROM numbers(64);

SELECT 'C-A7', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE hasAny(v, [toFixedString('VALUE0', 10)])) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'C-A8', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE hasAll(v, [toFixedString('VALUE0', 10)])) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'C-A9', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE has(v, toFixedString('VALUE0', 10))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'C-A12', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE s = toFixedString('VALUE0', 10)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'C-A13', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE s = 'VALUE0\0\0\0\0') WHERE explain ILIKE '%Granules: 1/64%';

-- The N3 (leave-unchanged) rows: id 7 holds a value with a REAL NUL tail, so trimming the probe would take each of
-- these from exact pruning to a full scan.
DROP TABLE IF EXISTS c_n3;
CREATE TABLE c_n3 (id UInt64, v Array(String), s String,
    INDEX idx_v v TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO c_n3 SELECT number,
    if(number = 7, ['VALUE0\0\0'], ['VALUE0' || substring('AB', 1 + (number % 2), 1)]),
    if(number = 7, 'VALUE0\0\0', 'VALUE0' || substring('AB', 1 + (number % 2), 1))
FROM numbers(64);

SELECT 'C-N3a', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_n3 WHERE has(v, toFixedString('VALUE0', 8))) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'C-N3b', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_n3 WHERE has([toFixedString('VALUE0', 8)], s)) WHERE explain ILIKE '%Granules: 1/64%';
SELECT 'C-N3c', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_n3 WHERE s = 'VALUE0\0\0') WHERE explain ILIKE '%Granules: 1/64%';

-- C-N3d: on the mapKeys redirect the constant becomes the map KEY while value_type still describes the map-value
-- operand, so selecting the primitive from value_type would trim a key whose NUL tail is genuine data.
DROP TABLE IF EXISTS c_n3d;
CREATE TABLE c_n3d (id UInt64, m Map(String, FixedString(8)),
    INDEX idx_k mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO c_n3d SELECT number,
    if(number = 7, map('VALUE0\0\0', toFixedString('x', 8)), map('VALUE0' || substring('AB', 1 + (number % 2), 1), toFixedString('y', 8)))
FROM numbers(64);
SELECT 'C-N3d', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_n3d WHERE m['VALUE0\0\0'] = toFixedString('x', 8)) WHERE explain ILIKE '%Granules: 1/64%';

-- C-N3e: a membership arm with a wide String needle. castColumn String->String is the identity while
-- FixedString->String strips, so the needle keeps its NULs and cannot match anything: reading a granule here is
-- the regression, not pruning to zero.
SELECT 'C-N3e', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE hasAny(v, ['VALUE0\0\0\0\0'])) WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'C-N3e2', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_fs WHERE has(v, 'VALUE0\0\0\0\0')) WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'C-N3f', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM c_n3 WHERE NOT (s != 'VALUE0\0\0')) WHERE explain ILIKE '%Granules: 1/64%';

-- On a String index the equals match set is the unbounded NUL-extension family, so there is no width to pad to and
-- the trimmed value is the only sound probe. On this adversarial fixture every filler shares all of its grams, so
-- reading all 64 granules is the CORRECT outcome here and not a regression: a padded probe would prune the only
-- matching granule away, which is what the correctness row below catches. These rows exist so a later reader does
-- not "optimize" this branch into a padded probe.
DROP TABLE IF EXISTS c_str;
DROP TABLE IF EXISTS c_str_log;
CREATE TABLE c_str (id UInt64, s String,
    INDEX idx_s s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE c_str_log (id UInt64, s String) ENGINE = Log;
INSERT INTO c_str SELECT number, if(number = 7, 'VALUE0', 'VALUE0' || substring('AB', 1 + (number % 2), 1)) FROM numbers(64);
INSERT INTO c_str_log SELECT number, if(number = 7, 'VALUE0', 'VALUE0' || substring('AB', 1 + (number % 2), 1)) FROM numbers(64);
SELECT 'A24', count() FROM c_str_log WHERE s = toFixedString('VALUE0', 10);
SELECT 'A24', count() FROM c_str     WHERE s = toFixedString('VALUE0', 10);
-- Asserted as "the matching granule is not pruned away" rather than as a `Granules: 64/64` match: the read step
-- prints its own `Granules: 64/64` line, so that pattern cannot distinguish the skip index keeping every granule
-- from the skip index simply having been consulted.
SELECT 'C-str', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM c_str WHERE s = toFixedString('VALUE0', 10)) WHERE explain ILIKE '%Granules: 0/64%';

-- ---------------------------------------------------------------------------------------------------
-- Direction D: the absent-key default-match cell is a different root cause, owned by
-- 2026-07-29-p2-tokenbfv1ngrambfv1-map-default-mat. It is wrong on master and stays wrong here, so it is pinned as
-- UNCHANGED rather than asserted against the oracle. This row is what proves the fix leaves it alone, and what
-- stops a later reader importing a second root cause into this change.
--
-- ⛔ The pinned D1 value is the WRONG answer, deliberately. An absent key yields the map value type's default, an
-- 8-NUL FixedString(8), which compares equal to toFixedString('', 3) under the zero-padded comparison, so the true
-- answer is 64 and not 0. The 0 comes from a separate guard that matches the constant against the value type's own
-- default and declines. D1 is pinned only to prove this change leaves that guard bit-identical to master: an all-NUL
-- constant is exactly the shape the trim path handles, so without this row a later widening of the normalization into
-- that guard would be silent. Do not "fix" D1 here; fixing it belongs to the task named above.
SELECT 'D1', count() FROM a_mv WHERE m['absent'] = toFixedString('', 3);
SELECT 'D2', count() FROM a_mk WHERE m[toFixedString('absent', 8)] = '';

DROP TABLE a_str;
DROP TABLE a_str_log;
DROP TABLE a_str_utf8;
DROP TABLE a_str_utf8_log;
DROP TABLE a_tok_utf8;
DROP TABLE a_tok_utf8_log;
DROP TABLE a_ip6;
DROP TABLE a_ip6_log;
DROP TABLE a_lc_utf8;
DROP TABLE a_lc_utf8_log;
DROP TABLE a_sparse;
DROP TABLE a_sparse_utf8;
DROP TABLE a_sparse_utf8_log;
DROP TABLE a_fs;
DROP TABLE a_fs_log;
DROP TABLE a_lc;
DROP TABLE a_lc_log;
DROP TABLE a_lcfs;
DROP TABLE a_lcfs_log;
DROP TABLE a_mk;
DROP TABLE a_mk_log;
DROP TABLE a_mv;
DROP TABLE a_mv_log;
DROP TABLE a_empty;
DROP TABLE a_empty_log;
DROP TABLE b_token;
DROP TABLE c_fs;
DROP TABLE c_n3;
DROP TABLE c_n3d;
DROP TABLE c_str;
DROP TABLE c_str_log;
DROP TABLE c_str_pfx;
DROP TABLE c_str_pfx_log;
