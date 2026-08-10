-- Text and token skip indexes tokenize the raw bytes of the indexed column, so index analysis has
-- to convert the compared constant into that encoding. Without it, an `IPv6` column probed with a
-- string literal was searched for the bytes of the literal text, and the matching granule was
-- dropped. The oracle for a row count is the same query with the index ignored, and
-- `force_data_skipping_indices` makes a silently declined atom an error instead of a full scan.

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE t_ngram (ip IPv6, INDEX idx ip TYPE ngrambf_v1(3, 512, 3, 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ngram VALUES ('2001:db8::');

SELECT '-- the matching row is found';
SELECT count() FROM t_ngram WHERE ip = '2001:db8::' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ngram WHERE ip = '2001:db8::' SETTINGS ignore_data_skipping_indices = 'idx';

-- Assert the presence of the pruning line. Asserting a full-scan line instead would be vacuous:
-- the primary key emits its own unconditional one.
SELECT '-- a non-matching address still prunes, so pruning is not disabled outright';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE ip = 'dead:beef::1') WHERE explain ILIKE '%Granules: 0/1%';

SELECT '-- the other index types over the same column';
CREATE TABLE t_token (ip IPv6, INDEX idx ip TYPE tokenbf_v1(512, 3, 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_token VALUES ('2001:db8::');
SELECT count() FROM t_token WHERE ip = '2001:db8::' SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE t_sparse (ip IPv6, INDEX idx ip TYPE sparse_grams(3, 100, 512, 3, 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sparse VALUES ('2001:db8::');
SELECT count() FROM t_sparse WHERE ip = '2001:db8::' SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE t_lc (ip LowCardinality(IPv6), INDEX idx ip TYPE ngrambf_v1(3, 512, 3, 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc VALUES ('2001:db8::');
SELECT count() FROM t_lc WHERE ip = '2001:db8::' SETTINGS force_data_skipping_indices = 'idx';

-- An `IN` set built from a subquery carries no type check against the index, unlike a literal tuple.
-- The multi-element arm puts the matching address last, so converting only the first element fails.
SELECT '-- an IN set from a subquery needs the same conversion';
SELECT count() FROM t_ngram WHERE ip IN (SELECT '2001:db8::') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_ngram WHERE ip IN (SELECT arrayJoin(['dead:beef::1', '::1', '2001:db8::'])) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE ip IN (SELECT 'dead:beef::1')) WHERE explain ILIKE '%Granules: 0/1%';

-- Here the converted value is the map key, so the conversion needs the key type. Given the compared
-- value's type instead, a `FixedString(16)` is read as a binary address and decodes to wrong bytes.
SELECT '-- the map key subcolumn is converted with the key type, not the compared type';
CREATE TABLE t_map (m Map(IPv6, String), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_map VALUES (map(toIPv6('2001:db8::abcd:1'), 'v'));
SELECT count() FROM t_map WHERE m.`key_2001:db8::abcd:1` = 'v' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_map WHERE m.`key_2001:db8::abcd:1` = toFixedString('v', 16) SETTINGS force_data_skipping_indices = 'idx';

-- On a `String` domain the constant is already in the index encoding, so it must not be routed
-- through a conversion: one wider than the column has no representation there and would decline.
SELECT '-- a FixedString column keeps pruning for a constant wider than the column';
CREATE TABLE t_fixed (s FixedString(8), INDEX idx s TYPE ngrambf_v1(3, 512, 3, 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fixed VALUES ('needle');
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fixed WHERE s = 'waytoolongvalue') WHERE explain ILIKE '%Granules: 0/1%';
