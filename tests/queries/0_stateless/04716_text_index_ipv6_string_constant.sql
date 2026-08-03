-- Text/token skip indexes must build their probe from bytes of the indexed column's own encoding.
-- The Log twin is the oracle for every row count; the granule assertions catch over-pruning, which
-- a correct-answer check alone is blind to once the index has already dropped the granule.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_oracle;
CREATE TABLE t_oracle (k UInt64, ip IPv6) ENGINE = Log;
INSERT INTO t_oracle
SELECT
    number,
    multiIf(number = 7, toIPv6('2001:db8::'),
            number = 8, toIPv6('::1'),
            number = 9, toIPv6('::ffff:1.2.3.4'),
            toIPv6('2001:db8:1:2:3:4:5:' || hex(number + 256)))
FROM numbers(64);

DROP TABLE IF EXISTS t_ngram;
CREATE TABLE t_ngram (k UInt64, ip IPv6, INDEX idx ip TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_ngram SELECT * FROM t_oracle;

DROP TABLE IF EXISTS t_token;
CREATE TABLE t_token (k UInt64, ip IPv6, INDEX idx ip TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_token SELECT * FROM t_oracle;

DROP TABLE IF EXISTS t_sparse;
CREATE TABLE t_sparse (k UInt64, ip IPv6, INDEX idx ip TYPE sparse_grams(3, 100, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_sparse SELECT * FROM t_oracle;

DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (k UInt64, ip LowCardinality(IPv6), INDEX idx ip TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_lc SELECT * FROM t_oracle;

SELECT '-- A: matching row is found, per index type';
SELECT 'A-oracle', count() FROM t_oracle WHERE ip = '2001:db8::';
SELECT 'A-ngram', count() FROM t_ngram WHERE ip = '2001:db8::';
SELECT 'A-token', count() FROM t_token WHERE ip = '2001:db8::';
SELECT 'A-sparse', count() FROM t_sparse WHERE ip = '2001:db8::';
SELECT 'A-lc', count() FROM t_lc WHERE ip = '2001:db8::';

SELECT '-- A: the skip index does not prune the matching granule away';
-- Assert the ABSENCE of the over-pruning line. Asserting the presence of `Granules: 64/64` would be
-- vacuous: the primary key emits its own unconditional full-scan line.
SELECT 'A-ngram-overprune', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE ip = '2001:db8::') WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'A-token-overprune', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_token WHERE ip = '2001:db8::') WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'A-sparse-overprune', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE ip = '2001:db8::') WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'A-lc-overprune', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc WHERE ip = '2001:db8::') WHERE explain ILIKE '%Granules: 0/64%';

SELECT '-- A: the same defect via the map-key subcolumn, whose serialized key is also a String constant';
DROP TABLE IF EXISTS m_oracle;
CREATE TABLE m_oracle (k UInt64, m Map(IPv6, String)) ENGINE = Log;
INSERT INTO m_oracle SELECT number, map(if(number = 7, toIPv6('2001:db8::'), toIPv6('::2')), 'v') FROM numbers(16);

DROP TABLE IF EXISTS m_idx;
CREATE TABLE m_idx (k UInt64, m Map(IPv6, String), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO m_idx SELECT * FROM m_oracle;

SELECT 'A-mapkey', (SELECT count() FROM m_oracle WHERE m.`key_2001:db8::` = 'v'), (SELECT count() FROM m_idx WHERE m.`key_2001:db8::` = 'v');
SELECT 'A-mapkey-overprune', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM m_idx WHERE m.`key_2001:db8::` = 'v') WHERE explain ILIKE '%Granules: 0/16%';
SELECT 'A-mapkey-absent-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM m_idx WHERE m.`key_dead:beef::1` = 'v') WHERE explain ILIKE '%Granules: 0/16%';

SELECT '-- B: a non-matching address still prunes, so pruning is not blanket-disabled';
SELECT 'B-rows', count() FROM t_ngram WHERE ip = 'dead:beef::1';
SELECT 'B-ngram-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram WHERE ip = 'dead:beef::1') WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'B-token-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_token WHERE ip = 'dead:beef::1') WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'B-sparse-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sparse WHERE ip = 'dead:beef::1') WHERE explain ILIKE '%Granules: 0/64%';
SELECT 'B-lc-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc WHERE ip = 'dead:beef::1') WHERE explain ILIKE '%Granules: 0/64%';

SELECT '-- C: address spellings agree with the oracle (oracle, indexed)';
SELECT 'C-expanded', (SELECT count() FROM t_oracle WHERE ip = '2001:0db8:0000:0000:0000:0000:0000:0000'), (SELECT count() FROM t_ngram WHERE ip = '2001:0db8:0000:0000:0000:0000:0000:0000');
SELECT 'C-uppercase', (SELECT count() FROM t_oracle WHERE ip = '2001:DB8::'), (SELECT count() FROM t_ngram WHERE ip = '2001:DB8::');
SELECT 'C-loopback', (SELECT count() FROM t_oracle WHERE ip = '::1'), (SELECT count() FROM t_ngram WHERE ip = '::1');
SELECT 'C-all-zero', (SELECT count() FROM t_oracle WHERE ip = '::'), (SELECT count() FROM t_ngram WHERE ip = '::');
SELECT 'C-v4-mapped', (SELECT count() FROM t_oracle WHERE ip = '::ffff:1.2.3.4'), (SELECT count() FROM t_ngram WHERE ip = '::ffff:1.2.3.4');
SELECT 'C-v4-bare', (SELECT count() FROM t_oracle WHERE ip = '1.2.3.4'), (SELECT count() FROM t_ngram WHERE ip = '1.2.3.4');
SELECT 'C-typed', (SELECT count() FROM t_oracle WHERE ip = toIPv6('2001:db8::')), (SELECT count() FROM t_ngram WHERE ip = toIPv6('2001:db8::'));
SELECT 'C-not-equals', (SELECT count() FROM t_oracle WHERE ip != '2001:db8::'), (SELECT count() FROM t_ngram WHERE ip != '2001:db8::');

SELECT '-- D: predicates and column types that never reach index analysis stay rejected';
SELECT count() FROM t_ngram WHERE ip LIKE '2001%'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_ngram WHERE startsWith(ip, '2001'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_ngram WHERE hasToken(ip, '2001'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_ngram WHERE match(ip, '2001'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_ngram WHERE has([ip], '2001:db8::'); -- { serverError NO_COMMON_TYPE }
CREATE TABLE t_bad_nullable (k UInt64, ip Nullable(IPv6), INDEX idx ip TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad_v4 (k UInt64, ip IPv4, INDEX idx ip TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad_text (k UInt64, ip IPv6, INDEX idx ip TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

SELECT '-- E: the String and FixedString domains are unaffected';
DROP TABLE IF EXISTS s_oracle;
CREATE TABLE s_oracle (k UInt64, s String) ENGINE = Log;
INSERT INTO s_oracle SELECT number, if(number = 3, 'needle', 'filler' || toString(number)) FROM numbers(16);

DROP TABLE IF EXISTS s_idx;
CREATE TABLE s_idx (k UInt64, s String, INDEX idx s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO s_idx SELECT * FROM s_oracle;

DROP TABLE IF EXISTS f_idx;
CREATE TABLE f_idx (k UInt64, s FixedString(8), INDEX idx s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO f_idx SELECT * FROM s_oracle;

SELECT 'E-string', (SELECT count() FROM s_oracle WHERE s = 'needle'), (SELECT count() FROM s_idx WHERE s = 'needle');
SELECT 'E-string-overprune', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM s_idx WHERE s = 'needle') WHERE explain ILIKE '%Granules: 0/16%';
SELECT 'E-string-neg-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM s_idx WHERE s = 'absentxx') WHERE explain ILIKE '%Granules: 0/16%';
SELECT 'E-fixedstring', (SELECT count() FROM s_oracle WHERE s = 'needle'), (SELECT count() FROM f_idx WHERE s = 'needle');
SELECT 'E-fixedstring-neg-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM f_idx WHERE s = 'absentxx') WHERE explain ILIKE '%Granules: 0/16%';
-- A constant wider than the FixedString still prunes: on this domain the constant's own bytes are
-- already the index encoding, so it must not be routed through a conversion that cannot represent it.
SELECT 'E-fixedstring-oversize', (SELECT count() FROM s_oracle WHERE s = 'waytoolongvalue'), (SELECT count() FROM f_idx WHERE s = 'waytoolongvalue');
SELECT 'E-fixedstring-oversize-prunes', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM f_idx WHERE s = 'waytoolongvalue') WHERE explain ILIKE '%Granules: 0/16%';

DROP TABLE t_oracle;
DROP TABLE t_ngram;
DROP TABLE t_token;
DROP TABLE t_sparse;
DROP TABLE t_lc;
DROP TABLE m_oracle;
DROP TABLE m_idx;
DROP TABLE s_oracle;
DROP TABLE s_idx;
DROP TABLE f_idx;
