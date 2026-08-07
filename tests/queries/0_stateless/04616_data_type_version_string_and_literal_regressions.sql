-- Regression tests locking in behavior already confirmed correct during review of
-- ClickHouse/ClickHouse#111308. No production code changes accompany this file.

SET allow_experimental_version_type = 1;

-- (1) Implicit string -> Version comparison, via convertFieldToType's generic string fallback
--     (the same mechanism IPv4/IPv6 use) -- both against a real table column and as a scalar
--     expression.
DROP TABLE IF EXISTS version_cmp_test;
CREATE TABLE version_cmp_test (id UInt32, ver Version) ENGINE = MergeTree ORDER BY id;
INSERT INTO version_cmp_test VALUES (1, '1.2.0.0'), (2, '2.0.0.0');
SELECT throwIf(count() != 1, 'FAIL: WHERE ver = shorthand string') FROM version_cmp_test WHERE ver = '1.2';
SELECT throwIf(count() != 1, 'FAIL: WHERE ver = full-form string') FROM version_cmp_test WHERE ver = '2.0.0.0';
DROP TABLE version_cmp_test;

SELECT throwIf(NOT (toVersion('1.2.3.4') = '1.2.3.4'), 'FAIL: scalar implicit string comparison, equal');
SELECT throwIf(toVersion('1.2.3.4') = '1.2.3.5', 'FAIL: scalar implicit string comparison, not-equal case');

-- (2) A bare string literal stays String -- it is never auto-inferred as Version, neither via
--     schema inference nor literal type inference.
DROP TABLE IF EXISTS version_literal_inference_test;
CREATE TABLE version_literal_inference_test (v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO version_literal_inference_test VALUES ('1.2.3');
SELECT throwIf(toTypeName(v) != 'String', 'FAIL: bare string literal in VALUES must stay String') FROM version_literal_inference_test;
DROP TABLE version_literal_inference_test;
SELECT throwIf(toTypeName('1.2.3.4') != 'String', 'FAIL: bare string literal type inference');

-- (3) Version compared against a bare integer literal has no common supertype and fails loudly
--     (safe failure, not silent wrong data). TypeIndex::Version is deliberately NOT folded into
--     UInt128 the way TypeIndex::IPv4 is folded into UInt32 in getLeastSupertype.cpp's
--     getNumericType().
SELECT toVersion('1.2.3.4') = 16909060; -- { serverError NO_COMMON_TYPE }

SET allow_experimental_version_type = 0;
