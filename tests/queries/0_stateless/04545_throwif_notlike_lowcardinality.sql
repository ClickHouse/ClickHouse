-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/110852
-- throwIf(notLike(LowCardinality(...), ...)) used to throw unconditionally: the default
-- LowCardinality implementation ran throwIf on the whole dictionary, which always contains
-- the reserved default value ('') even when no row references it. notLike('', pattern)=1 for a
-- non-matching pattern, so throwIf saw that spurious 1 and threw for values absent from the data.

DROP TABLE IF EXISTS t_throwif_lc;
CREATE TABLE t_throwif_lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_throwif_lc VALUES ('foo');

-- notLike('foo', '%foo%') = 0 -> must NOT throw (this was the bug: it threw).
SELECT throwIf(notLike(s, '%foo%'), 'unexpected throw for foo') FROM t_throwif_lc;
-- notLike('foo', '%bar%') = 1 -> must throw.
SELECT throwIf(notLike(s, '%bar%'), 'expected throw for bar') FROM t_throwif_lc; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- like inside throwIf stayed correct before and after the fix.
SELECT throwIf(like(s, '%bar%'), 'unexpected throw like bar') FROM t_throwif_lc;
SELECT throwIf(like(s, '%foo%'), 'expected throw like foo') FROM t_throwif_lc; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Multi-row: every row matches '%a%', so notLike is 0 for all real rows; only the unused
-- dictionary default would be non-zero. Must NOT throw.
DROP TABLE IF EXISTS t_throwif_lc_multi;
CREATE TABLE t_throwif_lc_multi (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_throwif_lc_multi VALUES ('a1'), ('a2');
SELECT sum(throwIf(notLike(s, '%a%'), 'unexpected throw multi')) FROM t_throwif_lc_multi;

-- LowCardinality(Nullable(String)) wrapper.
DROP TABLE IF EXISTS t_throwif_lcn;
CREATE TABLE t_throwif_lcn (s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY s SETTINGS allow_nullable_key = 1;
INSERT INTO t_throwif_lcn VALUES ('foo');
SELECT throwIf(notLike(s, '%foo%'), 'unexpected throw lcn foo') FROM t_throwif_lcn;

DROP TABLE t_throwif_lc;
DROP TABLE t_throwif_lc_multi;
DROP TABLE t_throwif_lcn;
