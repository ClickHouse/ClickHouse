-- Exercise the KQL `make-series` parser (Parsers/Kusto/ParserKQLMakeSeries.cpp):
-- parseAggregationColumns, parseFromToStepClause and makeSeries.
-- Each aggregation variant is tested in a separate query because the
-- generated SQL currently uses a shared alias `ga` and rejects multi-agg
-- forms. That is a server-side limitation, not a parser limitation.

-- CI randomizes the session timezone; pin it so the time-axis output is stable.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ms_num;
CREATE TABLE ms_num (x Int64, v Int64, g String) ENGINE = Memory;
INSERT INTO ms_num VALUES (0, 1, 'a'), (1, 2, 'a'), (2, 3, 'b'), (3, 4, 'b');

DROP TABLE IF EXISTS ms_time;
CREATE TABLE ms_time (ts DateTime, v Float64, g String) ENGINE = Memory;
INSERT INTO ms_time VALUES
    ('2020-01-01 00:00:00', 1.0, 'a'),
    ('2020-01-01 01:00:00', 2.0, 'a'),
    ('2020-01-01 02:00:00', 3.0, 'b'),
    ('2020-01-01 03:00:00', 4.0, 'b');

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- =========================================================================
-- Numeric axis, single aggregation
-- =========================================================================

print '--- numeric axis: step only, no from, no to, no group ---';
ms_num | make-series sum(v) on x step 1;

print '--- numeric axis: from + to + step ---';
ms_num | make-series sum(v) on x from 0 to 4 step 1;

print '--- numeric axis: from only ---';
ms_num | make-series sum(v) on x from 0 step 1;

print '--- numeric axis: to only ---';
ms_num | make-series sum(v) on x to 4 step 1;

print '--- numeric axis: aliased aggregation, explicit default ---';
ms_num | make-series s = sum(v) default=99 on x from 0 to 5 step 1;

print '--- numeric axis: group by ---';
ms_num | make-series sum(v) on x step 1 by g | sort by g asc;

-- =========================================================================
-- Time axis, timespan step (exercises the is_timespan branch in
-- parseFromToStepClause and the era_diff branch in makeSeries)
-- =========================================================================

print '--- time axis: from + to + 1h step ---';
ms_time | make-series sum(v) on ts from datetime(2020-01-01 00:00:00) to datetime(2020-01-01 04:00:00) step 1h;

print '--- time axis: from only (takes end from data) ---';
ms_time | make-series sum(v) on ts from datetime(2020-01-01 00:00:00) step 1h;

print '--- time axis: to only (takes start from data) ---';
ms_time | make-series sum(v) on ts to datetime(2020-01-01 04:00:00) step 1h;

print '--- time axis: no from, no to (start and end both from data) ---';
ms_time | make-series sum(v) on ts step 1h;

print '--- time axis: group by ---';
ms_time | make-series sum(v) on ts step 1h by g | sort by g asc;

-- =========================================================================
-- Exercise the `allowed_aggregation` set: every entry that resolves to a
-- real SQL function that this parser accepts.
-- =========================================================================

print '--- aggregation: avg ---';
ms_num | make-series avg(v) on x step 1;

print '--- aggregation: min ---';
ms_num | make-series min(v) on x step 1;

print '--- aggregation: max ---';
ms_num | make-series max(v) on x step 1;

print '--- aggregation: count ---';
ms_num | make-series count(v) on x step 1;

print '--- aggregation: sum ---';
ms_num | make-series sum(v) on x step 1;

-- =========================================================================
-- Error paths in parseAggregationColumns / parseFromToStepClause / parseImpl
-- =========================================================================

print '--- error: unknown aggregation function ---';
ms_num | make-series foo(v) on x step 1; -- { clientError SYNTAX_ERROR }

print '--- error: aggregation missing opening paren ---';
ms_num | make-series sum v on x step 1; -- { clientError SYNTAX_ERROR }

print '--- error: missing `on` keyword (no axis) ---';
ms_num | make-series sum(v) step 1; -- { clientError SYNTAX_ERROR }

print '--- error: missing `step` keyword ---';
ms_num | make-series sum(v) on x from 0; -- { clientError SYNTAX_ERROR }

-- These exercise aggregation names the parser accepts but whose underlying
-- SQL function doesn't exist in ClickHouse - they must parse OK and fail at
-- analysis time. This keeps parseAggregationColumns' allowed set covered.
print '--- server-side error: stdev resolves to unknown SQL function ---';
ms_num | make-series stdev(v) on x step 1; -- { serverError UNKNOWN_FUNCTION }

print '--- server-side error: dcount resolves to unknown SQL function ---';
ms_num | make-series dcount(v) on x step 1; -- { serverError UNKNOWN_FUNCTION }

print '--- server-side error: variance resolves to unknown SQL function ---';
ms_num | make-series variance(v) on x step 1; -- { serverError UNKNOWN_FUNCTION }

print '--- server-side error: percentile resolves to unknown SQL function ---';
ms_num | make-series percentile(v) on x step 1; -- { serverError UNKNOWN_FUNCTION }

print '--- server-side error: take_any resolves to unknown SQL function ---';
ms_num | make-series take_any(v) on x step 1; -- { serverError UNKNOWN_FUNCTION }
