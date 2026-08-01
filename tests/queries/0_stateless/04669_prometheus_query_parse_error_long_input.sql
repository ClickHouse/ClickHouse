-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated and the cleanup query hangs.
--
-- A PromQL query with a long tail of unrecognized characters (e.g. a FixedString
-- padded with NUL bytes) must fail fast. The lexer used to recover from each bad
-- byte one at a time, reporting an error whose position was computed by scanning
-- the query from its start, which was quadratic in the query length and kept a
-- thread busy for tens of minutes on a megabyte-sized input.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT count() FROM prometheusQuery(ts, toFixedString('rate(up[2d])', 1048577), 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT count() FROM prometheusQueryRange(ts, toFixedString('up', 1048577), 1000, 2000, 10); -- { serverError CANNOT_PARSE_PROMQL_QUERY }

DROP TABLE ts;
