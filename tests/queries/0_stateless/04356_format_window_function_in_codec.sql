-- A function with no arguments inside a CODEC (or engine) is normally formatted without parentheses
-- (e.g. NONE, LZ4, MergeTree). A no-argument *window* function, however, must keep its parentheses:
-- `cume_dist() OVER (...)` re-parses with the empty `()`, so dropping them made the formatting inconsistent
-- across a parse round-trip ("Inconsistent AST formatting"). Check that such a query is now idempotent under
-- re-formatting and keeps the parentheses, while plain zero-argument codecs/engines still drop them.

-- Idempotent under re-formatting (this is what the AST-consistency check verifies):
SELECT formatQuery(formatQuery($$CREATE TABLE t (`i` Int16 CODEC(cume_dist() OVER (ORDER BY x), NONE)) ENGINE = MergeTree ORDER BY i$$))
     = formatQuery($$CREATE TABLE t (`i` Int16 CODEC(cume_dist() OVER (ORDER BY x), NONE)) ENGINE = MergeTree ORDER BY i$$);

-- The window function keeps its parentheses.
SELECT formatQuery($$CREATE TABLE t (`i` Int16 CODEC(cume_dist() OVER (ORDER BY x ASC, b DESC), NONE)) ENGINE = MergeTree ORDER BY i$$) LIKE '%cume_dist() OVER%';

-- Regressions: a plain zero-argument codec/engine still formats without parentheses.
SELECT formatQuery($$CREATE TABLE t (`i` Int16 CODEC(NONE)) ENGINE = MergeTree() ORDER BY i$$) LIKE '%CODEC(NONE)%';
SELECT formatQuery($$CREATE TABLE t (`i` Int16 CODEC(NONE)) ENGINE = MergeTree() ORDER BY i$$) NOT LIKE '%NONE()%';
SELECT formatQuery($$CREATE TABLE t (`i` Int16 CODEC(NONE)) ENGINE = MergeTree() ORDER BY i$$) NOT LIKE '%MergeTree()%';

-- A normal window function in a SELECT keeps its parentheses, as before.
SELECT formatQuery($$SELECT count() OVER (ORDER BY x)$$) LIKE '%count() OVER%';
