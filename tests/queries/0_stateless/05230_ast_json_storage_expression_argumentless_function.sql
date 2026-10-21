-- `KeyDescription::getKeyFromAST`, `IndexDescription::getIndexFromAST` and `TTLDescription::getTTLFromAST`
-- analyse a storage-metadata expression with the legacy `TreeRewriter`, which dereferences
-- `ASTFunction::arguments` before any arity check. The parser fills that list inside an expression and
-- leaves it absent outside one, so `ASTFunction::readJSON` cannot require it and each expression slot
-- rejects the parser-impossible node itself. The rejections live in 05231, which reaches those analysers;
-- this test pins what must keep round-tripping, argument-less functions included.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8 CODEC(LZ4), c UInt8 STATISTICS(tdigest), d DateTime TTL d + toIntervalDay(1), INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY a % 8 PRIMARY KEY a ORDER BY (a, b) SAMPLE BY a TTL d + toIntervalDay(2) DELETE WHERE a > 0'));

-- `setNoEmptyArgs` erases the empty argument list of `MergeTree()`, `tuple()` keeps its own, and the
-- deprecated positional arguments become key expressions, so the engine slot is screened through its
-- arguments while the engine node itself stays argument-less.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree() ORDER BY tuple()'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (d Date, x UInt64) ENGINE = MergeTree(d, x, 8192)'));

-- The `ALTER` slots, the `Assignment` reader (`GROUP BY ... SET` and `UPDATE`), and a `RECOMPRESS` codec,
-- which `ASTTTLElement` holds outside `IAST::children` so the recursive screen never reaches it.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY ORDER BY (a, b), MODIFY SAMPLE BY a, ADD INDEX i a TYPE minmax GRANULARITY 1, UPDATE a = a + 1 WHERE a > 0, MODIFY TTL d + toIntervalDay(1) RECOMPRESS CODEC(LZ4)'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY TTL d + toIntervalDay(1) GROUP BY a SET d = max(d)'));

-- A column `DEFAULT` is deliberately left out of the screen, measured non-fatal: it reaches an analyser
-- that checks arity first. It keeps formatting, so this is the column that changes if a later PR widens
-- the screen; `countSubstrings` keeps it honest by proving the payload really lost its argument list.
SELECT
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8 DEFAULT a IN (1)) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}') = 1
        AND length(formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8 DEFAULT a IN (1)) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', ''))) > 0;
