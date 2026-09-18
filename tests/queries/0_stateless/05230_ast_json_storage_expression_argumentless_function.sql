-- A storage-metadata expression is consumed before analysis: `KeyDescription::getKeyFromAST`,
-- `IndexDescription::getIndexFromAST` and `TTLDescription::getTTLFromAST` run the legacy
-- `TreeRewriter`/`ExpressionAnalyzer` over the raw AST, where `MarkTableIdentifiersMatcher` (for the
-- `in` family) and `ActionsMatcher` (for `arrayJoin`/`grouping`) intercept the function name before any
-- arity check and dereference `ASTFunction::arguments`. Inside an expression the parser always fills
-- that list, because it builds a function only after an opening parenthesis; outside one it does not,
-- which is why a table engine, a `CODEC`/`STATISTICS` element and an index `TYPE` are argument-less
-- functions and `ASTFunction::readJSON` cannot require an `"arguments"` member on its own. Each of
-- these slots therefore rejects the parser-impossible node itself.

-- Parser-produced shapes stay accepted, one per screened slot; the reference pins what they format to.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8) ENGINE = MergeTree PRIMARY KEY a ORDER BY (a, b)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a SAMPLE BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) DELETE WHERE a > 0'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime TTL d + toIntervalDay(1)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (MODIFY ORDER BY (a, b))'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (MODIFY SAMPLE BY a)'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (MODIFY TTL d + toIntervalDay(1))'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (MODIFY TTL d + toIntervalDay(1) RECOMPRESS CODEC(ZSTD(3)))'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (MODIFY TTL d + toIntervalDay(1) GROUP BY a SET d = max(d))'));
SELECT formatQueryFromJSON(parseQueryToJSON('INSERT INTO TABLE FUNCTION file(''p_{_partition_id}.csv'', ''CSV'', ''a UInt8'') PARTITION BY a SELECT 1'));

-- The engine slot is screened through its ARGUMENTS only. The deprecated positional
-- `MergeTree(date, key, granularity)` arguments become key expressions in `registerStorageMergeTree`,
-- while the engine node itself is argument-less by construction, and an argument-less function in an
-- argument position keeps an empty (but present) `arguments` list, so both must still round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (d Date, x UInt64) ENGINE = MergeTree(d, x, 8192)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (d Date, a UInt8, b UInt8) ENGINE = SummingMergeTree(d, (a, b), 8192)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = Distributed(''c'', currentDatabase(), ''t'')'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = Distributed(''c'', ''d'', ''t'', rand())'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = Memory()'));

-- The `Assignment` node's expression child is screened through its own reader, because
-- `ASTTTLElement::group_by_assignments` is held outside `IAST::children` and no container-level screen
-- reaches it. That reader also serves `ALTER ... UPDATE` and the standalone `UPDATE`, so all three
-- must keep accepting valid assignments.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) GROUP BY a SET d = max(d)'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (UPDATE a = a + 1 WHERE a > 0)'));
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE t SET a = a + 1 WHERE a > 0'));

-- Slots that legitimately hold an argument-less function, i.e. what an over-broad screen would break:
-- a table engine (`ParserIdentifierWithOptionalParameters`, and `setNoEmptyArgs` erases the empty list
-- of `MergeTree()`), the elements of a `CODEC`/`STATISTICS` list, an index and a statistics `TYPE`, a
-- projection type, and a `BACKUP` destination. All must keep round-tripping unchanged.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree() ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8 CODEC(LZ4), b UInt8 CODEC(LZ4, ZSTD(3), Delta), c UInt8 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, INDEX i a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (ADD STATISTICS a TYPE tdigest)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) RECOMPRESS CODEC(ZSTD(3))'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP TABLE t TO Disk(''backups'', ''f'')'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (UNLOCK SNAPSHOT ''s'' FROM s3(''u'', ''k'', ''s''))'));

-- An argument list that is present but empty is a shape the parser DOES produce, so it stays accepted.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY tuple()'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY count()'));

-- Each deletion below must leave a parseable document holding exactly one copy of the member, otherwise
-- a negative could pass because the JSON parser rejected the payload, or because `replace` silently
-- edited a different node (or none).
SELECT
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a PARTITION BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a PARTITION BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8) ENGINE = MergeTree PRIMARY KEY a IN (1) ORDER BY b'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8) ENGINE = MergeTree PRIMARY KEY a IN (1) ORDER BY b'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a SAMPLE BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a SAMPLE BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]},"is_operator":true}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]},"is_operator":true}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL grouping(a)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL grouping(a)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) DELETE WHERE a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) DELETE WHERE a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime TTL arrayJoin([d])) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime TTL arrayJoin([d])) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}'),
    isValidJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY ORDER BY a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('ALTER TABLE t (MODIFY ORDER BY a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY SAMPLE BY a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('ALTER TABLE t (MODIFY SAMPLE BY a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) GROUP BY a SET d = max(arrayJoin([d]))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) GROUP BY a SET d = max(arrayJoin([d]))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY (a, a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY (a, a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}');

-- One negative per screened slot. `ORDER BY`/`PRIMARY KEY`/`SAMPLE BY`/`PARTITION BY` and the TTL
-- `DELETE WHERE` predicate abort in `checkFunctionIsInOrGlobalInOperator`; the skip index, the table
-- and column TTL expressions and a TTL `GROUP BY ... SET` assignment abort in `ActionsMatcher::visit`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a PARTITION BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8) ENGINE = MergeTree PRIMARY KEY a IN (1) ORDER BY b'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a SAMPLE BY a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]},"is_operator":true}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL grouping(a)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) DELETE WHERE a IN (1)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime TTL arrayJoin([d])) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY ORDER BY a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY SAMPLE BY a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }

-- The `Assignment` negative has to be NESTED. Unnested, `TTLDescription::getTTLFromAST` answers
-- "Invalid expression for assignment of column" from a shape check that runs before analysis, so the
-- assertion would pass on an unpatched server and prove nothing.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + toIntervalDay(1) GROUP BY a SET d = max(arrayJoin([d]))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}', '')); -- { serverError BAD_ARGUMENTS }

-- The screen walks the whole slot subtree, so a node nested inside a larger key expression is rejected
-- as well, not only one sitting at the slot's root.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY (a, a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }

-- The same guard for the three payloads below, the last of which replaces a whole node rather than
-- deleting a member: each `from` string must occur exactly once, and the result must still parse.
SELECT
    isValidJSON(replace(parseQueryToJSON('INSERT INTO TABLE FUNCTION file(''p_{_partition_id}.csv'', ''CSV'', ''a UInt8'') PARTITION BY a IN (1) SELECT 1'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('INSERT INTO TABLE FUNCTION file(''p_{_partition_id}.csv'', ''CSV'', ''a UInt8'') PARTITION BY a IN (1) SELECT 1'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (d Date, x UInt64) ENGINE = MergeTree(d, x IN (1), 8192)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (d Date, x UInt64) ENGINE = MergeTree(d, x IN (1), 8192)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'),
    isValidJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY TTL d + toIntervalDay(1))'), '{"type":"TTLElement","mode":"DELETE","destination_type":"DELETE","if_exists":false,"ttl_expr":{"type":"Function","name":"plus","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"},{"type":"Function","name":"toIntervalDay","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"is_operator":true}}', '{"type":"Function","name":"in"}')),
    countSubstrings(parseQueryToJSON('ALTER TABLE t (MODIFY TTL d + toIntervalDay(1))'), '{"type":"TTLElement","mode":"DELETE","destination_type":"DELETE","if_exists":false,"ttl_expr":{"type":"Function","name":"plus","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"},{"type":"Function","name":"toIntervalDay","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"is_operator":true}}');

-- `INSERT INTO FUNCTION ... PARTITION BY` reaches `KeyDescription::getKeyFromAST` through
-- `StorageFile::write` -> `PartitionStrategyFactory`, and the deprecated positional engine arguments
-- reach it through `registerStorageMergeTree`. Both are new in this screen.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('INSERT INTO TABLE FUNCTION file(''p_{_partition_id}.csv'', ''CSV'', ''a UInt8'') PARTITION BY a IN (1) SELECT 1'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (d Date, x UInt64) ENGINE = MergeTree(d, x IN (1), 8192)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', '')); -- { serverError BAD_ARGUMENTS }

-- `ALTER ... MODIFY TTL` needs its whole list element replaced, not a member deleted: the CREATE side
-- requires every `ttl_table` child to be an `ASTTTLElement`, the ALTER side imposed no type, and
-- `TTLDescription::getTTLFromAST` reads a foreign child as a column TTL and analyses it as a bare
-- expression. The first payload puts an argument-less function there (the crash), the second an
-- identifier, which only the restored list-of-`ASTTTLElement` contract can answer.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY TTL d + toIntervalDay(1))'), '{"type":"TTLElement","mode":"DELETE","destination_type":"DELETE","if_exists":false,"ttl_expr":{"type":"Function","name":"plus","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"},{"type":"Function","name":"toIntervalDay","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"is_operator":true}}', '{"type":"Function","name":"in"}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY TTL d + toIntervalDay(1))'), '{"type":"TTLElement","mode":"DELETE","destination_type":"DELETE","if_exists":false,"ttl_expr":{"type":"Function","name":"plus","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"},{"type":"Function","name":"toIntervalDay","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"is_operator":true}}', '{"type":"Identifier","name":"d"}')); -- { serverError BAD_ARGUMENTS }

-- `ALTER ... UPDATE` shares `ASTAssignment`'s reader with the TTL `GROUP BY ... SET` route above. This
-- shape is not fatal today (the mutation analyser checks arity first), so this row records that the
-- shared reader screens it too rather than claiming a crash.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (UPDATE d = max(arrayJoin([d])) WHERE a > 0)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}', '')); -- { serverError BAD_ARGUMENTS }

-- Slots deliberately left out of this screen, each measured non-fatal: a column `DEFAULT` and the
-- `ALTER ... DELETE WHERE` predicate reach an analyser that checks arity first, and `UNIQUE KEY` is an
-- identifier-list contract rather than an expression key. They keep formatting, so this is the row that
-- will change if a later PR widens the screen.
SELECT length(formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8 DEFAULT a IN (1)) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', ''))) > 0;
SELECT length(formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (DELETE WHERE a IN (1))'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}', ''))) > 0;
