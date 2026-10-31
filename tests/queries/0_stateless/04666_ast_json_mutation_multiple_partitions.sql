-- The multi-partition mutation scope (`IN PARTITION p1, p2, ...`) is carried by a separate `partitions`
-- slot next to the single-partition `partition` slot. It has to survive the `clickhouse_json` AST
-- round-trip, otherwise the deserialized query silently widens the mutation to the whole table.

SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION 1, 2 WHERE x = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t UPDATE y = 1 IN PARTITION 1, 2, 3 WHERE x = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('DELETE FROM t IN PARTITION 1, 2 WHERE x = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE t SET y = 1 IN PARTITION 1, 2 WHERE x = 1'));

-- Tuple and `ID` partition forms, and the single-partition form, which the parser collapses into the
-- `partition` slot and which must keep round-tripping unchanged.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION (1, \'a\'), (2, \'b\') WHERE x = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION ID \'p1\', ID \'p2\' WHERE x = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION 1 WHERE x = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('DELETE FROM t IN PARTITION 1 WHERE x = 1'));

-- Shapes the SQL parser can never produce must be rejected at the JSON boundary.

-- A `partitions` list with a foreign element type: `getPartitionIDFromQuery` downcasts every element
-- with `->as<ASTPartition &>()`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION 1, 2 WHERE x = 1'), '{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":2}},"all":false,"fields_count":1}', '{"type":"Literal","value":{"field_type":"UInt64","value":2}}')); -- { serverError BAD_ARGUMENTS }

-- A list shorter than two elements: the parser collapses a one-element list into `partition`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION 1, 2 WHERE x = 1'), ',{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":2}},"all":false,"fields_count":1}', '')); -- { serverError BAD_ARGUMENTS }

-- A `partitions` slot that is not a list at all.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION 1, 2 WHERE x = 1'), '"partitions":{"type":"ExpressionList"', '"partitions":{"type":"Function","name":"f"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('DELETE FROM t IN PARTITION 1, 2 WHERE x = 1'), '"partitions":{"type":"ExpressionList"', '"partitions":{"type":"Function","name":"f"')); -- { serverError BAD_ARGUMENTS }

-- `partitions` on a command that has no multi-partition form.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP COLUMN x'), '"column":{"type":"Identifier","name":"x"}', '"column":{"type":"Identifier","name":"x"},"partitions":{"type":"ExpressionList","children":[{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":1}},"all":false,"fields_count":1},{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":2}},"all":false,"fields_count":1}]}')); -- { serverError BAD_ARGUMENTS }

-- Both the single- and the multi-partition slot at the same time.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DELETE IN PARTITION 1, 2 WHERE x = 1'), '"partitions":{"type":"ExpressionList"', '"partition":{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":3}},"all":false,"fields_count":1},"partitions":{"type":"ExpressionList"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('DELETE FROM t IN PARTITION 1, 2 WHERE x = 1'), '"partitions":{"type":"ExpressionList"', '"partition":{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":3}},"all":false,"fields_count":1},"partitions":{"type":"ExpressionList"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('UPDATE t SET y = 1 IN PARTITION 1, 2 WHERE x = 1'), '"partitions":{"type":"ExpressionList"', '"partition":{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":3}},"all":false,"fields_count":1},"partitions":{"type":"ExpressionList"')); -- { serverError BAD_ARGUMENTS }
