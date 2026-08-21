-- `ParserPartition` produces exactly three mutually exclusive shapes (`ALL`, a literal/tuple/substitution
-- `value`, or a string-literal/substitution `id`), and `MergeTreeData::getPartitionIDFromQuery` later
-- downcasts those slots unconditionally, so `readJSON` must reject any other shape at the boundary.
-- Likewise `ParserUserNamesWithHost` requires at least one user name (`formatImpl` asserts a non-empty
-- list), and BACKUP/RESTORE `partitions` entries are always `ASTPartition`.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t PARTITION tuple(1, 2)'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t PARTITION (1, 2)'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t PARTITION {p:String}'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t PARTITION ID {p:String}'));
SELECT formatQueryFromJSON(parseQueryToJSON('BACKUP TABLE t PARTITIONS 1, 2 TO Disk(''backups'', ''b'')'));

-- `id` accepts only a string literal or a query parameter (`getPartitionIDFromQuery` reads it via
-- `as<ASTLiteral>()->value.safeGet<String>()`).
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"partition":{"type":"Partition","id":{"type":"Identifier","name":"p"},"all":false}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"partition":{"type":"Partition","id":{"type":"Literal","value":{"field_type":"UInt64","value":1}},"all":false}}'); -- { serverError BAD_ARGUMENTS }

-- The `value`/`id`/`all` branches are mutually exclusive; `formatImpl` would silently drop `id`.
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"partition":{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":1}},"id":{"type":"Literal","value":{"field_type":"String","value":"p"}},"all":false,"fields_count":1}}'); -- { serverError BAD_ARGUMENTS }

-- `value` accepts only a literal, a `tuple(...)` function, or a query parameter.
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"partition":{"type":"Partition","value":{"type":"Identifier","name":"v"},"all":false,"fields_count":1}}'); -- { serverError BAD_ARGUMENTS }

-- A forged `fields_count` that does not match the `value` shape is parser-impossible.
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"partition":{"type":"Partition","value":{"type":"Literal","value":{"field_type":"UInt64","value":1}},"all":false,"fields_count":2}}'); -- { serverError BAD_ARGUMENTS }

-- BACKUP/RESTORE `partitions` entries are parser-produced only by `ParserPartition`; MergeTree
-- backup/restore downcasts each entry via `as<ASTPartition &>()` in `getPartitionIDsFromQuery`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('BACKUP TABLE t PARTITION ID ''p'' TO Disk(''backups'', ''b'')'), '"type":"Partition","id"', '"type":"Identifier","name":"p","id"')); -- { serverError BAD_ARGUMENTS }

-- `UserNamesWithHost` requires at least one user name; an empty list would hit the `!children.empty()`
-- assertion in `formatImpl` as an internal exception.
SELECT formatQueryFromJSON('{"type":"UserNamesWithHost"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"UserNamesWithHost","children":[]}'); -- { serverError BAD_ARGUMENTS }
