-- The JSON AST reader enforces the invariants of the range fields that the parser guarantees: `ALL`
-- needs an `AFTER` expression, and an offset without a length cannot accompany a range.
SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('SELECT number FROM numbers(5) LIMIT UNTIL number = 3'), '"type":"SelectQuery",', '"type":"SelectQuery","limit_after_all":true,')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('SELECT 1 LIMIT AFTER 1'), '"limit_after":', '"limit_offset":{"type":"Literal","value":{"field_type":"UInt64","value":2}},"limit_after":')); -- { serverError BAD_ARGUMENTS }

-- The writer's own output round-trips.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT number FROM numbers(5) LIMIT 2 AFTER number = 1 ALL UNTIL number = 4'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT number FROM numbers(5) LIMIT UNTIL number = 3'));
