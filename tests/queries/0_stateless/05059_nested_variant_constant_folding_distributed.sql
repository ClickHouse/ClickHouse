-- A folded `Variant` constant nested inside a compound constant must survive serialization to a secondary
-- server. https://github.com/ClickHouse/ClickHouse/issues/74366 was fixed for a scalar `Variant` constant by
-- https://github.com/ClickHouse/ClickHouse/pull/111136, which scoped out the compound case covered here.

-- The exact serialization lives in the analyzer (ConstantNode::toASTImpl), so force the analyzer.
SET enable_analyzer = 1;

-- The member type has to be named per element, not once for the whole constant.
SELECT [42::UInt64]::Array(Variant(UInt64, String)) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT tuple(42::UInt64::Variant(UInt64, String)) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT map('k', 42::UInt64::Variant(UInt64, String)) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT [[(0., 0.)::Point::Geometry]] FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;

-- A `DateTime` member is exact only as its raw Unix timestamp: both epochs below format to the local
-- text `2023-10-29 02:10:00` in the DST overlap, so the text form comes back an hour early.
SELECT arrayMap(x -> toUnixTimestamp(assumeNotNull(variantElement(x, 'DateTime(\'Europe/Berlin\')'))), [toDateTime(1698541800, 'Europe/Berlin')::Variant(DateTime('Europe/Berlin'), String)]) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;

-- Naming a string-like member is not enough: a cast of a string to a `Variant` with more than one member
-- parses the text and picks whichever member it parses as, so `'42'` would arrive as a `UInt64`. Both arms
-- read `variantType` on the secondary server, so they see the member actually stored there.
SELECT variantType(materialize('42'::Variant(String)::Variant(String, UInt64))) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;
SELECT ['42'::Variant(String)::Variant(String, UInt64), 7::UInt64::Variant(String, UInt64)] AS a, arrayMap(x -> variantType(x), a) FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;

-- `array` and `map` resolve their own result type from their arguments before the enclosing cast runs, so
-- every element must already carry the whole `Variant` type: members with no common supertype do not
-- resolve against each other, and a bare `NULL` is not a valid `Map` key.
SELECT [42::UInt64::Variant(UInt64, String), 'x'::Variant(UInt64, String)] FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0, use_variant_as_common_type = 0;
SELECT map(NULL::Variant(UInt64, String), 'x') FROM remote('127.0.0.1', system.one) SETTINGS prefer_localhost_replica = 0;

-- Predicate-AST pushdown into a distributed subquery is a second serialization carrier for the same
-- constant, and it named no member type at all, not even for a scalar `Variant`.
DROP TABLE IF EXISTS t_variant_const_pushdown;
CREATE TABLE t_variant_const_pushdown (v Variant(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_const_pushdown VALUES (42::UInt64), (7::UInt64);

SELECT count()
FROM (SELECT v FROM remote('127.0.0.{1,2}', currentDatabase(), t_variant_const_pushdown))
WHERE v = 42::UInt64::Variant(UInt64)
SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1, prefer_localhost_replica = 0, serialize_query_plan = 0;

-- `count()` stays 2 even if the predicate never reaches the remote query, so assert the pushed filter is in
-- the remote-side plan: one `Filter` above the outer read plus one inside each of the two shards' plans.
SELECT countIf(explain ILIKE '%Filter column: equals(__table1.v, _CAST(%')
FROM
(
    EXPLAIN actions = 1, distributed = 1
    SELECT count()
    FROM (SELECT v FROM remote('127.0.0.{1,2}', currentDatabase(), t_variant_const_pushdown))
    WHERE v = 42::UInt64::Variant(UInt64)
    SETTINGS allow_push_predicate_ast_for_distributed_subqueries = 1, prefer_localhost_replica = 0, serialize_query_plan = 0, explain_query_plan_default = 'legacy'
);

DROP TABLE t_variant_const_pushdown;
