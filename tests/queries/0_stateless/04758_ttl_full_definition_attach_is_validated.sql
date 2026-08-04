-- A full-definition `ATTACH TABLE t UUID '...' (...) ENGINE = MergeTree ...` is CREATE-like user input,
-- not a load of previously validated metadata (only the short `ATTACH TABLE t` re-attach and server
-- startup are). It must therefore be validated like `CREATE TABLE`: a strict session must not be able to
-- attach a TTL whose expression can only be built leniently, because the first strict rebuild of the TTL
-- (an `INSERT`, a background TTL merge) would throw.

SET variant_throw_on_type_mismatch = 1;
SET allow_suspicious_ttl_expressions = 0;

-- `length` has no compatible alternative inside this `Variant`, so a strict build throws. The `ATTACH`
-- fails, so nothing is registered under the fixed UUID.
ATTACH TABLE test_ttl_full_definition_attach UUID '2f97a5a8-c33c-44c8-a0da-1f0b8b1a04b8'
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'rejected';
