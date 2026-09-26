-- A TTL expression is stored as an AST plus the list of columns it needs, and it is rebuilt from those two
-- every time it runs (INSERT, merge) and every time the table is loaded. The column list used to be taken
-- from the *built* expression, so constant folding could drop a column the AST still refers to and leave
-- behind a TTL that can never be rebuilt again.

SELECT '-- a TTL WHERE that folds to a constant --';

-- `isNull` over a non-`Nullable` column folds to `0`, which prunes `x` from the built expression while the
-- stored AST keeps referring to it. Rebuilding it without `x` used to fail with
-- `Missing columns: 'x' ... no source columns`.
CREATE TABLE test_ttl_folded_where
(
    key UInt64,
    x UInt64,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(x);

-- The rebuild on INSERT.
INSERT INTO test_ttl_folded_where VALUES (1, 1, now());

-- The rebuild on a merge. The second row is past its TTL, but the `WHERE` is constantly false, so both rows
-- must survive.
INSERT INTO test_ttl_folded_where VALUES (2, 2, now() - INTERVAL 10 DAY);
OPTIMIZE TABLE test_ttl_folded_where FINAL;
SELECT count() FROM test_ttl_folded_where;

-- The rebuild on loading.
DETACH TABLE test_ttl_folded_where;
ATTACH TABLE test_ttl_folded_where;
SELECT count() FROM test_ttl_folded_where;

DROP TABLE test_ttl_folded_where;

SELECT '-- loading does not depend on the mismatch settings --';

-- Whether a function over a `Variant` column throws on an incompatible stored type or resolves to NULL is
-- taken from `variant_throw_on_type_mismatch`, and the `Variant` adaptor consults it while *building* the
-- expression too: with no compatible alternative at all, a strict build throws and a lenient one resolves
-- the result to NULL. Table loading has no query context, so it always saw the strict default and refused
-- to attach a table created by a lenient session - i.e. the server did not start anymore.
SET variant_throw_on_type_mismatch = 0;
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_variant_attach
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v));

-- A restart, a background task or just another session reads the same metadata under the default settings.
SET variant_throw_on_type_mismatch = 1;
SET allow_suspicious_ttl_expressions = 0;

DETACH TABLE test_ttl_variant_attach;
ATTACH TABLE test_ttl_variant_attach;
SELECT 'attached';

DROP TABLE test_ttl_variant_attach;

-- The `Dynamic` adaptor consults `dynamic_throw_on_type_mismatch` only while *executing*: unlike the
-- `Variant` adaptor it enumerates no alternatives while building (its result type falls back to `Dynamic`),
-- so the build itself cannot throw and the startup failure above was specific to `Variant`. What loading
-- must still guarantee for `Dynamic` is the same contract: a stored TTL that only an escape-hatch session
-- could create (the strict DDL-time probe rejects `length` over a `Dynamic`, whatever the session policy)
-- is loaded back without re-validation, under any session settings.
SET dynamic_throw_on_type_mismatch = 0;

CREATE TABLE test_ttl_dynamic_attach
(
    key UInt64,
    v Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v)); -- { serverError BAD_TTL_EXPRESSION }

SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_dynamic_attach
(
    key UInt64,
    v Dynamic,
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v));

SET dynamic_throw_on_type_mismatch = 1;
SET allow_suspicious_ttl_expressions = 0;

DETACH TABLE test_ttl_dynamic_attach;
ATTACH TABLE test_ttl_dynamic_attach;
SELECT 'attached dynamic';

DROP TABLE test_ttl_dynamic_attach;

SELECT '-- the escape hatch keeps the session strictness --';

-- `allow_suspicious_ttl_expressions` only skips the TTL validator; the build itself still follows the
-- session's mismatch policy. A strict session is rejected right at CREATE time instead of getting a table
-- whose TTL would throw on the first rebuild (a default-settings INSERT, or a TTL merge under the
-- background profile).
SET variant_throw_on_type_mismatch = 1;
SET allow_suspicious_ttl_expressions = 1;

CREATE TABLE test_ttl_variant_strict_suspicious
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
