SET allow_experimental_time_decay_aggregate_functions = 1;

DROP TABLE IF EXISTS time_decay_variant_in;
CREATE TABLE time_decay_variant_in
(
    id UInt8,
    probe Variant(ExponentialTimeDecaying64(10), UInt8),
    key Variant(ExponentialTimeDecaying64(10), UInt8)
)
ENGINE = Memory;
INSERT INTO time_decay_variant_in VALUES
    (1, (-1., -10., 10.), (-1., -20., 10.)),
    (2, (0., 0., 10.), (1., 0., 10.)),
    (3, (1., 0., 10.), (1., 0., 10.)),
    (4, 1, 1),
    (5, 2, 3);

-- `IN` dispatches the probe alternatives separately; `nullIn` uses the full carrier.
SELECT id,
       probe IN (SELECT key FROM time_decay_variant_in),
       probe NOT IN (SELECT key FROM time_decay_variant_in),
       nullIn(probe, (SELECT key FROM time_decay_variant_in)),
       probe GLOBAL IN (SELECT key FROM time_decay_variant_in)
FROM time_decay_variant_in ORDER BY id;

-- Constant and materialized scalar probes may be wrapped in their exact set alternative.
SELECT CAST((1., 0., 10.), 'ExponentialTimeDecaying64(10)') IN (SELECT key FROM time_decay_variant_in);
SELECT materialize(CAST((1., 0., 10.), 'ExponentialTimeDecaying64(10)')) IN (SELECT key FROM time_decay_variant_in);
SELECT toUInt8(1) IN (SELECT key FROM time_decay_variant_in);

-- Equal tuple layouts are insufficient: a raw tuple or another decay length must be rejected.
-- The analyzer's single-key cast probe can reject these before the runtime type-compatibility check.
SELECT CAST((1., 0., 20.), 'ExponentialTimeDecaying64(20)') IN (SELECT key FROM time_decay_variant_in); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT, CANNOT_CONVERT_TYPE }
SELECT tuple(1., 0., 10.) IN (SELECT key FROM time_decay_variant_in); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH, ILLEGAL_TYPE_OF_ARGUMENT }

-- The same adaptor/cast path applies when an alternative contains an array of decaying values.
WITH CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecaying64(10))') AS a
SELECT CAST(a, 'Variant(Array(ExponentialTimeDecaying64(10)), UInt8)') IN
    (SELECT CAST(a, 'Variant(Array(ExponentialTimeDecaying64(10)), UInt8)'));

-- Validate malformed set and probe values independently.
SELECT probe IN (SELECT value FROM VALUES('value Variant(ExponentialTimeDecaying64(10), UInt8)', ((2., 0., 20.))))
FROM time_decay_variant_in; -- { serverError BAD_ARGUMENTS, TYPE_MISMATCH }
SELECT value IN (SELECT key FROM time_decay_variant_in)
FROM VALUES('value Variant(ExponentialTimeDecaying64(10), UInt8)', ((1., 0., 20.))); -- { serverError BAD_ARGUMENTS, TYPE_MISMATCH }
DROP TABLE time_decay_variant_in;
