SET allow_experimental_time_decay_aggregate_functions = 1;
SET use_variant_default_implementation_for_comparisons = 0;
SET allow_suspicious_types_in_order_by = 1;

-- Native comparisons must recurse through the carrier for constants and columns.
WITH CAST(CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)'),
          'Variant(ExponentialTimeDecayingFloat64(10), UInt8)') AS value
SELECT value = value, value <= value, value >= value, value != value;

DROP TABLE IF EXISTS time_decay_variant;
CREATE TABLE time_decay_variant
(
    id UInt8,
    a Variant(ExponentialTimeDecayingFloat64(10), UInt8),
    b Variant(ExponentialTimeDecayingFloat64(10), UInt8)
)
ENGINE = Memory;
INSERT INTO time_decay_variant VALUES
    (1, (-1., -10., 10.), (-1., -20., 10.)),
    (2, (0., 0., 10.), (1., 0., 10.)),
    (3, (1., 0., 10.), (1., 0., 10.)),
    (4, 1, 2);
SELECT id, a = b, a != b, a < b, a <= b, a > b, a >= b FROM time_decay_variant ORDER BY id;
SELECT id FROM time_decay_variant ORDER BY a;
SELECT id FROM time_decay_variant WHERE a IN (SELECT b FROM time_decay_variant) ORDER BY id;
DROP TABLE time_decay_variant;

-- Extending a carrier can give its member columns a different local order from
-- the global type discriminators. Empty decaying alternatives and NULL are valid.
SELECT CAST(CAST(toUInt8(number), 'Variant(UInt8)'),
            'Variant(ExponentialTimeDecayingFloat64(10), UInt8)') FROM numbers(3);
SELECT CAST(NULL, 'Variant(ExponentialTimeDecayingFloat64(10), UInt8)');

-- The same compatibility rules apply to nested members and reject mixed lengths.
WITH
    CAST(CAST([(1., 0., 10.)], 'Array(ExponentialTimeDecayingFloat64(10))'),
         'Variant(Array(ExponentialTimeDecayingFloat64(10)), UInt8)') AS value
SELECT value = value;
WITH
    CAST(CAST((1., 0., 10.), 'ExponentialTimeDecayingFloat64(10)'),
         'Variant(ExponentialTimeDecayingFloat64(10), UInt8)') AS a,
    CAST(CAST((1., 0., 20.), 'ExponentialTimeDecayingFloat64(20)'),
         'Variant(ExponentialTimeDecayingFloat64(20), UInt8)') AS b
SELECT a = b; -- { serverError BAD_ARGUMENTS }

-- Typed input must validate decaying members inside `Variant` before they can
-- reach native sorting or set membership.
SELECT *
FROM VALUES('value Variant(ExponentialTimeDecayingFloat64(10), UInt8)', ((1., 0., 20.)))
ORDER BY value; -- { serverError BAD_ARGUMENTS }
SELECT value IN (SELECT * FROM VALUES('value Variant(ExponentialTimeDecayingFloat64(10), UInt8)', ((1., 0., 10.))))
FROM VALUES('value Variant(ExponentialTimeDecayingFloat64(10), UInt8)', ((1., 0., 20.))); -- { serverError BAD_ARGUMENTS }
SELECT *
FROM VALUES('value Variant(Array(ExponentialTimeDecayingFloat64(10)), UInt8)', ([(1., 0., 20.)]))
ORDER BY value; -- { serverError BAD_ARGUMENTS }
