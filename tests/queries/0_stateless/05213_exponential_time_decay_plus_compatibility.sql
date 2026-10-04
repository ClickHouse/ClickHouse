SET allow_experimental_time_decay_aggregate_functions = 1;

-- A decaying value must not fall through to generic tuplePlus when paired with
-- a layout-compatible plain tuple.
SELECT
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)')
    + CAST((2., 0., 10.), 'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same compatibility rule applies recursively inside tuples.
SELECT
    tuple(CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)'))
    + tuple(CAST((2., 0., 10.), 'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Array addition re-enters plus for the element types and must reject the same mismatch.
SELECT
    [CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)')]
    + [CAST((2., 0., 10.), 'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)')]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Compatible decaying values still use exponentialTimeDecayingAdd semantics.
WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS a,
    CAST((1., 10 * log(2.), 10.), 'ExponentialTimeDecaying(10)') AS b
SELECT round(exponentialTimeDecayingValueAt(a + b, toFloat64(0)), 6);
