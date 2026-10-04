SET allow_experimental_time_decay_aggregate_functions = 1;
SET use_variant_as_common_type = 0;

WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS a,
    CAST((1., 1., 10.), 'ExponentialTimeDecaying(10)') AS b
SELECT toTypeName(if(toUInt8(1), a, b));

WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS a,
    CAST((1., 0., 20.), 'ExponentialTimeDecaying(20)') AS b
SELECT if(toUInt8(number % 2), a, b) FROM numbers(2); -- { serverError NO_COMMON_TYPE }

WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS a,
    CAST((1., 0., 20.), 'ExponentialTimeDecaying(20)') AS b
SELECT multiIf(toUInt8(number % 2), a, b) FROM numbers(2); -- { serverError NO_COMMON_TYPE }

WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS a,
    CAST((1., 0., 20.), 'ExponentialTimeDecaying(20)') AS b
SELECT coalesce(toNullable(a), toNullable(b)); -- { serverError NO_COMMON_TYPE }

WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS a,
    CAST((1., 0., 20.), 'ExponentialTimeDecaying(20)') AS b
SELECT array(a, b); -- { serverError NO_COMMON_TYPE }

WITH
    CAST((1., 0., 10.), 'ExponentialTimeDecaying(10)') AS a,
    CAST((1., 0., 10.), 'Tuple(sign Float64, signed_unit_time Float64, decay_length Float64)') AS b
SELECT if(toUInt8(number % 2), a, b) FROM numbers(2); -- { serverError NO_COMMON_TYPE }
