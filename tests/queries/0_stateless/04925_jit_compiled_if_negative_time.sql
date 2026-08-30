-- Compiled `if`/`multiIf` must sign-extend a negative `Time`, not zero-extend it.

SET session_timezone = 'UTC';
SET compile_expressions = 1, min_count_to_compile_expression = 0;

-- Widening to `Time64`/`DateTime64` (unequal scales).
SELECT if(number % 2 = 0, toTime('-01:00:00'), toTime64('00:00:00', 3)) FROM numbers(1);
SELECT if(number % 2 = 0, toTime('-01:00:00'), toTime64('00:00:00', 9)) FROM numbers(1);
SELECT if(number % 2 = 0, toTime('-01:00:00'), toDateTime64('1970-01-01 00:00:00', 3)) FROM numbers(1);
SELECT multiIf(number % 3 = 0, toTime('-01:00:00'), number % 3 = 1, toTime64('00:00:01', 3), toTime64('00:00:02', 3)) FROM numbers(3);

-- Widening to `DateTime` (equal scales, a separate cast path).
SELECT if(number % 2 = 0, toTime('-01:00:00'), toDateTime('1970-01-01 00:00:00')) FROM numbers(2);

-- `Nullable` is stripped by recursion before the cast, so it inherits the same behaviour.
SELECT if(number % 2 = 0, toNullable(toTime('-01:00:00')), toTime64('00:00:00', 3)) FROM numbers(1);

-- Values that must be unaffected: non-negative `Time`, and the largest negative magnitude.
SELECT if(number % 2 = 0, toTime('23:59:59'), toTime64('00:00:00', 3)) FROM numbers(1);
SELECT if(number % 2 = 0, toTime('999:00:00'), toTime64('00:00:00', 3)) FROM numbers(1);
SELECT if(number % 2 = 0, toTime('-999:00:00'), toTime64('00:00:00', 3)) FROM numbers(1);
SELECT if(number % 2 = 0, toDateTime64('1969-12-31 23:00:00', 3), toDateTime64('1970-01-01 00:00:00', 6)) FROM numbers(1);
