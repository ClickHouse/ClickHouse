-- Tags: no-fasttest
-- `SimdJSON` rejects nesting beyond its depth limit; iterative `RapidJSON` accepts it.
-- Skip the value to test parser selection without constructing nested column types.
SELECT isNull(accurateCastOrNull(concat('{"x":', repeat('[', 1024), '0', repeat(']', 1024), '}'), 'JSON(SKIP x)')) SETTINGS allow_simdjson = 1;
SELECT isNull(accurateCastOrNull(concat('{"x":', repeat('[', 1024), '0', repeat(']', 1024), '}'), 'JSON(SKIP x)')) SETTINGS allow_simdjson = 0;
