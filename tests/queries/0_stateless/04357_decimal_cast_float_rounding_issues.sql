-- Regression tests for float-to-Decimal cast rounding, adopted from:
--   https://github.com/ClickHouse/ClickHouse/issues/45247
--   https://github.com/ClickHouse/ClickHouse/issues/103104

-- #103104: the closest Float64 to 16.4 is 16.39999999999999857..., which a truncating
-- cast turns into 16.399999. Rounding to nearest must give 16.4.
SELECT toDecimal64(16.4, 6);

-- #45247: 64.32 is stored as ~64.3199999999, so a truncating cast yields 64.31, while the
-- `toString`/`round` workarounds yield 64.32. With rounding, the direct casts also give 64.32.
SELECT
    CAST('64.32', 'Float64') AS a,
    toString(a) AS b,
    CAST(a, 'Decimal64(2)') AS c,
    CAST(round(a, 2), 'Decimal64(2)') AS d,
    CAST(toString(a), 'Decimal64(2)') AS e,
    CAST(round(toDecimal64(a, 3), 2), 'Decimal64(2)') AS f
FORMAT Vertical;

-- #103104: numbers in the JSON data type materialize as Float64, so extracting them as
-- Decimal must round the same way as a direct float cast (all columns must read 16.4).
WITH
    '{"amount":16.4}' AS json,
    json::JSON AS event
SELECT
    JSON_VALUE(json, '$.amount') AS json_value,
    JSONExtract(json, 'amount', 'Decimal64(6)') AS json_extract,
    event.amount AS dynamic_type,
    event.amount::Decimal64(6) AS direct_cast_to_dec,
    toDecimal64(event.amount, 6) AS direct_to_dec,
    event.amount::String::Decimal64(6) AS indirect_via_str_to_dec,
    JSONExtract(event, 'amount', 'Decimal64(6)') AS json_extract_from_jdt
FORMAT Vertical;
