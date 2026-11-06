SET enable_analyzer = 1;

-- arrayFirst should preserve Variant discriminator (not reinterpret Date as Bool)
SELECT 'arrayFirst';
SELECT
    arrayFirst(x -> true, ['2026-03-05'::Date::Variant(Date, Bool)]) AS value,
    variantType(value);

-- arrayLast should preserve Variant discriminator (not reinterpret Date as Bool)
SELECT 'arrayLast';
SELECT
    arrayLast(x -> true, ['2026-03-05'::Date::Variant(Date, Bool)]) AS value,
    variantType(value);
-- arrayFirst should preserve Variant discriminator (not reinterpret Date as Bool)
SELECT 'arrayFirst_with_filter';
SELECT
    arrayFirst(x -> x.Date IS NOT NULL, ['2026-03-05'::Date::Variant(Date, Bool)]) AS value,
    variantType(value);

-- arrayLast should preserve Variant discriminator (not reinterpret Date as Bool)
SELECT 'arrayLast_with_filter';
SELECT
    arrayLast(x -> x.Date IS NOT NULL, ['2026-03-05'::Date::Variant(Date, Bool)]) AS value,
    variantType(value);
