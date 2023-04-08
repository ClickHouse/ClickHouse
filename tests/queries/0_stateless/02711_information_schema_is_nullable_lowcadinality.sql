CREATE TABLE users (low_card_nullable_name LowCardinality(Nullable(String)), name String, nullable_name Nullable(String)) ENGINE = Memory;

SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE (table_schema = 'default') AND (table_name = 'users')
ORDER BY column_name;