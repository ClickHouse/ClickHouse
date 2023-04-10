CREATE TABLE users_low_cardinality_nullable (low_card_nullable_name LowCardinality(Nullable(String)), name String, nullable_name Nullable(String)) ENGINE = Memory;

SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE (table_schema = currentDatabase()) AND (table_name = 'users_low_cardinality_nullable')
ORDER BY column_name;
