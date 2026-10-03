-- Columns wrapped in LowCardinality(Nullable(...)) accept NULL, so they must be reported
-- as nullable. See https://github.com/ClickHouse/ClickHouse/issues/44930

DROP TABLE IF EXISTS users_low_cardinality_nullable;

CREATE TABLE users_low_cardinality_nullable
(
    low_card_nullable_name LowCardinality(Nullable(String)),
    low_card_name LowCardinality(String),
    name String,
    nullable_name Nullable(String),
    array_of_nullable Array(Nullable(String))
)
ENGINE = Memory;

-- the column really does accept NULL
INSERT INTO users_low_cardinality_nullable (low_card_nullable_name) VALUES (NULL);
SELECT count() FROM users_low_cardinality_nullable WHERE low_card_nullable_name IS NULL;

SELECT name, type, is_nullable
FROM system.columns
WHERE (database = currentDatabase()) AND (table = 'users_low_cardinality_nullable')
ORDER BY name;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE (table_schema = currentDatabase()) AND (table_name = 'users_low_cardinality_nullable')
ORDER BY column_name;

DROP TABLE users_low_cardinality_nullable;
