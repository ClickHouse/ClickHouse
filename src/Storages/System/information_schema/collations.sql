ATTACH VIEW collations
(
    collation_name String,
    COLLATION_NAME String,
    -- MySQL-compatibility columns, appended after the standard columns to preserve their ordinal positions
    character_set_name Nullable(String),
    id Nullable(UInt64),
    is_default Nullable(String),
    is_compiled Nullable(String),
    sortlen Nullable(UInt64),
    pad_attribute Nullable(String),
    CHARACTER_SET_NAME Nullable(String),
    ID Nullable(UInt64),
    IS_DEFAULT Nullable(String),
    IS_COMPILED Nullable(String),
    SORTLEN Nullable(UInt64),
    PAD_ATTRIBUTE Nullable(String)
)
SQL SECURITY INVOKER
AS SELECT
    name AS collation_name,
    collation_name AS COLLATION_NAME,
    -- The ICU collations have no MySQL analog, so the MySQL-specific attributes are NULL for them
    NULL AS character_set_name,
    NULL AS id,
    NULL AS is_default,
    NULL AS is_compiled,
    NULL AS sortlen,
    NULL AS pad_attribute,
    character_set_name AS CHARACTER_SET_NAME,
    id AS ID,
    is_default AS IS_DEFAULT,
    is_compiled AS IS_COMPILED,
    sortlen AS SORTLEN,
    pad_attribute AS PAD_ATTRIBUTE
FROM system.collations
UNION ALL
-- The MySQL-compatibility collation advertised in the MySQL handshake (`CharacterSet::utf8mb4_0900_ai_ci` in `MySQLHandler`),
-- in `SCHEMATA.DEFAULT_COLLATION_NAME` and `TABLES.TABLE_COLLATION`, so that a client following the advertised
-- collation into this view finds it
SELECT
    'utf8mb4_0900_ai_ci' AS collation_name,
    collation_name AS COLLATION_NAME,
    'utf8mb4' AS character_set_name,
    255 AS id,
    'Yes' AS is_default,
    'Yes' AS is_compiled,
    0 AS sortlen,
    'NO PAD' AS pad_attribute,
    character_set_name AS CHARACTER_SET_NAME,
    id AS ID,
    is_default AS IS_DEFAULT,
    is_compiled AS IS_COMPILED,
    sortlen AS SORTLEN,
    pad_attribute AS PAD_ATTRIBUTE
UNION ALL
-- The `binary` pseudo-collation stamped on numeric and other non-string columns in the MySQL wire
-- protocol result-set metadata (`getColumnDefinition`) and enumerated by `SHOW COLLATION`
SELECT
    'binary' AS collation_name,
    collation_name AS COLLATION_NAME,
    'binary' AS character_set_name,
    63 AS id,
    'Yes' AS is_default,
    'Yes' AS is_compiled,
    1 AS sortlen,
    'NO PAD' AS pad_attribute,
    character_set_name AS CHARACTER_SET_NAME,
    id AS ID,
    is_default AS IS_DEFAULT,
    is_compiled AS IS_COMPILED,
    sortlen AS SORTLEN,
    pad_attribute AS PAD_ATTRIBUTE
