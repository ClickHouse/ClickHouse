SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS 02266_auto_add_nullable;

CREATE TABLE 02266_auto_add_nullable
(
    val0 Int8 DEFAULT NULL,
    val1 Nullable(Int8) DEFAULT NULL,
    val2 UInt8 DEFAULT NUll,
    val3 String DEFAULT null,
    val4 LowCardinality(Int8) DEFAULT NULL,
    val5 LowCardinality(Nullable(Int8)) DEFAULT NULL
)
ENGINE = MergeTree order by tuple();

DESCRIBE TABLE 02266_auto_add_nullable;

DROP TABLE IF EXISTS 02266_auto_add_nullable;