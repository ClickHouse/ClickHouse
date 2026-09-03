SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS constraint_on_nullable_type;
CREATE TABLE constraint_on_nullable_type
(
    `id` Nullable(UInt64),
    CONSTRAINT `c0` CHECK `id` = 1
)
ENGINE = TinyLog();

INSERT INTO constraint_on_nullable_type VALUES (0); -- {serverError VIOLATED_CONSTRAINT}
INSERT INTO constraint_on_nullable_type VALUES (1);

SELECT * FROM constraint_on_nullable_type;

DROP TABLE constraint_on_nullable_type;

DROP TABLE IF EXISTS constraint_on_low_cardinality_type;
CREATE TABLE constraint_on_low_cardinality_type
(
    `id` LowCardinality(UInt64),
    CONSTRAINT `c0` CHECK `id` = 2
)
ENGINE = TinyLog;

INSERT INTO constraint_on_low_cardinality_type VALUES (0); -- {serverError VIOLATED_CONSTRAINT}
INSERT INTO constraint_on_low_cardinality_type VALUES (2);

SELECT * FROM constraint_on_low_cardinality_type;

DROP TABLE constraint_on_low_cardinality_type;

DROP TABLE IF EXISTS constraint_on_low_cardinality_nullable_type;

CREATE TABLE constraint_on_low_cardinality_nullable_type
(
    `id` LowCardinality(Nullable(UInt64)),
    CONSTRAINT `c0` CHECK `id` = 3
)
ENGINE = TinyLog;

INSERT INTO constraint_on_low_cardinality_nullable_type VALUES (0); -- {serverError VIOLATED_CONSTRAINT}
INSERT INTO constraint_on_low_cardinality_nullable_type VALUES (3);

SELECT * FROM constraint_on_low_cardinality_nullable_type;

DROP TABLE constraint_on_low_cardinality_nullable_type;
