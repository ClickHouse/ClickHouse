DROP TABLE IF EXISTS empty_prewhere_preserves_validation;

CREATE TABLE empty_prewhere_preserves_validation
(
    id UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY id
SAMPLE BY id;

INSERT INTO empty_prewhere_preserves_validation VALUES (1, 1);

SELECT count()
FROM empty_prewhere_preserves_validation SAMPLE 1 OFFSET 0.5
PREWHERE value IN (SELECT toUInt64(1) WHERE false); -- { serverError ARGUMENT_OUT_OF_BOUND }

DROP TABLE empty_prewhere_preserves_validation;
