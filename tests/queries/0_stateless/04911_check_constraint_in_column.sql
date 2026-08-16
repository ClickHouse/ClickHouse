CREATE TABLE check_in_column
(
    id UInt64,
    allowed_ids Array(UInt64),
    CONSTRAINT id_in_allowed_ids CHECK id IN allowed_ids
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO check_in_column VALUES (1, [1, 2]);
INSERT INTO check_in_column VALUES (3, [1, 2]); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM check_in_column;

DROP TABLE check_in_column;
