DROP TABLE IF EXISTS data_null;

CREATE TABLE data_null (
    a INT NULL,
    b INT NOT NULL,
    C Nullable(INT)
);

INSERT INTO data_null VALUES (1, 2, 3);

SELECT toTypeName(*) FROM data_null;