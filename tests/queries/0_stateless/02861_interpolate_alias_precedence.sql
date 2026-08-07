DROP TABLE IF EXISTS 02861_interpolate;

CREATE TABLE 02861_interpolate (date Date, id String, f Int16) ENGINE=MergeTree() ORDER BY (date);
INSERT INTO 02861_interpolate VALUES ('2023-05-15', '1', 1), ('2023-05-22', '1', 15);

SELECT date AS d, toNullable(f) AS f FROM 02861_interpolate WHERE id = '1' ORDER BY d ASC WITH FILL STEP toIntervalDay(1) INTERPOLATE (f);

DROP TABLE 02861_interpolate;
