DROP TABLE IF EXISTS prewhere_bad_type;
CREATE TABLE prewhere_bad_type (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO prewhere_bad_type (id) VALUES (1);
SELECT id, s FROM prewhere_bad_type PREWHERE s; -- { serverError 59 }
