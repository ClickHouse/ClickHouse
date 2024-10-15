SET allow_experimental_json_type = 1;
SET allow_experimental_variant_type = 1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Memory();
INSERT INTO t0 (c0) VALUES (1);
ALTER TABLE t0 (ADD COLUMN c1 JSON(c1 Variant(Int,JSON(c1 Int))));
INSERT INTO t0 (c0, c1) VALUES (2, '{"c1":1}'::JSON);
SELECT kafkaMurmurHash(c1) FROM t0; -- {serverError NOT_IMPLEMENTED}
DROP TABLE t0;
