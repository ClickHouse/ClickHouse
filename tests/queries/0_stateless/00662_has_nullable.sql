DROP TABLE IF EXISTS 00662_has_nullable;
CREATE TABLE 00662_has_nullable(a Nullable(UInt64)) ENGINE = Memory;

INSERT INTO 00662_has_nullable VALUES (1), (Null);
SELECT a, has([0, 1], a) FROM 00662_has_nullable;

DROP TABLE 00662_has_nullable;
