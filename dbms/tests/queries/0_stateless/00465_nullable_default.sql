DROP TABLE IF EXISTS nullable;
CREATE TABLE nullable (id Nullable(UInt32), cat String) ENGINE = Log;
INSERT INTO nullable (cat) VALUES ('test');
SELECT * FROM nullable;
DROP TABLE nullable;
