
CREATE TABLE src (x UInt8) ENGINE = Null;
CREATE TABLE dst (x UInt8) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv_00508 TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2);
SELECT * FROM mv_00508 ORDER BY x;

-- Detach MV and see if the data is still readable
DETACH TABLE mv_00508;
SELECT * FROM dst ORDER BY x;

USE default;

-- Reattach MV (shortcut)
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.mv_00508;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.src VALUES (3);

SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.mv_00508 ORDER BY x;

-- Drop the MV and see if the data is still readable
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.mv_00508;
SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.dst ORDER BY x;

DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.src;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.dst;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
