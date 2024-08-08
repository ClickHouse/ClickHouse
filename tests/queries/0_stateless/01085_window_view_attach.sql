
SET send_logs_level = 'fatal';
SET enable_analyzer = 0;
SET allow_experimental_window_view = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE=Ordinary;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.mt;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.wv;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.mt(a Int32, market Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW {CLICKHOUSE_DATABASE:Identifier}.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM {CLICKHOUSE_DATABASE:Identifier}.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid, market;

SHOW tables FROM {CLICKHOUSE_DATABASE:Identifier};

DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.wv SYNC;
SHOW tables FROM {CLICKHOUSE_DATABASE:Identifier};

CREATE WINDOW VIEW {CLICKHOUSE_DATABASE:Identifier}.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM {CLICKHOUSE_DATABASE:Identifier}.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid, market;

DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.wv;
SHOW tables FROM {CLICKHOUSE_DATABASE:Identifier};

ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.wv;
SHOW tables FROM {CLICKHOUSE_DATABASE:Identifier};

DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.wv SYNC;
SHOW tables FROM {CLICKHOUSE_DATABASE:Identifier};
