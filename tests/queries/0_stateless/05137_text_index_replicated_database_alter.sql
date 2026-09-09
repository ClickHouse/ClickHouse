-- Tags: zookeeper

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/clickhouse/05137_text_index_replicated_database_alter/{database}', 'shard1', 'replica1') FORMAT Null;

-- The initiating replica validates a new definition as `CREATE`; a follower only replays a
-- definition after it has been committed by the initiator.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.invalid
(
    t Array(Array(String)),
    INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tab
(
    t Array(Array(String))
)
ENGINE = MergeTree ORDER BY tuple();

-- Initial `ALTER` DDL also remains strict; a replica must not introduce invalid metadata.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tab ADD INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }

-- A full-definition `ATTACH` is fresh DDL on the initiating replica, even in a `Replicated` database.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.attached
(
    t Array(Array(String)),
    INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
