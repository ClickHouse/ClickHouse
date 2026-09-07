-- Tags: zookeeper

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/clickhouse/05111_text_index_replicated_database/{database}', 'shard1', 'replica1') FORMAT Null;

-- The initiating replica validates a new definition as `CREATE`; followers replaying a committed
-- definition use `SECONDARY_CREATE` and must not revalidate it during a rolling upgrade.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tab
(
    t Array(Array(String)),
    INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
