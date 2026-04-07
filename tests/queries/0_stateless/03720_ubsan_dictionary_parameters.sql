-- https://github.com/ClickHouse/ClickHouse/issues/78506
-- Checks that there the dictionary parameters is read correctly

CREATE DICTIONARY `d55` (`c0` SimpleAggregateFunction(anyLast, Date)
DEFAULT '75942d37-37c4-8ea0-4175-1a4e0cb18c3b' INJECTIVE)
PRIMARY KEY (`c0`)
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 't13'))
LAYOUT(HASHED(SHARD_LOAD_QUEUE_BACKLOG 2147483648))
LIFETIME(2);

SELECT * FROM d55; -- { serverError UNKNOWN_TABLE }
