SET check_query_single_value_result = 0;
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS mt_without_pk;

CREATE TABLE mt_without_pk (SomeField1 Int64, SomeField2 Double) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO mt_without_pk VALUES (1, 2);

CHECK TABLE mt_without_pk;

DROP TABLE IF EXISTS mt_without_pk;

DROP TABLE IF EXISTS replicated_mt_without_pk;

CREATE TABLE replicated_mt_without_pk (SomeField1 Int64, SomeField2 Double) ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt_without_pk', '1') ORDER BY tuple();

INSERT INTO replicated_mt_without_pk VALUES (1, 2);

CHECK TABLE replicated_mt_without_pk;

DROP TABLE IF EXISTS replicated_mt_without_pk;
