CREATE TABLE mt (v UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01497/mt')
    ORDER BY tuple() -- { serverError 36 }

