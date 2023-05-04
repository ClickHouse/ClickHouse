DROP TABLE IF EXISTS segfault;
DROP TABLE IF EXISTS segfault_mv;

CREATE TABLE segfault
(
    id          UInt32,
    uuid        UUID,
    tags_ids    Array(UInt32)
) ENGINE = MergeTree()
ORDER BY (id);

CREATE MATERIALIZED VIEW segfault_mv
    ENGINE = AggregatingMergeTree()
    ORDER BY (id)
AS SELECT
    id,
    uniqState(uuid) as uniq_uuids,
    uniqMapState(CAST((tags_ids, arrayMap(_ -> toString(uuid), tags_ids)), 'Map(UInt32, String)')) as uniq_tags_ids
FROM segfault
GROUP BY id;

INSERT INTO segfault SELECT * FROM generateRandom('id UInt32, uuid UUID, c Array(UInt32)', 10, 5, 5) LIMIT 100;
INSERT INTO segfault SELECT * FROM generateRandom('id UInt32, uuid UUID, c Array(UInt32)', 10, 5, 5) LIMIT 100;
INSERT INTO segfault SELECT * FROM generateRandom('id UInt32, uuid UUID, c Array(UInt32)', 10, 5, 5) LIMIT 100;

SELECT ignore(CAST((arrayMap(k -> toString(k), mapKeys(uniqMapMerge(uniq_tags_ids) AS m)), mapValues(m)), 'Map(String, UInt32)')) FROM segfault_mv;
