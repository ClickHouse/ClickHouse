-- Tags: no-parallel, no-fasttest

SET prefer_localhost_replica=1;

DROP TABLE IF EXISTS file;
DROP TABLE IF EXISTS url;
DROP TABLE IF EXISTS view;
DROP TABLE IF EXISTS buffer;
DROP TABLE IF EXISTS merge;
DROP TABLE IF EXISTS merge_tf;
DROP TABLE IF EXISTS distributed;
DROP TABLE IF EXISTS distributed_tf;
DROP TABLE IF EXISTS rich_syntax;
DROP DICTIONARY IF EXISTS dict;

CREATE TABLE file (n Int8) ENGINE = File(upper('tsv') || 'WithNames' || 'AndTypes');
CREATE TABLE buffer (n Int8) ENGINE = Buffer(currentDatabase(), file, 16, 10, 200, 10000, 1000000, 10000000, 1000000000);
CREATE TABLE merge (n Int8) ENGINE = Merge('', lower('DISTRIBUTED'));
CREATE TABLE merge_tf as merge(currentDatabase(), '.*');
CREATE TABLE distributed (n Int8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'fi' || 'le');
CREATE TABLE distributed_tf as cluster('test' || '_' || 'shard_localhost', '', 'buf' || 'fer');

INSERT INTO buffer VALUES (1);
DETACH TABLE buffer;        -- trigger flushing
ATTACH TABLE buffer;

CREATE TABLE url (n UInt64, col String) ENGINE=URL
(
    replace
    (
        'https://localhost:' || getServerPort('https_port') || '/?query='  || 'select n, _table from ' || currentDatabase() || '.merge format CSV', ' ', '+'
    ),
    CSV
);

CREATE VIEW view AS SELECT toInt64(n) as n FROM (SELECT toString(n) as n from merge WHERE _table != 'qwerty' ORDER BY _table) UNION ALL SELECT * FROM file;

-- The following line is needed just to disable checking stderr for emptiness
SELECT nonexistentsomething; -- { serverError UNKNOWN_IDENTIFIER }

CREATE DICTIONARY dict (n UInt64, col String DEFAULT '42') PRIMARY KEY n
SOURCE(CLICKHOUSE(HOST 'localhost' PORT getServerPort('tcp_port_secure') SECURE 1 USER 'default' TABLE 'url')) LIFETIME(1) LAYOUT(CACHE(SIZE_IN_CELLS 1));

-- dict --> url --> merge |-> distributed -> file (1)
--                        |-> distributed_tf -> buffer -> file (1)

-- TODO make fuzz test from this
CREATE TABLE rich_syntax as remote
(
    'localhos{x|y|t}',
    cluster
    (
        'test' || '_' || 'shard_localhost',
        remote
        (
            '127.0.0.{1..4}',
            if
            (
                toString(40 + 2) NOT IN ('hello', dictGetString(currentDatabase() || '.dict', 'col', toUInt64('0001'))),
                currentDatabase(),
                'FAIL'
            ),
            extract('123view456', '[a-z]+')
        )
    )
);


SHOW CREATE file;
SHOW CREATE buffer;
SHOW CREATE merge;
SHOW CREATE merge_tf;
SHOW CREATE distributed;
SHOW CREATE distributed_tf;
SHOW CREATE url;
SHOW CREATE rich_syntax;
SHOW CREATE VIEW view;
SHOW CREATE dict;

INSERT INTO buffer VALUES (1);
-- remote(localhost) --> cluster(test_shard_localhost) |-> remote(127.0.0.1) --> view |-> subquery --> merge |-> distributed --> file (1)
--                                                     |                              |                      |-> distributed_tf -> buffer (1) -> file (1)
--                                                     |                              |-> file (1)
--                                                     |-> remote(127.0.0.2) --> ...
SELECT sum(n) from rich_syntax settings enable_parallel_replicas=0;
SELECT sum(n) from rich_syntax settings serialize_query_plan=0;

-- Clear cache to avoid future errors in the logs
SYSTEM DROP DNS CACHE;

DROP TABLE file;
DROP DICTIONARY dict;
DROP TABLE url;
DROP TABLE view;
DROP TABLE buffer;
DROP TABLE merge;
DROP TABLE merge_tf;
DROP TABLE distributed;
DROP TABLE distributed_tf;
DROP TABLE rich_syntax;
