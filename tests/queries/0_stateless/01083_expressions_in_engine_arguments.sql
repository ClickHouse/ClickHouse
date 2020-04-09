DROP DATABASE IF EXISTS test_01083;
CREATE DATABASE test_01083;
USE test_01083;

CREATE TABLE file (n Int8) ENGINE = File(upper('tsv') || 'WithNames' || 'AndTypes');
CREATE TABLE buffer (n Int8) ENGINE = Buffer(currentDatabase(), file, 16, 10, 200, 10000, 1000000, 10000000, 1000000000);
CREATE TABLE merge (n Int8) ENGINE = Merge('', lower('DISTRIBUTED'));
CREATE TABLE merge_tf as merge(currentDatabase(), '.*');
CREATE TABLE distributed (n Int8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'fi' || 'le');
CREATE TABLE distributed_tf as cluster('test' || '_' || 'shard_localhost', '', 'fi' || 'le');

INSERT INTO file VALUES (1);
CREATE TABLE url (n UInt64, _path String) ENGINE=URL
(
    replace
    (
        'https://localhost:8443/?query='  || 'select n, _path from ' || currentDatabase() || '.file format CSV', ' ', '+'    -- replace `file` with `merge` here after #9246 is fixed
    ),
    CSV
);

-- The following line is needed just to disable checking stderr for emptiness
SELECT nonexistentsomething; -- { serverError 47 }

CREATE DICTIONARY dict (n UInt64, _path String DEFAULT '42') PRIMARY KEY n
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9440 SECURE 1 USER 'default' TABLE 'url' DB 'test_01083')) LIFETIME(1) LAYOUT(CACHE(SIZE_IN_CELLS 1));

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
                toString(40 + 2.0) NOT IN ('hello', dictGetString(currentDatabase() || '.dict', '_path', toUInt64('0001'))),
                currentDatabase(),
                'FAIL'
            ),
            extract('123url456', '[a-z]+')
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

SELECT n from rich_syntax;

DROP DATABASE test_01083;
