DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    ip IPv4 Codec(ZSTD(6)),
) ENGINE MergeTree() order by ip;

INSERT INTO test values ('1.1.1.1');
INSERT INTO test values (toIPv4('8.8.8.8'));

SELECT * FROM test ORDER BY ip;
SELECT ip IN IPv4StringToNum('1.1.1.1') FROM test order by ip;
SELECT ip IN ('1.1.1.1') FROM test order by ip;
SELECT ip IN IPv4StringToNum('8.8.8.8') FROM test order by ip;
