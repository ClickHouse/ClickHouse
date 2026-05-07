-- Tags: no-parallel-replicas

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id   UInt32,
    ip   String,
    INDEX idx(ip) TYPE text(tokenizer = segmentation) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, '192.168.1.106'),
    (2, '10.0.0.1'),
    (3, '172.16.254.1'),
    (4, 'plaintext'),
    (5, '192.168.2.1');

SELECT 'exact match';
SELECT id FROM tab WHERE ip = '192.168.1.106' ORDER BY id;

SELECT 'prefix segment';
SELECT id FROM tab WHERE ip LIKE '192.168.%' ORDER BY id;

SELECT 'infix segment';
SELECT id FROM tab WHERE ip LIKE '%168.1%' ORDER BY id;

SELECT 'single octet';
SELECT id FROM tab WHERE ip LIKE '%172%' ORDER BY id;

SELECT 'no match';
SELECT id FROM tab WHERE ip LIKE '%999%' ORDER BY id;

SELECT 'no split char';
SELECT id FROM tab WHERE ip = 'plaintext' ORDER BY id;

DROP TABLE tab;

SELECT 'uuid test';

DROP TABLE IF EXISTS tab_uuid;

CREATE TABLE tab_uuid
(
    id   UInt32,
    u    String,
    INDEX idx(u) TYPE text(tokenizer = segmentation) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_uuid VALUES
    (1, '550e8400-e29b-41d4-a716-446655440000'),
    (2, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'),
    (3, 'not-a-full-uuid');

SELECT id FROM tab_uuid WHERE u LIKE '%e29b%' ORDER BY id;
SELECT id FROM tab_uuid WHERE u LIKE '%550e8400-e29b%' ORDER BY id;
SELECT id FROM tab_uuid WHERE u LIKE '%bbbb-cccc%' ORDER BY id;
SELECT id FROM tab_uuid WHERE u LIKE '%ffff%' ORDER BY id;

DROP TABLE tab_uuid;

SELECT 'ipv6 test';

DROP TABLE IF EXISTS tab_ipv6;

CREATE TABLE tab_ipv6
(
    id   UInt32,
    ip6  String,
    INDEX idx(ip6) TYPE text(tokenizer = segmentation) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_ipv6 VALUES
    (1, '2001:db8:85a3:0:0:8a2e:370:7334'),
    (2, 'fe80:0:0:0:202:b3ff:fe1e:8329');

SELECT id FROM tab_ipv6 WHERE ip6 LIKE '%85a3%' ORDER BY id;
SELECT id FROM tab_ipv6 WHERE ip6 LIKE '%db8:85a3%' ORDER BY id;
SELECT id FROM tab_ipv6 WHERE ip6 LIKE '%fe1e:8329' ORDER BY id;
SELECT id FROM tab_ipv6 WHERE ip6 LIKE '%0000%' ORDER BY id;

DROP TABLE tab_ipv6;

SELECT 'mac test';

DROP TABLE IF EXISTS tab_mac;

CREATE TABLE tab_mac
(
    id   UInt32,
    mac  String,
    INDEX idx(mac) TYPE text(tokenizer = segmentation) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_mac VALUES
    (1, 'AA:BB:CC:DD:EE:FF'),
    (2, '11-22-33-44-55-66'),
    (3, 'AA:BB:CC:11:22:33');

SELECT id FROM tab_mac WHERE mac LIKE '%BB:CC%' ORDER BY id;
SELECT id FROM tab_mac WHERE mac LIKE '%22-33%' ORDER BY id;
SELECT id FROM tab_mac WHERE mac LIKE 'AA:BB%' ORDER BY id;
SELECT id FROM tab_mac WHERE mac LIKE '%ZZ:ZZ%' ORDER BY id;

DROP TABLE tab_mac;

SELECT 'uri test';

DROP TABLE IF EXISTS tab_uri;

CREATE TABLE tab_uri
(
    id   UInt32,
    url  String,
    INDEX idx(url) TYPE text(tokenizer = segmentation) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_uri VALUES
    (1, 'https://api.example.com/v1/users/123'),
    (2, 'https://www.example.com/home'),
    (3, 'http://other.org/path/to/page'),
    (4, 'ftp://files.example.com/data');

SELECT id FROM tab_uri WHERE url LIKE '%api.example.com%' ORDER BY id;
SELECT id FROM tab_uri WHERE url LIKE '%v1/users%' ORDER BY id;
SELECT id FROM tab_uri WHERE url LIKE 'https://%' ORDER BY id;
SELECT id FROM tab_uri WHERE url LIKE '%example.com%' ORDER BY id;
SELECT id FROM tab_uri WHERE url LIKE '%unknown.net%' ORDER BY id;
SELECT id FROM tab_uri WHERE url LIKE '%.com%' ORDER BY id;
SELECT id FROM tab_uri WHERE url LIKE '%/v1/%' ORDER BY id;

DROP TABLE tab_uri;

SELECT 'timestamp test';

DROP TABLE IF EXISTS tab_ts;

CREATE TABLE tab_ts
(
    id  UInt32,
    ts  String,
    INDEX idx(ts) TYPE text(tokenizer = segmentation) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_ts VALUES
    (1, '2024-01-15 10:30:45'),
    (2, '2024-01-16 08:00:00'),
    (3, '2023-12-31 23:59:59'),
    (4, '2024-06-01 10:30:00');

SELECT id FROM tab_ts WHERE ts LIKE '2024-01%' ORDER BY id;
SELECT id FROM tab_ts WHERE ts LIKE '%10:30%' ORDER BY id;
SELECT id FROM tab_ts WHERE ts LIKE '%01-15 10%' ORDER BY id;
SELECT id FROM tab_ts WHERE ts LIKE '%2024%' ORDER BY id;
SELECT id FROM tab_ts WHERE ts LIKE '%1999%' ORDER BY id;

DROP TABLE tab_ts;
