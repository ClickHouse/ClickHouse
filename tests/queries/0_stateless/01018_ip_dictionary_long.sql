-- Tags: long

SET send_logs_level = 'fatal';

SELECT '***ipv4 trie dict***';
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie
(
    prefix String,
    asn UInt32,
    cca2 String
)
engine = TinyLog;

-- numbers reordered to test sorting criteria too
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie
SELECT
  '255.255.255.255/' || toString((number + 1) * 13 % 33) AS prefix,
  toUInt32((number + 1) * 13 % 33) AS asn,
  'NA' as cca2
FROM system.numbers LIMIT 33;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.0.2', 1272, 'RU');
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.0.0/8', 1270, 'RU');
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('202.79.32.2', 11211, 'NP');
-- non-unique entries will be squashed into one
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('202.79.32.2', 11211, 'NP');
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('202.79.32.2', 11211, 'NP');
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('202.79.32.2', 11211, 'NP');
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('101.79.55.22', 11212, 'UK');

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict_ipv4_trie
(
  prefix String,
  asn UInt32,
  cca2 String
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' db currentDatabase() table 'table_ipv4_trie'))
LAYOUT(IP_TRIE())
LIFETIME(MIN 10 MAX 100)
SETTINGS(dictionary_use_async_executor=1, max_threads=8)
;

-- fuzzer
SELECT '127.0.0.0/24' = dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefixprefixprefixprefix', tuple(IPv4StringToNumOrDefault('127.0.0.0127.0.0.0'))); -- { serverError BAD_ARGUMENTS }

SELECT 0 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('0.0.0.0')));
SELECT 1 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('128.0.0.0')));
SELECT 2 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('192.0.0.0')));
SELECT 3 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('224.0.0.0')));
SELECT 4 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('240.0.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('248.0.0.0')));
SELECT 6 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('252.0.0.0')));
SELECT 7 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('254.0.0.0')));
SELECT 8 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.0.0.0')));
SELECT 9 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.128.0.0')));
SELECT 10 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.192.0.0')));
SELECT 11 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.224.0.0')));
SELECT 12 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.240.0.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.248.0.0')));
SELECT 14 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.252.0.0')));
SELECT 15 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.254.0.0')));
SELECT 16 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.0.0')));
SELECT 17 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.128.0')));
SELECT 18 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.192.0')));
SELECT 19 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.224.0')));
SELECT 20 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.240.0')));
SELECT 21 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.248.0')));
SELECT 22 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.252.0')));
SELECT 23 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.254.0')));
SELECT 24 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.0')));
SELECT 25 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.128')));
SELECT 26 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.192')));
SELECT 27 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.224')));
SELECT 28 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.240')));
SELECT 29 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.248')));
SELECT 30 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.252')));
SELECT 31 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.254')));
SELECT 32 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('255.255.255.255')));

SELECT 'RU' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'cca2', tuple(IPv4StringToNum('127.0.0.1')));

SELECT 1270 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('127.0.0.0')));
SELECT 1270 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('127.0.0.1')));
SELECT 1272 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('127.0.0.2')));
SELECT 1270 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('127.0.0.3')));
SELECT 1270 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('127.0.0.255')));

SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.0')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.1')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.2')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.3')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.255')));

SELECT 11212 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('101.79.55.22')));
SELECT 11212 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv6StringToNum('::ffff:654f:3716')));
SELECT 11212 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv6StringToNum('::ffff:101.79.55.22')));

SELECT 11211 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(IPv4StringToNum('202.79.32.2')));

-- check that dictionary works with aliased types `IPv4` and `IPv6`
SELECT 11211 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(toIPv4('202.79.32.2')));
SELECT 11212 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'asn', tuple(toIPv6('::ffff:101.79.55.22')));

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_from_ipv4_trie_dict
(
  prefix String,
  asn UInt32,
  cca2 String
) ENGINE = Dictionary({CLICKHOUSE_DATABASE:Identifier}.dict_ipv4_trie);

SELECT 1272 == asn AND 'RU' == cca2 FROM {CLICKHOUSE_DATABASE:Identifier}.table_from_ipv4_trie_dict
WHERE prefix == '127.0.0.2/32';

SELECT 37 == COUNT(*) FROM {CLICKHOUSE_DATABASE:Identifier}.table_from_ipv4_trie_dict;
SELECT 37 == COUNT(DISTINCT prefix) FROM {CLICKHOUSE_DATABASE:Identifier}.table_from_ipv4_trie_dict;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.table_from_ipv4_trie_dict;
DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.dict_ipv4_trie;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie;

SELECT '***ipv4 trie dict mask***';
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie
(
  prefix String,
  val UInt32
)
engine = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie
SELECT
  '255.255.255.255/' || toString(number) AS prefix,
  toUInt32(number) AS val
FROM VALUES ('number UInt32', 5, 13, 24, 30);

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict_ipv4_trie
(
  prefix String,
  val UInt32
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' db currentDatabase() table 'table_ipv4_trie'))
LAYOUT(IP_TRIE())
LIFETIME(MIN 10 MAX 100);

SELECT 0 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('0.0.0.0')));
SELECT 0 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('128.0.0.0')));
SELECT 0 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('192.0.0.0')));
SELECT 0 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('224.0.0.0')));
SELECT 0 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('240.0.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('248.0.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('252.0.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('254.0.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.0.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.128.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.192.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.224.0.0')));
SELECT 5 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.240.0.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.248.0.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.252.0.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.254.0.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.0.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.128.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.192.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.224.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.240.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.248.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.252.0')));
SELECT 13 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.254.0')));
SELECT 24 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.0')));
SELECT 24 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.128')));
SELECT 24 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.192')));
SELECT 24 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.224')));
SELECT 24 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.240')));
SELECT 24 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.248')));
SELECT 30 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.252')));
SELECT 30 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.254')));
SELECT 30 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('255.255.255.255')));

DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.dict_ipv4_trie;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.table_from_ipv4_trie_dict;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie;

SELECT '***ipv4 trie dict pt2***';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie ( prefix String, val UInt32 ) engine = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.0.0/8', 1);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.0.0/16', 2);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.0.0/24', 3);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.0.1/32', 4);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.127.0/32', 5);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.128.1/32', 6);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.255.0/32', 7);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.255.1/32', 8);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.0.255.255/32', 9);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.1.0.0/16', 10);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.1.1.0', 11);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.1.255.0/24', 12);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.254.0.0/15', 13);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.254.0.127', 14);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.255.0.0/16', 15);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.255.128.0/24', 16);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.255.128.1/32', 17);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.255.128.10/32', 18);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.255.128.128/25', 19);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.255.255.128/32', 20);
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ipv4_trie VALUES ('127.255.255.255/32', 21);

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict_ipv4_trie ( prefix String, val UInt32 )
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' db currentDatabase() table 'table_ipv4_trie'))
LAYOUT(IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1))
LIFETIME(MIN 10 MAX 100);

SELECT '127.0.0.0/24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.0.0.0')));
SELECT '127.0.0.1/32' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.0.0.1')));
SELECT '127.0.0.0/24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.0.0.127')));
SELECT '127.0.0.0/16' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.0.255.127')));
SELECT '127.255.0.0/16' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.255.127.127')));
SELECT '127.255.128.0/24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.255.128.9')));
SELECT '127.255.128.0/24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.255.128.127')));
SELECT '127.255.128.10/32' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.255.128.10')));
SELECT '127.255.128.128/25' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.255.128.255')));
SELECT '127.255.255.128/32' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'prefix', tuple(IPv4StringToNum('127.255.255.128')));

SELECT 3 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.0.0.0')));
SELECT 4 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.0.0.1')));
SELECT 3 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.0.0.127')));
SELECT 2 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.0.255.127')));
SELECT 15 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.255.127.127')));
SELECT 16 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.255.128.9')));
SELECT 16 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.255.128.127')));
SELECT 18 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.255.128.10')));
SELECT 19 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.255.128.255')));
SELECT 20 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv4StringToNum('127.255.255.128')));

SELECT 3 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7f00:0')));
SELECT 4 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7f00:1')));
SELECT 3 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7f00:7f')));
SELECT 2 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7f00:ff7f')));
SELECT 15 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7fff:7f7f')));
SELECT 16 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7fff:8009')));
SELECT 16 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7fff:807f')));
SELECT 18 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7fff:800a')));
SELECT 19 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7fff:80ff')));
SELECT 20 == dictGetUInt32({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', 'val', tuple(IPv6StringToNum('::ffff:7fff:ff80')));

SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.0')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.1')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.0.127')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.0.255.127')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.255.127.127')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.255.128.9')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.255.128.127')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.255.128.10')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.255.128.255')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('127.255.255.128')));

SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('128.127.127.127')));
SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('128.127.127.0')));
SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('255.127.127.0')));
SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('255.0.0.0')));
SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('0.0.0.0')));
SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ipv4_trie', tuple(IPv4StringToNum('1.1.1.1')));

SELECT '***ipv6 trie dict***';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie
(
    prefix String,
    val String
)
engine = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie VALUES ('101.79.55.22', 'JA'), ('127.0.0.1', 'RU'), ('2620:0:870::/48', 'US'), ('2a02:6b8:1::/48', 'UK'), ('2001:db8::/32', 'ZZ');

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie
SELECT
  'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/' || toString((number + 1) * 13 % 129) AS prefix,
  toString((number + 1) * 13 % 129) AS val
FROM system.numbers LIMIT 129;

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict_ip_trie
(
  prefix String,
  val String
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' db currentDatabase() table 'table_ip_trie'))
LAYOUT(IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1))
LIFETIME(MIN 10 MAX 100);

SELECT 'US' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('2620:0:870::')));
SELECT 'UK' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('2a02:6b8:1::')));
SELECT 'ZZ' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('2001:db8::')));
SELECT 'ZZ' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('2001:db8:ffff::')));

SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv6StringToNum('2001:db8:ffff::')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv6StringToNum('2001:db8:ffff:ffff::')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv6StringToNum('2001:db8:ffff:1::')));

SELECT '0' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('654f:3716::')));

SELECT 'JA' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('::ffff:654f:3716')));
SELECT 'JA' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('::ffff:101.79.55.22')));
SELECT 'JA' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('101.79.55.22')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv4StringToNum('127.0.0.1')));
SELECT 1 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv6StringToNum('::ffff:127.0.0.1')));

SELECT '2620:0:870::/48' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'prefix', tuple(IPv6StringToNum('2620:0:870::')));
SELECT '2a02:6b8:1::/48' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'prefix', tuple(IPv6StringToNum('2a02:6b8:1::1')));
SELECT '2001:db8::/32' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'prefix', tuple(IPv6StringToNum('2001:db8::1')));
SELECT '::ffff:101.79.55.22/128' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'prefix', tuple(IPv6StringToNum('::ffff:654f:3716')));
SELECT '::ffff:101.79.55.22/128' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'prefix', tuple(IPv6StringToNum('::ffff:101.79.55.22')));

SELECT '0' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('::0')));
SELECT '1' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('8000::')));
SELECT '2' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('c000::')));
SELECT '3' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('e000::')));
SELECT '4' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('f000::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('f800::')));
SELECT '6' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fc00::')));
SELECT '7' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fe00::')));
SELECT '8' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ff00::')));
SELECT '9' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ff80::')));
SELECT '10' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffc0::')));
SELECT '11' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffe0::')));
SELECT '12' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fff0::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fff8::')));
SELECT '14' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fffc::')));
SELECT '15' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fffe::')));
SELECT '16' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff::')));
SELECT '17' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:8000::')));
SELECT '18' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:c000::')));
SELECT '19' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:e000::')));
SELECT '20' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f000::')));
SELECT '21' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f800::')));
SELECT '22' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fc00::')));
SELECT '18' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:c000::')));
SELECT '19' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:e000::')));
SELECT '20' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f000::')));
SELECT '21' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f800::')));
SELECT '22' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fc00::')));
SELECT '23' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fe00::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ff00::')));
SELECT '25' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ff80::')));
SELECT '26' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffc0::')));
SELECT '27' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffe0::')));
SELECT '28' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fff0::')));
SELECT '29' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fff8::')));
SELECT '30' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fffc::')));
SELECT '31' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fffe::')));
SELECT '32' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff::')));
SELECT '33' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:8000::')));
SELECT '34' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:c000::')));
SELECT '35' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:e000::')));
SELECT '36' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:f000::')));
SELECT '37' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:f800::')));
SELECT '38' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fc00::')));
SELECT '39' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fe00::')));
SELECT '40' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ff00::')));
SELECT '41' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ff80::')));
SELECT '42' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffc0::')));
SELECT '43' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffe0::')));
SELECT '44' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fff0::')));
SELECT '45' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fff8::')));
SELECT '46' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fffc::')));
SELECT '47' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fffe::')));
SELECT '48' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:8000::')));
SELECT '50' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:c000::')));
SELECT '51' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:e000::')));
SELECT '52' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:f000::')));
SELECT '53' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:f800::')));
SELECT '54' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fc00::')));
SELECT '55' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fe00::')));
SELECT '56' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ff00::')));
SELECT '57' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ff80::')));
SELECT '58' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffc0::')));
SELECT '59' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffe0::')));
SELECT '60' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fff0::')));
SELECT '61' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fff8::')));
SELECT '62' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fffc::')));
SELECT '63' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fffe::')));
SELECT '64' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff::')));
SELECT '65' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:8000::')));
SELECT '66' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:c000::')));
SELECT '67' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:e000::')));
SELECT '68' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:f000::')));
SELECT '69' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:f800::')));
SELECT '70' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fc00::')));
SELECT '71' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fe00::')));
SELECT '72' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ff00::')));
SELECT '73' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ff80::')));
SELECT '74' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffc0::')));
SELECT '75' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffe0::')));
SELECT '76' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fff0::')));
SELECT '77' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fff8::')));
SELECT '78' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fffc::')));
SELECT '79' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fffe::')));
SELECT '80' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff::')));
SELECT '81' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:8000::')));
SELECT '82' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:c000::')));
SELECT '83' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:e000::')));
SELECT '84' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:f000::')));
SELECT '85' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:f800::')));
SELECT '86' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fc00::')));
SELECT '87' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fe00::')));
SELECT '88' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ff00::')));
SELECT '89' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ff80::')));
SELECT '90' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffc0::')));
SELECT '91' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffe0::')));
SELECT '92' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fff0::')));
SELECT '93' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fff8::')));
SELECT '94' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fffc::')));
SELECT '95' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fffe::')));
SELECT '96' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff::')));
SELECT '97' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:8000:0')));
SELECT '98' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:c000:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:e000:0')));
SELECT '100' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:f000:0')));
SELECT '101' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:f800:0')));
SELECT '102' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fc00:0')));
SELECT '103' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fe00:0')));
SELECT '104' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ff00:0')));
SELECT '105' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ff80:0')));
SELECT '106' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffc0:0')));
SELECT '107' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffe0:0')));
SELECT '108' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fff0:0')));
SELECT '109' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fff8:0')));
SELECT '110' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fffc:0')));
SELECT '111' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fffe:0')));
SELECT '112' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:0')));
SELECT '113' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:8000')));
SELECT '114' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:c000')));
SELECT '115' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:e000')));
SELECT '116' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:f000')));
SELECT '117' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:f800')));
SELECT '118' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fc00')));
SELECT '119' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fe00')));
SELECT '120' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff00')));
SELECT '121' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff80')));
SELECT '122' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffc0')));
SELECT '123' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffe0')));
SELECT '124' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fff0')));
SELECT '125' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fff8')));
SELECT '126' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffc')));
SELECT '127' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe')));
SELECT '128' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')));

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_from_ip_trie_dict
(
  prefix String,
  val String
) ENGINE = Dictionary({CLICKHOUSE_DATABASE:Identifier}.dict_ip_trie);

SELECT MIN(val == 'US') FROM {CLICKHOUSE_DATABASE:Identifier}.table_from_ip_trie_dict
WHERE prefix == '2620:0:870::/48';

SELECT 134 == COUNT(*) FROM {CLICKHOUSE_DATABASE:Identifier}.table_from_ip_trie_dict;

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.table_from_ip_trie_dict;
DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.dict_ip_trie;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie;

SELECT '***ipv6 trie dict mask***';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie
(
    prefix String,
    val String
)
engine = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie
SELECT
  'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/' || toString(number) AS prefix,
  toString(number) AS val
FROM VALUES ('number UInt32', 5, 13, 24, 48, 49, 99, 127);

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie VALUES ('101.79.55.22', 'JA');

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_ip_trie
SELECT
  '255.255.255.255/' || toString(number) AS prefix,
  toString(number) AS val
FROM VALUES ('number UInt32', 5, 13, 24, 30);

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict_ip_trie
(
  prefix String,
  val String
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' db currentDatabase() table 'table_ip_trie'))
LAYOUT(IP_TRIE())
LIFETIME(MIN 10 MAX 100);

SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv6StringToNum('::ffff:1:1')));

SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('654f:3716::')));
SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv6StringToNum('654f:3716::')));
SELECT 0 == dictHas({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', tuple(IPv6StringToNum('654f:3716:ffff::')));

SELECT 'JA' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('::ffff:654f:3716')));
SELECT 'JA' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('::ffff:101.79.55.22')));
SELECT 'JA' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('101.79.55.22')));

SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('::0')));
SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('8000::')));
SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('c000::')));
SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('e000::')));
SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('f000::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('f800::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fc00::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fe00::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ff00::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ff80::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffc0::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffe0::')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fff0::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fff8::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fffc::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('fffe::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:8000::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:c000::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:e000::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f000::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f800::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fc00::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:c000::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:e000::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f000::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:f800::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fc00::')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fe00::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ff00::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ff80::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffc0::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffe0::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fff0::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fff8::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fffc::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:fffe::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:8000::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:c000::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:e000::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:f000::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:f800::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fc00::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fe00::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ff00::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ff80::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffc0::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffe0::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fff0::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fff8::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fffc::')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:fffe::')));
SELECT '48' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:8000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:c000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:e000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:f000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:f800::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fc00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fe00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ff00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ff80::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffc0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffe0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fff0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fff8::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fffc::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:fffe::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:8000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:c000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:e000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:f000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:f800::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fc00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fe00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ff00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ff80::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffc0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffe0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fff0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fff8::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fffc::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:fffe::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:8000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:c000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:e000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:f000::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:f800::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fc00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fe00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ff00::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ff80::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffc0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffe0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fff0::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fff8::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fffc::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:fffe::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff::')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:8000:0')));
SELECT '49' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:c000:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:e000:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:f000:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:f800:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fc00:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fe00:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ff00:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ff80:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffc0:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffe0:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fff0:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fff8:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fffc:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:fffe:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:8000')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:c000')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:e000')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:f000')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:f800')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fc00')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fe00')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff00')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff80')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffc0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffe0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fff0')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fff8')));
SELECT '99' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffc')));
SELECT '127' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe')));
SELECT '127' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv6StringToNum('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')));

SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('0.0.0.0')));
SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('128.0.0.0')));
SELECT '' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('240.0.0.0')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('248.0.0.0')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('252.0.0.0')));
SELECT '5' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.240.0.0')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.248.0.0')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.252.0.0')));
SELECT '13' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.255.254.0')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.255.255.0')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.255.255.128')));
SELECT '24' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.255.255.248')));
SELECT '30' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.255.255.252')));
SELECT '30' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.255.255.254')));
SELECT '30' == dictGetString({CLICKHOUSE_DATABASE:String} || '.dict_ip_trie', 'val', tuple(IPv4StringToNum('255.255.255.255')));

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
