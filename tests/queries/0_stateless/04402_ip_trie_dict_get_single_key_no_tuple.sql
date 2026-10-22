-- An ip_trie dictionary has a single-attribute (complex) key, so the key value
-- can be passed to dictGet / dictHas directly, without wrapping it in tuple.

DROP DICTIONARY IF EXISTS ip_trie_dict;
DROP TABLE IF EXISTS ip_trie_src;

CREATE TABLE ip_trie_src (prefix String, asn UInt32, cca2 String) ENGINE = TinyLog;
INSERT INTO ip_trie_src VALUES ('202.79.32.0/20', 17501, 'NP'), ('2001:db8::/32', 65536, 'ZZ');

CREATE DICTIONARY ip_trie_dict (prefix String, asn UInt32, cca2 String)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_src'))
LAYOUT(IP_TRIE)
LIFETIME(0);

SELECT 'dictGet IPv4';
SELECT dictGet('ip_trie_dict', 'cca2', toIPv4('202.79.32.10'));
SELECT dictGet('ip_trie_dict', 'cca2', tuple(toIPv4('202.79.32.10')));
SELECT dictGet('ip_trie_dict', 'asn', materialize(toIPv4('202.79.32.10')));

SELECT 'dictGet IPv6';
SELECT dictGet('ip_trie_dict', 'cca2', toIPv6('2001:db8::1'));
SELECT dictGet('ip_trie_dict', 'cca2', tuple(toIPv6('2001:db8::1')));

SELECT 'dictGet multiple attributes';
SELECT dictGet('ip_trie_dict', ('asn', 'cca2'), toIPv4('202.79.32.10'));

SELECT 'dictHas';
SELECT dictHas('ip_trie_dict', toIPv4('202.79.32.10'));
SELECT dictHas('ip_trie_dict', tuple(toIPv4('202.79.32.10')));
SELECT dictHas('ip_trie_dict', toIPv4('8.8.8.8'));

DROP DICTIONARY ip_trie_dict;
DROP TABLE ip_trie_src;
