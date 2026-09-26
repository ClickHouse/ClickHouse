-- `dictGetOrDefault` of an `Array` attribute from an `ip_trie` dictionary for a mix of matching and
-- non-matching addresses. The default is only defined for the addresses that are not in the dictionary.

DROP DICTIONARY IF EXISTS ip_trie_array_dictionary;
DROP TABLE IF EXISTS ip_trie_array_source_table;
DROP TABLE IF EXISTS ip_trie_array_probe_table;
DROP TABLE IF EXISTS ip_trie_array_probe_v6_table;

CREATE TABLE ip_trie_array_source_table
(
    prefix String,
    array_value Array(String)
) ENGINE = TinyLog;

INSERT INTO ip_trie_array_source_table VALUES ('10.0.0.0/8', ['a', 'b']), ('192.168.0.0/16', ['c']), ('2001:db8::/32', ['d', 'e']);

CREATE TABLE ip_trie_array_probe_table
(
    ip String,
    is_missing UInt8
) ENGINE = TinyLog;

INSERT INTO ip_trie_array_probe_table VALUES ('10.1.2.3', 0), ('8.8.8.8', 1), ('192.168.1.1', 0), ('1.2.3.4', 1);

CREATE TABLE ip_trie_array_probe_v6_table
(
    ip String,
    is_missing UInt8
) ENGINE = TinyLog;

INSERT INTO ip_trie_array_probe_v6_table VALUES ('2001:db8::1', 0), ('2620:0:870::1', 1), ('2001:db8:1::1', 0), ('2a02:6b8:1::1', 1);

CREATE DICTIONARY ip_trie_array_dictionary
(
    prefix String,
    array_value Array(String)
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_array_source_table' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(IP_TRIE());

SELECT 'IPv4';
SELECT ip, dictGetOrDefault('ip_trie_array_dictionary', 'array_value', toIPv4(ip), [toString(intDiv(1, is_missing))])
FROM ip_trie_array_probe_table
ORDER BY ip
SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'IPv6';
SELECT ip, dictGetOrDefault('ip_trie_array_dictionary', 'array_value', IPv6StringToNum(ip), [toString(intDiv(1, is_missing))])
FROM ip_trie_array_probe_v6_table
ORDER BY ip
SETTINGS short_circuit_function_evaluation = 'enable';

DROP DICTIONARY ip_trie_array_dictionary;
DROP TABLE ip_trie_array_source_table;
DROP TABLE ip_trie_array_probe_table;
DROP TABLE ip_trie_array_probe_v6_table;
