-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: dictionary source tables are not available on parallel-replica workers.

-- An IP_TRIE dictionary accepts a two-element key (address, prefix length) which
-- selects one subnet by exact match instead of by longest-prefix match.
-- See docs/reference/statements/create/dictionary/layouts/ip-trie.mdx.
-- dictGetOrDefault with a lazily evaluated default argument takes the
-- short-circuit lookup path, so matching rows get the attribute and the
-- remaining rows get the value of the default expression.
-- The default expression below divides by zero on exactly the prefix lengths
-- that the dictionary matches, so it can only produce a result when it is
-- evaluated on the non-matching rows alone. The divisor is written as a
-- column expression on purpose: a literal zero would be folded at analysis
-- time and would throw even on the lazy path.

SET short_circuit_function_evaluation = 'force_enable';

DROP DICTIONARY IF EXISTS d_v4;
DROP DICTIONARY IF EXISTS d_v6;
DROP TABLE IF EXISTS src_v4;
DROP TABLE IF EXISTS src_v6;
DROP TABLE IF EXISTS keys_v4;
DROP TABLE IF EXISTS keys_v6;

CREATE TABLE src_v4 (prefix String, cca2 String) ENGINE = TinyLog;
INSERT INTO src_v4 VALUES ('10.0.0.0/8', 'AA'), ('202.79.32.0/20', 'NP'), ('202.79.32.0/24', 'NQ');

CREATE DICTIONARY d_v4 (prefix String, cca2 String DEFAULT 'xx')
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'src_v4'))
LAYOUT(IP_TRIE)
LIFETIME(0);

CREATE TABLE keys_v4 (addr UInt32, len UInt8) ENGINE = TinyLog;
INSERT INTO keys_v4 VALUES (toUInt32(toIPv4('10.0.0.0')), 8), (toUInt32(toIPv4('202.79.32.0')), 20),
    (toUInt32(toIPv4('202.79.32.0')), 24), (toUInt32(toIPv4('202.79.32.0')), 21), (toUInt32(toIPv4('8.8.8.8')), 32);

SELECT 'v4', IPv4NumToString(addr), len,
    dictGetOrDefault('d_v4', 'cca2', (addr, len), concat('no-', toString(intDiv(len, len NOT IN (8, 20, 24)))))
FROM keys_v4
ORDER BY addr, len;

SELECT 'v4 matched', count()
FROM keys_v4
WHERE dictGetOrDefault('d_v4', 'cca2', (addr, len), concat('no-', toString(intDiv(len, len NOT IN (8, 20, 24))))) NOT LIKE 'no-%';

CREATE TABLE src_v6 (prefix String, cca2 String) ENGINE = TinyLog;
INSERT INTO src_v6 VALUES ('2001:db8::/32', 'BB'), ('2001:db8:1::/48', 'CC');

CREATE DICTIONARY d_v6 (prefix String, cca2 String DEFAULT 'xx')
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'src_v6'))
LAYOUT(IP_TRIE)
LIFETIME(0);

CREATE TABLE keys_v6 (addr FixedString(16), len UInt8) ENGINE = TinyLog;
INSERT INTO keys_v6 VALUES (IPv6StringToNum('2001:db8::'), 32), (IPv6StringToNum('2001:db8:1::'), 48),
    (IPv6StringToNum('2001:db8::'), 33);

SELECT 'v6', IPv6NumToString(addr), len,
    dictGetOrDefault('d_v6', 'cca2', (addr, len), concat('no-', toString(intDiv(len, len NOT IN (32, 48)))))
FROM keys_v6
ORDER BY addr, len;

SELECT 'v6 matched', count()
FROM keys_v6
WHERE dictGetOrDefault('d_v6', 'cca2', (addr, len), concat('no-', toString(intDiv(len, len NOT IN (32, 48))))) NOT LIKE 'no-%';

DROP DICTIONARY d_v4;
DROP DICTIONARY d_v6;
DROP TABLE keys_v4;
DROP TABLE keys_v6;
DROP TABLE src_v4;
DROP TABLE src_v6;
