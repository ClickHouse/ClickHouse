-- Random settings limits: index_granularity=(8192, None)

-- A key the row does not have reads as the map value type's default, so a `mapKeys` / `mapValues`
-- bloom filter index must not prune a granule when the predicate accepts that default.

DROP TABLE IF EXISTS tab_ip;
CREATE TABLE tab_ip (m Map(String, IPv6), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_ip VALUES (map('abc', toIPv6('2001:db8:1:2:3:4:5:6')));

SELECT '-- absent key reads as the IPv6 default, arrayElement spelling';
SELECT count() FROM tab_ip WHERE m['nokey'] = '::' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_ip WHERE m['nokey'] = '::' SETTINGS optimize_functions_to_subcolumns = 0, ignore_data_skipping_indices = 'idx';

SELECT '-- the same through the map subcolumn spelling';
SELECT count() FROM tab_ip WHERE m['nokey'] = '::' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_ip WHERE m['nokey'] = '::' SETTINGS optimize_functions_to_subcolumns = 1, ignore_data_skipping_indices = 'idx';

SELECT '-- a constant the default cannot satisfy still prunes';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_ip WHERE m['zzz'] = 'dead:beef::1') WHERE explain ILIKE '%Granules: 0/1%';
SELECT count() FROM tab_ip WHERE m['abc'] = '2001:db8:1:2:3:4:5:6' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_ip WHERE m['abc'] = '2001:db8:1:2:3:4:5:6' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1;

SELECT '-- a negating predicate needs the mirrored condition';
SELECT count() FROM tab_ip WHERE m['nokey'] != 'dead:beef::1' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab_ip WHERE m['nokey'] != '::' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_ip WHERE m['nokey'] != '::' SETTINGS ignore_data_skipping_indices = 'idx';

SELECT '-- evaluating the predicate raises what the scan raises';
SELECT count() FROM tab_ip WHERE m['nokey'] != 'zzz'; -- { serverError CANNOT_PARSE_IPV6 }
SELECT count() FROM tab_ip WHERE m['nokey'] != 'zzz' SETTINGS ignore_data_skipping_indices = 'idx'; -- { serverError CANNOT_PARSE_IPV6 }

DROP TABLE tab_ip;

DROP TABLE IF EXISTS tab_str;
CREATE TABLE tab_str (m Map(String, String), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_str VALUES (map('abc', 'hello'));

SELECT '-- the whole predicate decides, not equality with the default';
SELECT count() FROM tab_str WHERE m['nokey'] LIKE '%';
SELECT count() FROM tab_str WHERE m['nokey'] LIKE '%' SETTINGS ignore_data_skipping_indices = 'idx';

SELECT '-- equality with the String default keeps working';
SELECT count() FROM tab_str WHERE m['nokey'] = '';
SELECT count() FROM tab_str WHERE m['nokey'] = '' SETTINGS ignore_data_skipping_indices = 'idx';

DROP TABLE tab_str;

DROP TABLE IF EXISTS tab_null;
CREATE TABLE tab_null (m Map(String, Nullable(String)), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_null VALUES (map('abc', 'hello'));

SELECT '-- a NULL default is not a match, so the index stays usable';
SELECT count() FROM tab_null WHERE m['nokey'] = '';
SELECT count() FROM tab_null WHERE m['nokey'] = '' SETTINGS ignore_data_skipping_indices = 'idx';
SELECT count() FROM tab_null WHERE m['abc'] = 'hello' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_null;

DROP TABLE IF EXISTS tab_values;
CREATE TABLE tab_values (m Map(String, String), INDEX idx mapValues(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_values VALUES (map('abc', 'hello'));

SELECT '-- the mapValues carrier reads the same default';
SELECT count() FROM tab_values WHERE m['nokey'] = '';
SELECT count() FROM tab_values WHERE m['abc'] = 'hello' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_values;

DROP TABLE IF EXISTS tab_token;
CREATE TABLE tab_token (m Map(String, UInt32), INDEX idx mapKeys(m) TYPE tokenbf_v1(512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_token VALUES (map('abc', 42));

SELECT '-- tokenbf_v1 shares the condition class';
SELECT count() FROM tab_token WHERE m['nokey'] = '0';
SELECT count() FROM tab_token WHERE m['nokey'] = '0' SETTINGS ignore_data_skipping_indices = 'idx';

DROP TABLE tab_token;
