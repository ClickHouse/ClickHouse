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
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_ip WHERE m['zzz'] = 'dead:beef::1') WHERE explain ILIKE '%Granules: 0/1%';
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

-- A perfect-affix pattern such as `zzz%` is rewritten to `startsWith` before index analysis, so it
-- never reaches the `like` / `notLike` branches. An inner wildcard keeps the pattern a `like`.
SELECT '-- a pattern the default does not satisfy still prunes';
SELECT count() FROM tab_str WHERE m['nokey'] LIKE '%zzz%' SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- notLike declines on the mirrored condition';
SELECT count() FROM tab_str WHERE m['nokey'] NOT LIKE '%' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_str WHERE m['nokey'] NOT LIKE '%' SETTINGS ignore_data_skipping_indices = 'idx';
SELECT count() FROM tab_str WHERE m['nokey'] NOT LIKE '%zzz%' SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- an array needle cannot be served by a mapKeys probe';
SELECT count() FROM tab_str WHERE multiSearchAny(m['nokey'], CAST([], 'Array(String)')) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_str WHERE multiSearchAny(m['nokey'], CAST([], 'Array(String)')) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_str WHERE multiSearchAny(m['nokey'], CAST([], 'Array(String)')) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_str WHERE multiSearchAny(m['abc'], ['hello']) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_str WHERE multiSearchAny(m['abc'], ['hello']) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_str WHERE m['abc'] = 'hello' SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- a set inside the subscript is prepared, not executed unready';
SELECT count() FROM tab_str WHERE m[if(0 IN (SELECT number FROM numbers(1)), 'abc', 'zzz')] = 'hello';

DROP TABLE tab_str;

DROP TABLE IF EXISTS tab_key_escape;
CREATE TABLE tab_key_escape (m Map(String, Nullable(String)), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_key_escape VALUES (map('abc\\+def', ''));

SELECT '-- a map key is a literal, not a pattern';
SELECT count() FROM tab_key_escape WHERE m['abc\\+def'] LIKE '' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_key_escape WHERE m['abc\\+def'] LIKE '' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_key_escape WHERE match(m['abc\\+def'], '') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_key_escape WHERE match(m['abc\\+def'], '') SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE tab_key_escape;

DROP TABLE IF EXISTS tab_key_escape_str;
CREATE TABLE tab_key_escape_str (m Map(String, String), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_key_escape_str VALUES (map('abc\\+def', 'zzz'));

SELECT '-- and the same key under a pattern the value satisfies';
SELECT count() FROM tab_key_escape_str WHERE m['abc\\+def'] LIKE '%zzz%' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_key_escape_str WHERE m['abc\\+def'] LIKE '%zzz%' SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE tab_key_escape_str;

DROP TABLE IF EXISTS tab_arr;
CREATE TABLE tab_arr (m Map(String, Array(String)), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_arr VALUES (map('abc', ['hello']));

SELECT '-- hasAny and hasAll reach the same substitution';
SELECT count() FROM tab_arr WHERE hasAny(m['abc'], ['hello']);
SELECT count() FROM tab_arr WHERE hasAll(m['abc'], ['hello']);
SELECT count() FROM tab_arr WHERE hasAny(m['nokey'], CAST([], 'Array(String)'));

DROP TABLE tab_arr;

DROP TABLE IF EXISTS tab_null;
CREATE TABLE tab_null (m Map(String, Nullable(String)), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_null VALUES (map('abc', 'hello'));

SELECT '-- a NULL default is not a match, so the index stays usable';
SELECT count() FROM tab_null WHERE m['nokey'] = '';
SELECT count() FROM tab_null WHERE m['nokey'] = '' SETTINGS ignore_data_skipping_indices = 'idx';
SELECT count() FROM tab_null WHERE m['nokey'] = '' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab_null WHERE m['abc'] = 'hello' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_null;

DROP TABLE IF EXISTS tab_ip_key;
CREATE TABLE tab_ip_key (m Map(IPv6, Nullable(String)), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_ip_key VALUES (map(toIPv6('2001:db8:1:2:3:4:5:6'), ''));

-- The map subcolumn spelling probes for the key serialized as text, while the index holds the raw
-- bytes of the key column, so a key that is not a string cannot be looked up in the index at all.
SELECT '-- a mapKeys index over a non-String key cannot serve the probe';
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS ignore_data_skipping_indices = 'idx';

-- Assert the disposition: both counts above are the same whether the index is declined or merely
-- fails to prune, and an absent key answers 0 either way.
SELECT '-- so it is declined for either spelling and for an absent key';
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_ip_key WHERE m[toIPv6('dead:beef::1')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_ip_key WHERE m[toIPv6('dead:beef::1')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE tab_ip_key;

DROP TABLE IF EXISTS tab_fs_key;
CREATE TABLE tab_fs_key (m Map(FixedString(4), String), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_fs_key VALUES (map(toFixedString('abcd', 4), 'hello'));

-- A key filling the whole `FixedString(4)` is the same byte string in the probe and in the index.
SELECT '-- a FixedString key is that representation, so it stays indexed';
SELECT count() FROM tab_fs_key WHERE m[toFixedString('abcd', 4)] = 'hello' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_fs_key WHERE m[toFixedString('abcd', 4)] = 'hello' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1;

DROP TABLE tab_fs_key;

DROP TABLE IF EXISTS tab_values;
CREATE TABLE tab_values (m Map(String, String), INDEX idx mapValues(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_values VALUES (map('abc', 'hello'));

SELECT '-- the mapValues carrier reads the same default';
SELECT count() FROM tab_values WHERE m['nokey'] = '';
SELECT count() FROM tab_values WHERE m['abc'] = 'hello' SETTINGS force_data_skipping_indices = 'idx';

SELECT '-- and a set inside the subscript on that carrier too';
SELECT count() FROM tab_values WHERE m[if(0 IN (SELECT number FROM numbers(1)), 'abc', 'zzz')] = 'hello';

DROP TABLE tab_values;

DROP TABLE IF EXISTS tab_values_ip;
CREATE TABLE tab_values_ip (m Map(String, IPv6), INDEX idx mapValues(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_values_ip VALUES (map('abc', toIPv6('2001:db8:1:2:3:4:5:6')));

-- The constant spells the IPv6 default as 15 characters, so the probe has trigrams to build. `'::'`
-- is shorter than the 3-gram width, which makes the probe empty and the granule survive regardless.
SELECT '-- the mapValues carrier over a non-String domain, arrayElement spelling';
SELECT count() FROM tab_values_ip WHERE m['nokey'] = '0:0:0:0:0:0:0:0' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_values_ip WHERE m['nokey'] = '0:0:0:0:0:0:0:0' SETTINGS optimize_functions_to_subcolumns = 0, ignore_data_skipping_indices = 'idx';

SELECT '-- and the same through the map subcolumn spelling';
SELECT count() FROM tab_values_ip WHERE m['nokey'] = '0:0:0:0:0:0:0:0' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_values_ip WHERE m['nokey'] = '0:0:0:0:0:0:0:0' SETTINGS optimize_functions_to_subcolumns = 1, ignore_data_skipping_indices = 'idx';

DROP TABLE tab_values_ip;

DROP TABLE IF EXISTS tab_token;
CREATE TABLE tab_token (m Map(String, UInt32), INDEX idx mapKeys(m) TYPE tokenbf_v1(512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_token VALUES (map('abc', 42));

SELECT '-- tokenbf_v1 shares the condition class';
SELECT count() FROM tab_token WHERE m['nokey'] = '0';
SELECT count() FROM tab_token WHERE m['nokey'] = '0' SETTINGS ignore_data_skipping_indices = 'idx';

DROP TABLE tab_token;
