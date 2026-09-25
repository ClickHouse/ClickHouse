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

-- The subscript divides by a length that is zero only for the empty map the evaluation substitutes.
SELECT '-- an error only the substituted default raises declines the index instead';
SELECT count() FROM tab_ip WHERE m[if(intDiv(1, length(toString(m)) - 2) = 0, 'abc', 'zzz')] = '2001:db8:1:2:3:4:5:6';
SELECT count() FROM tab_ip WHERE m[if(intDiv(1, length(toString(m)) - 2) = 0, 'abc', 'zzz')] = '2001:db8:1:2:3:4:5:6' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- The row count depends on where the pipeline stopped, so only the absence of that error is asserted.
-- The 'throw' arm below is the control: it shows the deadline really is crossed during analysis here.
SELECT '-- a passed deadline under `break` does not surface that error either';
SELECT count() FROM tab_ip WHERE m[if(intDiv(1, length(toString(m)) - 2) = 0, 'abc', 'zzz')] = '2001:db8:1:2:3:4:5:6' SETTINGS max_execution_time = 0.000001, timeout_overflow_mode = 'break' FORMAT Null;
SELECT count() FROM tab_ip WHERE m[if(intDiv(1, length(toString(m)) - 2) = 0, 'abc', 'zzz')] = '2001:db8:1:2:3:4:5:6' SETTINGS max_execution_time = 0.000001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

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
SELECT count() FROM tab_key_escape WHERE match(m['abc\\+def'], '') SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM tab_key_escape WHERE match(m['abc\\+def'], '') SETTINGS optimize_functions_to_subcolumns = 1, force_data_skipping_indices = 'idx';

DROP TABLE tab_key_escape;

DROP TABLE IF EXISTS tab_key_escape_str;
CREATE TABLE tab_key_escape_str (m Map(String, String), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO tab_key_escape_str VALUES (map('abc\\+def', 'zzz'));

SELECT '-- and the same key under a pattern the value satisfies';
SELECT count() FROM tab_key_escape_str WHERE m['abc\\+def'] LIKE '%zzz%' SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM tab_key_escape_str WHERE m['abc\\+def'] LIKE '%zzz%' SETTINGS optimize_functions_to_subcolumns = 1, force_data_skipping_indices = 'idx';

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

-- The subcolumn spelling probes for the key serialized as text and that text is converted back into
-- the index domain, so it can use the index. The `arrayElement` spelling needs a String, FixedString
-- or Array key constant and declines an IPv6 one.
SELECT '-- a mapKeys index over a non-String key serves the subcolumn probe only';
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS ignore_data_skipping_indices = 'idx';

-- An absent key reads as NULL for a `Nullable(String)` map value, and `= ''` does not hold for NULL,
-- so the row does not match and pruning its granule keeps the unindexed answer.
SELECT '-- the subcolumn probe keeps the present key and answers 0 for an absent one';
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_ip_key WHERE m[toIPv6('2001:db8:1:2:3:4:5:6')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_ip_key WHERE m[toIPv6('dead:beef::1')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_ip_key WHERE m[toIPv6('dead:beef::1')] = '' SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1;
SELECT count() FROM tab_ip_key WHERE m[toIPv6('dead:beef::1')] = '' SETTINGS use_skip_indexes = 0, optimize_functions_to_subcolumns = 1;

DROP TABLE tab_ip_key;

DROP TABLE IF EXISTS tab_ip_key_pattern;
CREATE TABLE tab_ip_key_pattern (m Map(IPv6, String), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab_ip_key_pattern VALUES (map(toIPv6('2001:db8::abcd:1'), 'v'));

-- A pattern atom probes the index with the substituted key, so the key text needs the same conversion
-- into the index domain that equality does. Tokenized as text against a binary key it matches nothing
-- and prunes the granule that holds the key. Forcing the index asserts the probe stays active, so a
-- conversion that silently stopped working would fail here rather than read as a pass.
SELECT '-- a pattern atom over a non-String map key keeps the matching row';
SELECT count() FROM tab_ip_key_pattern WHERE m.`key_2001:db8::abcd:1` LIKE '%v%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab_ip_key_pattern WHERE m.`key_2001:db8::abcd:1` LIKE '%v%' SETTINGS ignore_data_skipping_indices = 'idx';
SELECT count() FROM tab_ip_key_pattern WHERE match(m.`key_2001:db8::abcd:1`, 'v') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab_ip_key_pattern WHERE match(m.`key_2001:db8::abcd:1`, 'v') SETTINGS ignore_data_skipping_indices = 'idx';

-- A substring or token atom tokenizes its needle, which describes the substituted key only while the
-- index holds it as text, so such an atom declines a binary key instead of mispruning it.
SELECT '-- a substring or token atom declines a non-String map key';
SELECT count() FROM tab_ip_key_pattern WHERE startsWith(m.`key_2001:db8::abcd:1`, 'v');
SELECT count() FROM tab_ip_key_pattern WHERE startsWith(m.`key_2001:db8::abcd:1`, 'v') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM tab_ip_key_pattern WHERE endsWith(m.`key_2001:db8::abcd:1`, 'v');
SELECT count() FROM tab_ip_key_pattern WHERE hasToken(m.`key_2001:db8::abcd:1`, 'v');
SELECT count() FROM tab_ip_key_pattern WHERE hasToken(m.`key_2001:db8::abcd:1`, 'v') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab_ip_key_pattern;

DROP TABLE IF EXISTS tab_ip_key_array;
CREATE TABLE tab_ip_key_array (m Map(IPv6, Array(String)), INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO tab_ip_key_array VALUES (map(toIPv6('2001:db8::abcd:1'), ['']));

-- `has` reaches the same tokenizer with a scalar needle, so the array needle guard above does not cover
-- it; an empty needle also leaves the array default unmatched, so the default guard does not either.
SELECT '-- and so does an array membership atom';
SELECT count() FROM tab_ip_key_array WHERE has(m.`key_2001:db8::abcd:1`, '');
SELECT count() FROM tab_ip_key_array WHERE has(m.`key_2001:db8::abcd:1`, '') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE tab_ip_key_array;

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

SELECT '-- and a set inside the subscript on that carrier too, whether or not its elements are stored';
SELECT count() FROM tab_values WHERE m[if(0 IN (SELECT number FROM numbers(1)), 'abc', 'zzz')] = 'hello';
SELECT count() FROM tab_values WHERE m[if(0 IN (SELECT number FROM numbers(5)), 'abc', 'zzz')] = 'hello'
SETTINGS use_index_for_in_with_subqueries_max_values = 1, force_data_skipping_indices = 'idx';

-- `sleep` refuses constant folding so that query analysis cannot run it, which the guard would.
SELECT '-- a subscript analysis may not evaluate declines the carrier';
SELECT count() FROM tab_values WHERE m[if(sleep(0) = 0, 'abc', 'zzz')] = 'hello';
SELECT count() FROM tab_values WHERE m[if(sleep(0) = 0, 'abc', 'zzz')] = 'hello' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

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
