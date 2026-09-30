-- FixedString(N, 'representation'): values are stored as N raw bytes and converted from/to Hex, Base64, Base64URL or Base58 text.

SELECT '-- type names';
SELECT toTypeName(CAST('abc' AS FixedString(3))), toTypeName(CAST('abc' AS FixedString(3, 'Raw')));
SELECT toTypeName(CAST('0102' AS FixedString(2, 'hex'))), toTypeName(CAST('AQI=' AS FixedString(2, 'BASE64'))), toTypeName(CAST('AQI' AS FixedString(2, 'base64url'))), toTypeName(CAST('5y' AS FixedString(2, 'base58')));
SELECT CAST('abc' AS FixedString(3, 'Unknown')); -- { serverError BAD_ARGUMENTS }
SELECT CAST('abc' AS FixedString(3, 1)); -- { serverError UNEXPECTED_AST_STRUCTURE }
SELECT CAST('abc' AS FixedString(3, 'Hex', 'Hex')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE IF EXISTS fs_accounts;
DROP TABLE IF EXISTS fs_transfers;
DROP TABLE IF EXISTS fs_migration;

-- 32 bytes keys, the text order of the Base58 values differs from the order of bytes. index_granularity = 1 so that wrong primary key analysis would lose rows.
CREATE TABLE fs_accounts
(
    id FixedString(32, 'Base58'),
    id64 FixedString(32, 'Base64'),
    id64url FixedString(32, 'Base64URL'),
    idhex FixedString(32, 'Hex'),
    name String
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO fs_accounts (id, name) VALUES
    ('11111111111111111111111111111111', 'zero'),
    ('V8r5WP8Hq5HN6PN1kAXYUcskoSgvSVoYAqqjwsXQNtw', 'beta'),
    ('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j', 'alpha'),
    ('EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y', 'gamma'),
    ('ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF', 'delta');

-- The same bytes in the other representations.
ALTER TABLE fs_accounts UPDATE id64 = id, id64url = id, idhex = id WHERE 1 SETTINGS mutations_sync = 2;

SELECT '-- output in the declared representation, sorted by bytes';
SELECT id, id64, id64url, idhex, name FROM fs_accounts ORDER BY id;

SELECT '-- output formats';
SELECT id, id64 FROM fs_accounts WHERE name = 'gamma' FORMAT TSV;
SELECT id, id64 FROM fs_accounts WHERE name = 'gamma' FORMAT CSV;
SELECT id, id64 FROM fs_accounts WHERE name = 'gamma' FORMAT JSONEachRow;
SELECT id, id64 FROM fs_accounts WHERE name = 'gamma' FORMAT Values;
SELECT '';
SELECT id64, id64url, idhex FROM fs_accounts WHERE name = 'alpha' FORMAT JSONEachRow;
SELECT id64 FROM fs_accounts WHERE name = 'alpha' FORMAT JSONEachRow SETTINGS output_format_json_escape_forward_slashes = 0;
SELECT id64, idhex FROM fs_accounts WHERE name = 'alpha' FORMAT CSV;
SELECT id64 FROM fs_accounts WHERE name = 'alpha' FORMAT XML SETTINGS output_format_write_statistics = 0;
SELECT '';

SELECT '-- stored bytes';
SELECT length(id), hex(id) = idhex, base58Encode(id) = toString(id), base64Encode(id64) = toString(id64) FROM fs_accounts WHERE name = 'gamma';

SELECT '-- toString and CAST to String return the text representation';
SELECT toString(id), CAST(id AS String), toString(id64), toString(id64url), toString(idhex) FROM fs_accounts WHERE name = 'alpha';
SELECT toTypeName(toString(id)), toString(id) = 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j' FROM fs_accounts WHERE name = 'alpha';

SELECT '-- equality with string constants uses the primary key';
SELECT name FROM fs_accounts WHERE id = 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y' SETTINGS force_primary_key = 1;
SELECT name FROM fs_accounts WHERE 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y' = id SETTINGS force_primary_key = 1;
SELECT name FROM fs_accounts WHERE id != 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y' ORDER BY name;
SELECT count() FROM fs_accounts WHERE id64 = base64Encode(base58Decode('EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y'));
SELECT count() FROM fs_accounts WHERE id = 'not base58 0OIl'; -- { serverError INCORRECT_DATA }
SELECT count() FROM fs_accounts WHERE id = '1'; -- { serverError INCORRECT_DATA }

SELECT '-- comparison with FixedString(32) holding the same bytes';
SELECT name FROM fs_accounts WHERE id = toFixedString(base58Decode('EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y'), 32) SETTINGS force_primary_key = 1;
SELECT count() FROM fs_accounts WHERE id = CAST(id64 AS FixedString(32));
SELECT count() FROM fs_accounts WHERE id = id64; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM fs_accounts WHERE id = CAST('abc' AS FixedString(3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- comparison with a String column';
SELECT count() FROM fs_accounts WHERE id = materialize('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j');
SELECT count() FROM fs_accounts WHERE id = materialize('invalid!'); -- { serverError INCORRECT_DATA }

SELECT '-- toString is not monotonic for Base58 and Base64: neither the primary key nor reading in order must rely on it';
SELECT name FROM fs_accounts WHERE toString(id) = 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y';
SELECT name FROM fs_accounts WHERE toString(id) >= 'T' ORDER BY name;
SELECT groupArray(s) FROM (SELECT toString(id) AS s FROM fs_accounts ORDER BY toString(id));
SELECT groupArray(s) FROM (SELECT toString(id) AS s FROM fs_accounts ORDER BY toString(id) DESC LIMIT 2);

SELECT '-- top-K filtering uses the stored bytes as the threshold';
SELECT id FROM fs_accounts ORDER BY id LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100;
SELECT id FROM fs_accounts ORDER BY id DESC LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100;
SELECT id FROM fs_accounts ORDER BY id LIMIT 2 SETTINGS use_top_k_dynamic_filtering = 0;

SELECT '-- IN';
SELECT name FROM fs_accounts WHERE (id, name) IN ((toFixedString(base58Decode('EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y'), 32), 'gamma'), ('ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF', 'delta')) ORDER BY name;
SELECT name FROM fs_accounts WHERE id IN ('EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y', 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j') ORDER BY name SETTINGS force_primary_key = 1;
SELECT name FROM fs_accounts WHERE id NOT IN ('EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y', 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j') ORDER BY name;
SELECT name FROM fs_accounts WHERE id IN (SELECT id FROM fs_accounts WHERE name IN ('beta', 'delta')) ORDER BY name;
SELECT name FROM fs_accounts WHERE id IN (SELECT 'ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF') ORDER BY name;
SELECT 'ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF' IN (SELECT id FROM fs_accounts);
SELECT count() FROM fs_accounts WHERE id IN ('invalid!'); -- { serverError INCORRECT_DATA }

SELECT '-- arrays';
SELECT has([id], 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y'), hasAny([id], ['EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y']), hasAll([id], [id, 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y']), indexOf([id64, id64], 'xnoO/WwfBo+ye0zSkAoUMz07UIgt1RE28npxtsYGGCU=') FROM fs_accounts WHERE name = 'gamma';
SELECT name, has(['EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y', 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j'], id), indexOf(['TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j', 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y'], id), has([toString(id)], id) FROM fs_accounts ORDER BY name;
SELECT name FROM fs_accounts WHERE has(['EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y', 'ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF'], id) ORDER BY name;
SELECT has(['invalid!'], id) FROM fs_accounts; -- { serverError INCORRECT_DATA }
SELECT toTypeName([id, 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j']), [id, 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j'] FROM fs_accounts WHERE name = 'gamma';
SELECT groupArray(id) FROM (SELECT id FROM fs_accounts WHERE name IN ('gamma', 'alpha') ORDER BY id);
-- There is no common FixedString type for different representations.
SELECT [id, id64] FROM fs_accounts SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }
SELECT toTypeName([id, id64]), [id, id64] FROM fs_accounts WHERE name = 'gamma' SETTINGS use_variant_as_common_type = 1;

SELECT '-- common type with String';
SELECT toTypeName(if(name = 'gamma', id, 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j')), if(name = 'gamma', id, 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j') FROM fs_accounts WHERE name IN ('gamma', 'delta') ORDER BY name;
SELECT id FROM (SELECT id FROM fs_accounts WHERE name = 'delta' UNION ALL SELECT 'V8r5WP8Hq5HN6PN1kAXYUcskoSgvSVoYAqqjwsXQNtw') ORDER BY id;
SELECT if(name = 'gamma', id, 'not base58!') FROM fs_accounts; -- { serverError INCORRECT_DATA }

SELECT '-- JOIN';
CREATE TABLE fs_transfers
(
    source FixedString(32, 'Base58'),
    destination FixedString(32, 'Base58'),
    destination_str String,
    amount UInt64,
    INDEX destination_bf destination TYPE bloom_filter GRANULARITY 1,
    INDEX destination_set destination TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY (source, amount) SETTINGS index_granularity = 1;
INSERT INTO fs_transfers SELECT a.id, b.id, toString(b.id), number FROM fs_accounts AS a, fs_accounts AS b, numbers(2) AS n WHERE a.name = 'gamma' AND b.name != 'gamma';
SELECT a.name, sum(t.amount), count() FROM fs_transfers AS t INNER JOIN fs_accounts AS a ON t.destination = a.id GROUP BY a.name ORDER BY a.name;
SELECT a.name, count() FROM fs_transfers AS t INNER JOIN fs_accounts AS a ON t.destination_str = a.id GROUP BY a.name ORDER BY a.name;
SELECT a.name, count() FROM fs_transfers AS t INNER JOIN fs_accounts AS a ON toFixedString(base58Decode(t.destination_str), 32) = a.id GROUP BY a.name ORDER BY a.name;
SELECT t.destination, a.name FROM fs_transfers AS t LEFT JOIN fs_accounts AS a ON t.destination = a.id WHERE t.amount = 0 ORDER BY t.destination;

SELECT '-- skipping indexes';
SELECT count() FROM fs_transfers WHERE destination = 'ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF' SETTINGS force_data_skipping_indices = 'destination_bf';
SELECT count() FROM fs_transfers WHERE destination IN ('ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF', 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j') SETTINGS force_data_skipping_indices = 'destination_bf';
SELECT count() FROM fs_transfers WHERE destination = 'ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF' SETTINGS force_data_skipping_indices = 'destination_set';
SELECT count() FROM fs_transfers WHERE has(['ahUhdBiK524uzg5ihjoLrU5fnsaWnpzbJoKeAQPjeyF'], destination) SETTINGS force_data_skipping_indices = 'destination_bf';

SELECT '-- GROUP BY, DISTINCT, ORDER BY, aggregate functions';
SELECT destination, count() FROM fs_transfers GROUP BY destination ORDER BY destination;
SELECT DISTINCT source FROM fs_transfers;
SELECT uniqExact(destination), min(destination), max(destination), argMax(destination, (amount, destination)), any(source) FROM fs_transfers;
SELECT toTypeName(min(destination)), toTypeName(groupArray(destination)) FROM fs_transfers;

SELECT '-- Nullable, LowCardinality, Map, Tuple';
SELECT CAST(NULL AS Nullable(FixedString(32, 'Base58'))), CAST('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j' AS Nullable(FixedString(32, 'Base58')));
SELECT CAST('invalid!' AS Nullable(FixedString(32, 'Base58'))), accurateCastOrNull('invalid!', 'FixedString(32, \'Base58\')'), accurateCastOrNull('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j', 'FixedString(32, \'Base58\')');
SELECT CAST(materialize('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j') AS LowCardinality(FixedString(32, 'Base58'))) AS x, toTypeName(x), x = 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j';
SELECT CAST(map('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j', 'AQI='), 'Map(FixedString(32, \'Base58\'), FixedString(2, \'Base64\'))') AS m, toTypeName(m), m['TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j'], mapContains(m, 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j'), mapContainsKey(m, 'EMmg4BpEzf3RJwoZH9HFzQ1mmdm7gQEYqFa8k4ib3s4Y');
SELECT CAST(('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j', '0102'), 'Tuple(a FixedString(32, \'Base58\'), b FixedString(2, \'Hex\'))') AS t, t.a = 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j', t.b = '0x0102';
SELECT x FROM (SELECT CAST(arrayJoin(['TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j', NULL]) AS Nullable(FixedString(32, 'Base58'))) AS x) WHERE x = 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j';

SELECT '-- Dynamic and Variant keep the representation';
SELECT CAST(CAST('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j' AS FixedString(32, 'Base58')) AS Dynamic) AS d, dynamicType(d);
SELECT CAST(CAST('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j' AS FixedString(32, 'Base58')) AS Variant(UInt64, FixedString(32, 'Base58'))) AS v, variantType(v);
DROP TABLE IF EXISTS fs_dynamic;
CREATE TABLE fs_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO fs_dynamic SELECT CAST('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j' AS FixedString(32, 'Base58'));
INSERT INTO fs_dynamic SELECT CAST('0x0102' AS FixedString(2, 'Hex'));
SELECT d, dynamicType(d) FROM fs_dynamic ORDER BY dynamicType(d);
DROP TABLE fs_dynamic;
-- With max_types = 1, the second type is stored in the shared variant.
CREATE TABLE fs_dynamic (d Dynamic(max_types = 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO fs_dynamic VALUES (42::UInt64);
INSERT INTO fs_dynamic SELECT CAST('TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j' AS FixedString(32, 'Base58'));
OPTIMIZE TABLE fs_dynamic FINAL;
SELECT d, dynamicType(d), isDynamicElementInSharedData(d) FROM fs_dynamic ORDER BY dynamicType(d);
SELECT groupArray(d) FROM (SELECT d FROM fs_dynamic ORDER BY dynamicType(d));
DROP TABLE fs_dynamic;

SELECT '-- input formats';
SELECT * FROM format(JSONEachRow, 'id FixedString(32, \'Base58\'), h FixedString(2, \'Hex\')', '{"id":"TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j","h":"0x0aFF"}');
SELECT * FROM format(CSV, 'id FixedString(32, \'Base58\'), b FixedString(1, \'Base64URL\')', '"TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j",_w');
SELECT * FROM format(TSV, 'id FixedString(32, \'Base58\'), b FixedString(1, \'Base64\')', 'TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j\t/w==');
SELECT * FROM format(JSONEachRow, 'id FixedString(32, \'Base58\')', '{"id":"invalid!"}'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(JSONEachRow, 'id FixedString(32, \'Base58\')', '{"id":"invalid!"}\n{"id":"TNEYcisRwH4QpzGBov7zrizG9VnddqNbDaYEqsfWe2j"}') SETTINGS input_format_allow_errors_num = 1;

SELECT '-- invalid values in INSERT';
INSERT INTO fs_accounts (id, name) VALUES ('invalid!', 'invalid'); -- { error INCORRECT_DATA }
INSERT INTO fs_accounts (id, name) SELECT 'invalid!', 'invalid'; -- { serverError INCORRECT_DATA }
SELECT count() FROM fs_accounts;

SELECT '-- persisted type';
DETACH TABLE fs_accounts;
ATTACH TABLE fs_accounts;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'fs_accounts' ORDER BY position;
SELECT id FROM fs_accounts WHERE name = 'beta';

SELECT '-- migration from FixedString(32)';
CREATE TABLE fs_migration (id FixedString(32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO fs_migration SELECT toFixedString(base58Decode('V8r5WP8Hq5HN6PN1kAXYUcskoSgvSVoYAqqjwsXQNtw'), 32);
ALTER TABLE fs_migration MODIFY COLUMN id FixedString(32, 'Base58') SETTINGS mutations_sync = 2;
SELECT id, toTypeName(id) FROM fs_migration WHERE id = 'V8r5WP8Hq5HN6PN1kAXYUcskoSgvSVoYAqqjwsXQNtw';

DROP TABLE fs_accounts;
DROP TABLE fs_transfers;
DROP TABLE fs_migration;

SELECT '-- Base58 of other sizes';
SELECT CAST('2g' AS FixedString(1, 'Base58')), hex(CAST('1' AS FixedString(1, 'Base58'))), hex(CAST('11' AS FixedString(2, 'Base58')));
SELECT CAST(base58Encode(unhex('00000102030405060708090a0b0c0d0e')) AS FixedString(16, 'Base58')) AS x, hex(x);
SELECT CAST(base58Encode(repeat('\xFF', 20)) AS FixedString(20, 'Base58')) AS x, length(toString(x));
SELECT CAST(base58Encode(repeat('\xFF', 64)) AS FixedString(64, 'Base58')) AS x, length(toString(x));
SELECT CAST(repeat('1', 64) AS FixedString(64, 'Base58')) = toFixedString(repeat('\0', 64), 64);
-- A value decoded to more than N bytes must be rejected without writing past the column buffer.
SELECT CAST(repeat('z', 10000) AS FixedString(16, 'Base58')); -- { serverError INCORRECT_DATA }
SELECT CAST(repeat('z', 23) AS FixedString(16, 'Base58')); -- { serverError INCORRECT_DATA }
SELECT CAST(base58Encode(repeat('\xFF', 17)) AS FixedString(16, 'Base58')); -- { serverError INCORRECT_DATA }
SELECT CAST(base58Encode(repeat('\xFF', 15)) AS FixedString(16, 'Base58')); -- { serverError INCORRECT_DATA }
SELECT CAST(repeat('z', 10000) AS FixedString(32, 'Base58')); -- { serverError INCORRECT_DATA }
SELECT CAST(repeat('z', 10000) AS FixedString(64, 'Base58')); -- { serverError INCORRECT_DATA }
SELECT CAST('' AS FixedString(32, 'Base58')); -- { serverError INCORRECT_DATA }

SELECT '-- Hex';
SELECT CAST('0x0aFF' AS FixedString(2, 'Hex')), CAST('0X0AFF' AS FixedString(2, 'Hex')), hex(CAST('0aff' AS FixedString(2, 'Hex')));
SELECT CAST('0g' AS FixedString(1, 'Hex')); -- { serverError INCORRECT_DATA }
SELECT CAST('0a0' AS FixedString(2, 'Hex')); -- { serverError INCORRECT_DATA }
SELECT CAST('0x' AS FixedString(1, 'Hex')); -- { serverError INCORRECT_DATA }
SELECT toString(x) FROM (SELECT arrayJoin(['0x01', '0x00', '0xff', '0x10']) AS s, CAST(s AS FixedString(1, 'Hex')) AS x) WHERE toString(x) > '0f' ORDER BY x;

SELECT '-- Base64 and Base64URL';
SELECT CAST('/w==' AS FixedString(1, 'Base64')), CAST('_w' AS FixedString(1, 'Base64URL')), CAST('_w==' AS FixedString(1, 'Base64URL')), CAST('/w==' AS FixedString(1, 'Base64URL'));
SELECT hex(CAST('AQID' AS FixedString(3, 'Base64'))), hex(CAST('AQID' AS FixedString(3, 'Base64URL')));
SELECT CAST('/w' AS FixedString(1, 'Base64')); -- { serverError INCORRECT_DATA }
SELECT CAST('AQID' AS FixedString(2, 'Base64')); -- { serverError INCORRECT_DATA }
SELECT CAST('AQID' AS FixedString(4, 'Base64')); -- { serverError INCORRECT_DATA }
SELECT CAST('!!!!' AS FixedString(3, 'Base64')); -- { serverError INCORRECT_DATA }
SELECT CAST(base64Encode(repeat('x', 10000)) AS FixedString(3, 'Base64')); -- { serverError INCORRECT_DATA }

SELECT '-- Raw is the default';
SELECT CAST('ab' AS FixedString(3, 'Raw')) = CAST('ab' AS FixedString(3)), toTypeName([CAST('ab' AS FixedString(3, 'Raw')), CAST('ab' AS FixedString(3))]);
