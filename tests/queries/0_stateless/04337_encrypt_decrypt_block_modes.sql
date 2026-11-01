-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- Known-answer and round-trip tests for the block-composed fast path of plain AES
-- ECB/CBC modes in `encrypt`/`decrypt`/`tryDecrypt`/`aes_encrypt_mysql`/`aes_decrypt_mysql`
-- (a streaming ECB context with manual PKCS#7 padding and CBC chaining XOR).
-- The reference output was produced by the generic per-row EVP implementation,
-- so any divergence of the fast path from OpenSSL semantics fails this test.
-- Input lengths cross block boundaries and the threshold at which CBC encryption
-- switches to the stitched implementation.
-- https://github.com/ClickHouse/ClickHouse/issues/65116

WITH
    arrayJoin([0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 256, 1000]) AS len,
    repeat('x', len) AS plain,
    'key-key-key-key+' AS key16,
    'key-key-key-key+key-key+' AS key24,
    'key-key-key-key+key-key-key-key+' AS key32,
    'iviviviviviviviv' AS iv16
SELECT
    len,
    hex(encrypt('aes-128-ecb', plain, key16)) AS e128ecb,
    hex(encrypt('aes-256-ecb', plain, key32)) AS e256ecb,
    hex(encrypt('aes-128-cbc', plain, key16)) AS e128cbc_noiv,
    hex(encrypt('aes-128-cbc', plain, key16, iv16)) AS e128cbc_iv,
    hex(encrypt('aes-192-cbc', plain, key24, iv16)) AS e192cbc_iv,
    hex(encrypt('aes-256-cbc', plain, key32, iv16)) AS e256cbc_iv,
    hex(encrypt('aes-128-cbc', plain, key16, '')) AS e128cbc_emptyiv,
    hex(aes_encrypt_mysql('aes-128-ecb', plain, key16)) AS m128ecb,
    hex(aes_encrypt_mysql('aes-128-cbc', plain, key16, iv16)) AS m128cbc,
    hex(aes_encrypt_mysql('aes-256-cbc', plain, repeat(key32, 2), repeat(iv16, 2))) AS m256cbc_longkey
ORDER BY len;

-- Keys and IVs varying per row and alternating keys exercise the key schedule cache
-- transitions; results must not depend on the previous row's key or IV state.
WITH
    arrayJoin(range(20)) AS n,
    leftPad(toString(n), 3, '0') AS n3,
    concat('key-key-key-k', n3) AS vkey,
    concat('iviviviviviiv', n3) AS viv,
    concat('key-key-key-k', leftPad(toString(n % 2), 3, '0')) AS akey,
    repeat('p', 1 + (n * 7) % 40) AS plain
SELECT
    n,
    hex(encrypt('aes-128-cbc', plain, vkey, viv)) AS vkey_viv,
    hex(encrypt('aes-128-cbc', plain, akey, viv)) AS akey_viv,
    hex(encrypt('aes-128-ecb', plain, vkey)) AS vkey_ecb,
    hex(encrypt('aes-128-cbc', plain, 'key-key-key-key+', viv)) AS ckey_viv,
    hex(encrypt('aes-128-cbc', plain, vkey, 'iviviviviviviviv')) AS vkey_civ
ORDER BY n;

-- CTR and OFB are streaming modes that do not use the ECB/CBC fast path, but they
-- share the same per-row key-schedule cache: a key match resets only the IV, a key
-- change re-expands the schedule. Alternating keys force a cache miss every row, a
-- constant key with a varying IV exercises the IV-only reset, and zero-length rows
-- exercise the empty-input skip path that must not corrupt the cipher state. Output
-- must depend only on the current row's key and IV, never on the previous row's state.
WITH
    arrayJoin(range(20)) AS n,
    leftPad(toString(n), 3, '0') AS n3,
    concat('key-key-key-k', n3) AS vkey,
    concat('iviviviviviiv', n3) AS viv,
    concat('key-key-key-k', leftPad(toString(n % 2), 3, '0')) AS akey,
    repeat('s', (n * 7) % 40) AS plain
SELECT
    n,
    hex(encrypt('aes-128-ctr', plain, vkey, viv)) AS ctr_vkey_viv,
    hex(encrypt('aes-128-ctr', plain, akey, viv)) AS ctr_akey_viv,
    hex(encrypt('aes-128-ctr', plain, 'key-key-key-key+', viv)) AS ctr_ckey_viv,
    hex(encrypt('aes-128-ctr', plain, vkey, 'iviviviviviviviv')) AS ctr_vkey_civ,
    hex(encrypt('aes-128-ofb', plain, vkey, viv)) AS ofb_vkey_viv,
    hex(encrypt('aes-128-ofb', plain, akey, viv)) AS ofb_akey_viv,
    hex(encrypt('aes-128-ofb', plain, 'key-key-key-key+', viv)) AS ofb_ckey_viv,
    hex(encrypt('aes-128-ofb', plain, vkey, 'iviviviviviviviv')) AS ofb_vkey_civ
ORDER BY n;

-- Round trips through the same cache transitions must recover the plaintext for the
-- streaming modes regardless of how the key and IV change between rows, both with an
-- explicit IV and with no IV.
WITH
    arrayJoin(range(20)) AS n,
    leftPad(toString(n), 3, '0') AS n3,
    concat('key-key-key-k', leftPad(toString(n % 2), 3, '0')) AS akey,
    concat('iviviviviviiv', n3) AS viv,
    repeat('w', (n * 11) % 50) AS plain
SELECT
    n,
    decrypt('aes-128-ctr', encrypt('aes-128-ctr', plain, akey, viv), akey, viv) = plain AS ctr_rt,
    decrypt('aes-128-ofb', encrypt('aes-128-ofb', plain, akey, viv), akey, viv) = plain AS ofb_rt,
    decrypt('aes-128-ctr', encrypt('aes-128-ctr', plain, akey), akey) = plain AS ctr_noiv_rt,
    decrypt('aes-128-ofb', encrypt('aes-128-ofb', plain, akey), akey) = plain AS ofb_noiv_rt
ORDER BY n;

-- Round trips must recover the plaintext.
WITH
    arrayJoin([0, 1, 15, 16, 17, 63, 64, 65, 256]) AS len,
    repeat('y', len) AS plain,
    'key-key-key-key+' AS key16,
    'key-key-key-key+key-key-key-key+' AS key32,
    'iviviviviviviviv' AS iv16
SELECT
    len,
    decrypt('aes-128-ecb', encrypt('aes-128-ecb', plain, key16), key16) = plain,
    decrypt('aes-128-cbc', encrypt('aes-128-cbc', plain, key16), key16) = plain,
    decrypt('aes-256-cbc', encrypt('aes-256-cbc', plain, key32, iv16), key32, iv16) = plain,
    decrypt('aes-128-cbc', encrypt('aes-128-cbc', plain, key16, ''), key16, '') = plain,
    aes_decrypt_mysql('aes-128-cbc', aes_encrypt_mysql('aes-128-cbc', plain, repeat(key16, 3), repeat(iv16, 2)), repeat(key16, 3), repeat(iv16, 2)) = plain
ORDER BY len;

-- Corrupted, truncated, extended, and wrong-key ciphertexts must decrypt to NULL via
-- `tryDecrypt`, and PKCS#7 padding must be validated in full.
WITH
    arrayJoin(range(12)) AS n,
    'key-key-key-key+' AS key16,
    encrypt('aes-128-cbc', repeat('z', n * 3), key16) AS good
SELECT
    n,
    tryDecrypt('aes-128-cbc', good, key16) IS NULL AS good_is_null,
    tryDecrypt('aes-128-cbc', substring(good, 1, length(good) - 1), key16) IS NULL AS truncated_is_null,
    tryDecrypt('aes-128-cbc', concat(good, 'q'), key16) IS NULL AS extended_is_null,
    tryDecrypt('aes-128-cbc', good, 'wrong-key-wrong-') IS NULL AS wrongkey_is_null,
    tryDecrypt('aes-128-ecb', unhex('00000000000000000000000000000000'), key16) IS NULL AS zeros_is_null,
    tryDecrypt('aes-128-cbc', '', key16) AS empty_result
ORDER BY n;

-- Failed rows must not corrupt the cipher state of subsequent rows.
SELECT n, isNull(tryDecrypt('aes-128-cbc',
    if(n % 2 = 0, encrypt('aes-128-cbc', toString(n), 'key-key-key-key+'), 'badc'),
    'key-key-key-key+')) AS is_null
FROM (SELECT arrayJoin(range(8)) AS n)
ORDER BY n;

-- Mixed-length rows, including empty strings between non-empty ones.
SELECT hex(encrypt('aes-128-cbc', arrayJoin(['', 'a', '', repeat('b', 100), 'c', '']), 'key-key-key-key+')) AS mixed;

-- Decryption errors keep the OPENSSL_ERROR code.
SELECT decrypt('aes-128-cbc', 'hello there', 'key-key-key-key+'); -- {serverError OPENSSL_ERROR} not a multiple of block size
SELECT decrypt('aes-128-cbc', 'sixteen bytes ab', 'key-key-key-key+'); -- {serverError OPENSSL_ERROR} invalid padding
