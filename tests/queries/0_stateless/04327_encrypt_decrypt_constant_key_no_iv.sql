-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- Regression test for the constant-key fast path in encrypt/decrypt.
-- When the key is constant the cipher context is initialized once and only the IV is reset
-- per row. With an absent or empty IV the per-row reset must restore the initial (zero) IV
-- state, otherwise rows after the first are computed from the previous row's advanced IV.
-- The constant-key fast path must produce exactly the same result as the per-row path,
-- which is forced by materializing the key into a non-constant column. Every comparison
-- below must therefore be 1, independently of the data, for all stateful (non-ECB) modes.
-- https://github.com/ClickHouse/ClickHouse/pull/99105

-- The input has a repeated row so that a leak of IV state across rows would change the
-- second occurrence and break the equality.

WITH unhex('00112233445566778899aabbccddeeff') AS key
SELECT
    'aes-128-cbc' AS mode,
    hex(encrypt('aes-128-cbc', p, key)) = hex(encrypt('aes-128-cbc', p, materialize(key))) AS encrypt_no_iv_matches,
    hex(encrypt('aes-128-cbc', p, key, '')) = hex(encrypt('aes-128-cbc', p, materialize(key), '')) AS encrypt_empty_iv_matches,
    decrypt('aes-128-cbc', encrypt('aes-128-cbc', p, key), key) = p AS roundtrip_no_iv_ok
FROM (SELECT arrayJoin(['the quick brown!', 'the quick brown!', 'lazy dog jumped!']) AS p)
ORDER BY p;

WITH unhex('00112233445566778899aabbccddeeff') AS key
SELECT
    'aes-128-ctr' AS mode,
    hex(encrypt('aes-128-ctr', p, key)) = hex(encrypt('aes-128-ctr', p, materialize(key))) AS encrypt_no_iv_matches,
    hex(encrypt('aes-128-ctr', p, key, '')) = hex(encrypt('aes-128-ctr', p, materialize(key), '')) AS encrypt_empty_iv_matches,
    decrypt('aes-128-ctr', encrypt('aes-128-ctr', p, key), key) = p AS roundtrip_no_iv_ok
FROM (SELECT arrayJoin(['the quick brown!', 'the quick brown!', 'lazy dog jumped!']) AS p)
ORDER BY p;

WITH unhex('00112233445566778899aabbccddeeff') AS key
SELECT
    'aes-128-ofb' AS mode,
    hex(encrypt('aes-128-ofb', p, key)) = hex(encrypt('aes-128-ofb', p, materialize(key))) AS encrypt_no_iv_matches,
    hex(encrypt('aes-128-ofb', p, key, '')) = hex(encrypt('aes-128-ofb', p, materialize(key), '')) AS encrypt_empty_iv_matches,
    decrypt('aes-128-ofb', encrypt('aes-128-ofb', p, key), key) = p AS roundtrip_no_iv_ok
FROM (SELECT arrayJoin(['the quick brown!', 'the quick brown!', 'lazy dog jumped!']) AS p)
ORDER BY p;
