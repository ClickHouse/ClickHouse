-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- Encrypting/decrypting with a constant key (and absent or empty IV) must produce exactly
-- the same result as with a non-constant key, for every row. This guards against any
-- constant-key specialization diverging from the generic path -- e.g. by reusing a single
-- cipher context across rows and leaking the IV state of the previous row into the next.
-- The per-row (correct) path is forced by materializing the key into a non-constant column,
-- so every comparison below must be 1, independently of the data, for all stateful modes.
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
