-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- FixedString, constant and non-constant arguments of encrypt/decrypt must give the same result
-- as String arguments with the same bytes (every count below equals the row count).

SELECT 'A', 4, count(),
    countIf(encrypt('aes-128-ecb', fp, fk) = encrypt('aes-128-ecb', p, k)),
    countIf(encrypt('aes-128-ecb', fp, toFixedString('key-key-key-key+', 16)) = encrypt('aes-128-ecb', p, 'key-key-key-key+')),
    countIf(encrypt('aes-256-cbc', fp, fk32, fiv) = encrypt('aes-256-cbc', p, k32, iv)),
    countIf(encrypt('aes-256-cbc', p, toFixedString(repeat('key-key-', 4), 32), toFixedString('iviviviviviviviv', 16)) = encrypt('aes-256-cbc', p, repeat('key-key-', 4), 'iviviviviviviviv')),
    countIf(encrypt('aes-128-ctr', fp, fk, toFixedString('iviviviviviviviv', 16)) = encrypt('aes-128-ctr', p, k, 'iviviviviviviviv')),
    countIf(encrypt('aes-128-ofb', fp, toFixedString('key-key-key-key+', 16), fiv) = encrypt('aes-128-ofb', p, 'key-key-key-key+', iv)),
    countIf(encrypt('aes-128-gcm', fp, fk, fiv12) = encrypt('aes-128-gcm', p, k, iv12)),
    countIf(encrypt('aes-128-gcm', fp, fk, fiv12, faad) = encrypt('aes-128-gcm', p, k, iv12, aad)),
    countIf(encrypt('aes-128-gcm', p, k, iv12, toFixedString('aadaad', 6)) = encrypt('aes-128-gcm', p, k, iv12, 'aadaad')),
    countIf(decrypt('aes-128-ecb', toFixedString(encrypt('aes-128-ecb', p, k), 16), fk) = p),
    countIf(decrypt('aes-256-cbc', toFixedString(encrypt('aes-256-cbc', p, k32, iv), 16), fk32, fiv) = p),
    countIf(decrypt('aes-128-ctr', toFixedString(encrypt('aes-128-ctr', p, k, iv), 4), fk, fiv) = p),
    countIf(decrypt('aes-128-gcm', toFixedString(encrypt('aes-128-gcm', p, k, iv12, aad), 20), fk, fiv12, faad) = p),
    countIf(aes_encrypt_mysql('aes-128-cbc', fp, fk24, fiv) = aes_encrypt_mysql('aes-128-cbc', p, k24, iv)),
    countIf(aes_decrypt_mysql('aes-128-cbc', toFixedString(aes_encrypt_mysql('aes-128-cbc', p, k24, iv), 16), fk24, fiv) = p),
    countIf(isNull(tryDecrypt('aes-128-cbc', toFixedString(encrypt('aes-128-cbc', p, k, iv), 16), toFixedString(reverse(k), 16), fiv))
        = isNull(tryDecrypt('aes-128-cbc', encrypt('aes-128-cbc', p, k, iv), reverse(k), iv))),
    countIf(isNull(tryDecrypt('aes-128-cbc', toFixedString(encrypt('aes-128-cbc', p, k, iv), 16), toFixedString(reverse(k), 16), fiv)))
FROM
(
    SELECT
        substring(concat(toString(number), repeat('p', 40)), 1, 4) AS p, toFixedString(p, 4) AS fp,
        substring(concat(toString(number % 7), repeat('k', 16)), 1, 16) AS k, toFixedString(k, 16) AS fk,
        repeat(k, 2) AS k32, toFixedString(k32, 32) AS fk32,
        substring(k32, 1, 24) AS k24, toFixedString(k24, 24) AS fk24,
        substring(concat(toString(number % 5), repeat('v', 16)), 1, 16) AS iv, toFixedString(iv, 16) AS fiv,
        substring(iv, 1, 12) AS iv12, toFixedString(iv12, 12) AS fiv12,
        substring(concat(toString(number % 3), 'aadaad'), 1, 6) AS aad, toFixedString(aad, 6) AS faad
    FROM numbers(200)
);

SELECT 'A', 17, count(),
    countIf(encrypt('aes-128-ecb', fp, fk) = encrypt('aes-128-ecb', p, k)),
    countIf(encrypt('aes-128-ecb', fp, toFixedString('key-key-key-key+', 16)) = encrypt('aes-128-ecb', p, 'key-key-key-key+')),
    countIf(encrypt('aes-256-cbc', fp, fk32, fiv) = encrypt('aes-256-cbc', p, k32, iv)),
    countIf(encrypt('aes-256-cbc', p, toFixedString(repeat('key-key-', 4), 32), toFixedString('iviviviviviviviv', 16)) = encrypt('aes-256-cbc', p, repeat('key-key-', 4), 'iviviviviviviviv')),
    countIf(encrypt('aes-128-ctr', fp, fk, toFixedString('iviviviviviviviv', 16)) = encrypt('aes-128-ctr', p, k, 'iviviviviviviviv')),
    countIf(encrypt('aes-128-ofb', fp, toFixedString('key-key-key-key+', 16), fiv) = encrypt('aes-128-ofb', p, 'key-key-key-key+', iv)),
    countIf(encrypt('aes-128-gcm', fp, fk, fiv12) = encrypt('aes-128-gcm', p, k, iv12)),
    countIf(encrypt('aes-128-gcm', fp, fk, fiv12, faad) = encrypt('aes-128-gcm', p, k, iv12, aad)),
    countIf(encrypt('aes-128-gcm', p, k, iv12, toFixedString('aadaad', 6)) = encrypt('aes-128-gcm', p, k, iv12, 'aadaad')),
    countIf(decrypt('aes-128-ecb', toFixedString(encrypt('aes-128-ecb', p, k), 32), fk) = p),
    countIf(decrypt('aes-256-cbc', toFixedString(encrypt('aes-256-cbc', p, k32, iv), 32), fk32, fiv) = p),
    countIf(decrypt('aes-128-ctr', toFixedString(encrypt('aes-128-ctr', p, k, iv), 17), fk, fiv) = p),
    countIf(decrypt('aes-128-gcm', toFixedString(encrypt('aes-128-gcm', p, k, iv12, aad), 33), fk, fiv12, faad) = p),
    countIf(aes_encrypt_mysql('aes-128-cbc', fp, fk24, fiv) = aes_encrypt_mysql('aes-128-cbc', p, k24, iv)),
    countIf(aes_decrypt_mysql('aes-128-cbc', toFixedString(aes_encrypt_mysql('aes-128-cbc', p, k24, iv), 32), fk24, fiv) = p),
    countIf(isNull(tryDecrypt('aes-128-cbc', toFixedString(encrypt('aes-128-cbc', p, k, iv), 32), toFixedString(reverse(k), 16), fiv))
        = isNull(tryDecrypt('aes-128-cbc', encrypt('aes-128-cbc', p, k, iv), reverse(k), iv))),
    countIf(isNull(tryDecrypt('aes-128-cbc', toFixedString(encrypt('aes-128-cbc', p, k, iv), 32), toFixedString(reverse(k), 16), fiv)))
FROM
(
    SELECT
        substring(concat(toString(number), repeat('p', 40)), 1, 17) AS p, toFixedString(p, 17) AS fp,
        substring(concat(toString(number % 7), repeat('k', 16)), 1, 16) AS k, toFixedString(k, 16) AS fk,
        repeat(k, 2) AS k32, toFixedString(k32, 32) AS fk32,
        substring(k32, 1, 24) AS k24, toFixedString(k24, 24) AS fk24,
        substring(concat(toString(number % 5), repeat('v', 16)), 1, 16) AS iv, toFixedString(iv, 16) AS fiv,
        substring(iv, 1, 12) AS iv12, toFixedString(iv12, 12) AS fiv12,
        substring(concat(toString(number % 3), 'aadaad'), 1, 6) AS aad, toFixedString(aad, 6) AS faad
    FROM numbers(200)
);

SELECT 'A', 33, count(),
    countIf(encrypt('aes-128-ecb', fp, fk) = encrypt('aes-128-ecb', p, k)),
    countIf(encrypt('aes-128-ecb', fp, toFixedString('key-key-key-key+', 16)) = encrypt('aes-128-ecb', p, 'key-key-key-key+')),
    countIf(encrypt('aes-256-cbc', fp, fk32, fiv) = encrypt('aes-256-cbc', p, k32, iv)),
    countIf(encrypt('aes-256-cbc', p, toFixedString(repeat('key-key-', 4), 32), toFixedString('iviviviviviviviv', 16)) = encrypt('aes-256-cbc', p, repeat('key-key-', 4), 'iviviviviviviviv')),
    countIf(encrypt('aes-128-ctr', fp, fk, toFixedString('iviviviviviviviv', 16)) = encrypt('aes-128-ctr', p, k, 'iviviviviviviviv')),
    countIf(encrypt('aes-128-ofb', fp, toFixedString('key-key-key-key+', 16), fiv) = encrypt('aes-128-ofb', p, 'key-key-key-key+', iv)),
    countIf(encrypt('aes-128-gcm', fp, fk, fiv12) = encrypt('aes-128-gcm', p, k, iv12)),
    countIf(encrypt('aes-128-gcm', fp, fk, fiv12, faad) = encrypt('aes-128-gcm', p, k, iv12, aad)),
    countIf(encrypt('aes-128-gcm', p, k, iv12, toFixedString('aadaad', 6)) = encrypt('aes-128-gcm', p, k, iv12, 'aadaad')),
    countIf(decrypt('aes-128-ecb', toFixedString(encrypt('aes-128-ecb', p, k), 48), fk) = p),
    countIf(decrypt('aes-256-cbc', toFixedString(encrypt('aes-256-cbc', p, k32, iv), 48), fk32, fiv) = p),
    countIf(decrypt('aes-128-ctr', toFixedString(encrypt('aes-128-ctr', p, k, iv), 33), fk, fiv) = p),
    countIf(decrypt('aes-128-gcm', toFixedString(encrypt('aes-128-gcm', p, k, iv12, aad), 49), fk, fiv12, faad) = p),
    countIf(aes_encrypt_mysql('aes-128-cbc', fp, fk24, fiv) = aes_encrypt_mysql('aes-128-cbc', p, k24, iv)),
    countIf(aes_decrypt_mysql('aes-128-cbc', toFixedString(aes_encrypt_mysql('aes-128-cbc', p, k24, iv), 48), fk24, fiv) = p),
    countIf(isNull(tryDecrypt('aes-128-cbc', toFixedString(encrypt('aes-128-cbc', p, k, iv), 48), toFixedString(reverse(k), 16), fiv))
        = isNull(tryDecrypt('aes-128-cbc', encrypt('aes-128-cbc', p, k, iv), reverse(k), iv))),
    countIf(isNull(tryDecrypt('aes-128-cbc', toFixedString(encrypt('aes-128-cbc', p, k, iv), 48), toFixedString(reverse(k), 16), fiv)))
FROM
(
    SELECT
        substring(concat(toString(number), repeat('p', 40)), 1, 33) AS p, toFixedString(p, 33) AS fp,
        substring(concat(toString(number % 7), repeat('k', 16)), 1, 16) AS k, toFixedString(k, 16) AS fk,
        repeat(k, 2) AS k32, toFixedString(k32, 32) AS fk32,
        substring(k32, 1, 24) AS k24, toFixedString(k24, 24) AS fk24,
        substring(concat(toString(number % 5), repeat('v', 16)), 1, 16) AS iv, toFixedString(iv, 16) AS fiv,
        substring(iv, 1, 12) AS iv12, toFixedString(iv12, 12) AS fiv12,
        substring(concat(toString(number % 3), 'aadaad'), 1, 6) AS aad, toFixedString(aad, 6) AS faad
    FROM numbers(200)
);

-- Many rows per block, mixing rows shorter than one AES block with longer ones, key changes and
-- per-row IVs. The hashes were produced by the implementation that encrypts every row separately.

SELECT 'B1', count(), sum(cityHash64(c)), countIf(decrypt('aes-128-ecb', c, 'key-key-key-key+') = p)
FROM (SELECT toString(number) AS p, encrypt('aes-128-ecb', p, 'key-key-key-key+') AS c FROM numbers(5000));

SELECT 'B2', count(), sum(cityHash64(c)), countIf(decrypt('aes-128-cbc', c, 'key-key-key-key+', 'iviviviviviviviv') = p)
FROM (SELECT toString(number) AS p, encrypt('aes-128-cbc', p, 'key-key-key-key+', 'iviviviviviviviv') AS c FROM numbers(5000));

SELECT 'B3', count(), sum(cityHash64(c)), countIf(decrypt('aes-256-cbc', c, repeat('key-key-', 4), iv) = p)
FROM
(
    SELECT toString(number) AS p, substring(concat(toString(number), 'iviviviviviviviv'), 1, 16) AS iv,
        encrypt('aes-256-cbc', p, repeat('key-key-', 4), iv) AS c
    FROM numbers(5000)
);

SELECT 'B4', count(), sum(cityHash64(c)), countIf(decrypt('aes-128-cbc', c, 'key-key-key-key+') = p)
FROM (SELECT toString(number) AS p, encrypt('aes-128-cbc', p, 'key-key-key-key+') AS c FROM numbers(5000));

SELECT 'B5', count(), sum(cityHash64(c)), countIf(decrypt('aes-128-ecb', c, 'key-key-key-key+') = p)
FROM (SELECT repeat('x', number % 70) AS p, encrypt('aes-128-ecb', p, 'key-key-key-key+') AS c FROM numbers(5000));

SELECT 'B6', count(), sum(cityHash64(c)), countIf(decrypt('aes-128-cbc', c, 'key-key-key-key+', 'iviviviviviviviv') = p)
FROM (SELECT repeat('x', number % 70) AS p, encrypt('aes-128-cbc', p, 'key-key-key-key+', 'iviviviviviviviv') AS c FROM numbers(5000));

SELECT 'B7', count(), sum(cityHash64(c)), countIf(decrypt('aes-128-cbc', c, k, 'iviviviviviviviv') = p)
FROM
(
    SELECT repeat('y', number % 20) AS p, if(intDiv(number, 37) % 2 = 0, 'key-key-key-key1', 'key-key-key-key2') AS k,
        encrypt('aes-128-cbc', p, k, 'iviviviviviviviv') AS c
    FROM numbers(5000)
);

SELECT 'B8', count(), sum(cityHash64(c)), countIf(aes_decrypt_mysql('aes-128-cbc', c, repeat('key-key-', 4), 'iviviviviviviviv') = p)
FROM (SELECT toString(number) AS p, aes_encrypt_mysql('aes-128-cbc', p, repeat('key-key-', 4), 'iviviviviviviviv') AS c FROM numbers(5000));

SELECT 'B9', count(), sum(cityHash64(c)), countIf(aes_decrypt_mysql('aes-128-ecb', c, k) = p)
FROM
(
    SELECT repeat('z', number % 33) AS p, substring(concat(toString(intDiv(number, 3)), repeat('q', 24)), 1, 24) AS k,
        aes_encrypt_mysql('aes-128-ecb', p, k) AS c
    FROM numbers(5000)
);
