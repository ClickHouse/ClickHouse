-- Tags: no-fasttest, no-openssl-fips

SET output_format_pretty_single_large_number_tip_threshold = 0;
SET enable_analyzer = 1;

SELECT
    toIPv4('1.2.3.4') AS ipv4,
    halfMD5(ipv4),
    farmFingerprint64(ipv4),
    xxh3(ipv4),
    wyHash64(ipv4),
    xxHash32(ipv4),
    gccMurmurHash(ipv4),
    murmurHash2_32(ipv4),
    javaHashUTF16LE(ipv4),
    intHash64(ipv4),
    intHash32(ipv4),
    metroHash64(ipv4),
    hex(murmurHash3_128(ipv4)),
    jumpConsistentHash(ipv4, 42),
    sipHash64(ipv4),
    hex(sipHash128(ipv4)),
    kostikConsistentHash(ipv4, 42),
    xxHash64(ipv4),
    murmurHash2_64(ipv4),
    cityHash64(ipv4),
    hiveHash(ipv4),
    murmurHash3_64(ipv4),
    murmurHash3_32(ipv4),
    yandexConsistentHash(ipv4,42)
FORMAT Vertical;

SELECT
    toIPv6('fe80::62:5aff:fed1:daf0') AS ipv6,
    halfMD5(ipv6),
    hex(MD4(ipv6)),
    hex(MD5(ipv6)),
    hex(SHA1(ipv6)),
    hex(SHA224(ipv6)),
    hex(SHA256(ipv6)),
    hex(SHA512(ipv6)),
    hex(SHA512_256(ipv6)),
    farmFingerprint64(ipv6),
    javaHash(ipv6),
    xxh3(ipv6),
    wyHash64(ipv6),
    xxHash32(ipv6),
    gccMurmurHash(ipv6),
    murmurHash2_32(ipv6),
    javaHashUTF16LE(ipv6),
    metroHash64(ipv6),
    hex(sipHash128(ipv6)),
    hex(murmurHash3_128(ipv6)),
    sipHash64(ipv6),
    xxHash64(ipv6),
    murmurHash2_64(ipv6),
    cityHash64(ipv6),
    hiveHash(ipv6),
    murmurHash3_64(ipv6),
    murmurHash3_32(ipv6)
FORMAT Vertical;
