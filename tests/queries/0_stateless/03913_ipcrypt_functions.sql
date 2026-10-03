-- Tags: no-fasttest
SET allow_experimental_ipcrypt_functions = 1;

-- Deterministic encrypt/decrypt round-trip with IPv4
SELECT 'det_roundtrip_ipv4';
SELECT ipcryptDecrypt(ipcryptEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'));
SELECT ipcryptDecrypt(ipcryptEncrypt(toIPv4('10.0.0.1'), unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'));

-- Deterministic encrypt/decrypt round-trip with IPv6
SELECT 'det_roundtrip_ipv6';
SELECT ipcryptDecrypt(ipcryptEncrypt(toIPv6('2001:db8::1'), unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'));
SELECT ipcryptDecrypt(ipcryptEncrypt(toIPv6('::1'), unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'));

-- Prefix-preserving encrypt/decrypt round-trip with IPv4 (class-preserving: IPv4 -> IPv4)
SELECT 'prefix_roundtrip_ipv4';
SELECT ipcryptPrefixDecrypt(ipcryptPrefixEncrypt(toIPv4('10.0.0.1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));
SELECT ipcryptPrefixDecrypt(ipcryptPrefixEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- Prefix-preserving encrypt/decrypt round-trip with IPv6 (class-preserving: IPv6 -> IPv6)
SELECT 'prefix_roundtrip_ipv6';
SELECT ipcryptPrefixDecrypt(ipcryptPrefixEncrypt(toIPv6('2001:db8::1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- Determinism: same input + key always produces same output
SELECT 'determinism';
SELECT ipcryptEncrypt(toIPv4('10.0.0.1'), unhex('00112233445566778899aabbccddeeff')) = ipcryptEncrypt(toIPv4('10.0.0.1'), unhex('00112233445566778899aabbccddeeff'));

-- Known-answer vectors (deterministic mode)
SELECT 'det_known_answer';
SELECT ipcryptEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeff'));
SELECT ipcryptEncrypt(toIPv6('2001:db8::1'), unhex('00112233445566778899aabbccddeeff'));

-- Known-answer vectors (prefix-preserving mode, typed IPv4 — locks byte-mapping correctness)
SELECT 'prefix_known_answer_ipv4';
SELECT ipcryptPrefixEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- Known-answer vectors (prefix-preserving mode, typed IPv6)
SELECT 'prefix_known_answer_ipv6';
SELECT ipcryptPrefixEncrypt(toIPv6('2001:db8::1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- Type coverage: String input (deterministic)
SELECT 'string_ipv4';
SELECT ipcryptDecrypt(ipcryptEncrypt('192.168.1.1', unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'));

SELECT 'string_ipv6';
SELECT ipcryptDecrypt(ipcryptEncrypt('2001:db8::1', unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'));

-- Prefix-preserving String input class preservation (IPv4 string -> IPv4 string)
SELECT 'prefix_string_ipv4';
SELECT ipcryptPrefixDecrypt(ipcryptPrefixEncrypt('192.168.1.1', unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- Prefix-preserving String input class preservation (IPv6 string -> IPv6 string)
SELECT 'prefix_string_ipv6';
SELECT ipcryptPrefixDecrypt(ipcryptPrefixEncrypt('2001:db8::1', unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- Hex key format (32 hex chars for deterministic, 64 for prefix-preserving)
SELECT 'hex_key_det';
SELECT ipcryptEncrypt(toIPv4('192.168.1.1'), '00112233445566778899aabbccddeeff');

SELECT 'hex_key_prefix';
SELECT ipcryptPrefixEncrypt(toIPv4('192.168.1.1'), '00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100');

-- Return types: deterministic returns IPv6 for typed, String for String; prefix-preserving preserves class
SELECT 'return_type';
SELECT toTypeName(ipcryptEncrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeff')));
SELECT toTypeName(ipcryptDecrypt(toIPv6('::1'), unhex('00112233445566778899aabbccddeeff')));
SELECT toTypeName(ipcryptEncrypt('1.2.3.4', unhex('00112233445566778899aabbccddeeff')));
SELECT toTypeName(ipcryptDecrypt('::1', unhex('00112233445566778899aabbccddeeff')));
SELECT toTypeName(ipcryptPrefixEncrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));
SELECT toTypeName(ipcryptPrefixDecrypt(toIPv6('::1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));
SELECT toTypeName(ipcryptPrefixEncrypt(toIPv6('2001:db8::1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));
SELECT toTypeName(ipcryptPrefixDecrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));
SELECT toTypeName(ipcryptPrefixEncrypt('1.2.3.4', unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));
SELECT toTypeName(ipcryptPrefixDecrypt('::1', unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));

-- Prefix-preserving determinism
SELECT 'prefix_determinism';
SELECT ipcryptPrefixEncrypt(toIPv4('10.0.0.1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'))
     = ipcryptPrefixEncrypt(toIPv4('10.0.0.1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- Nullable pass-through (non-null and explicit NULL)
SELECT 'nullable';
SELECT toTypeName(ipcryptPrefixEncrypt(toNullable(toIPv4('1.2.3.4')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));
SELECT ipcryptPrefixDecrypt(ipcryptPrefixEncrypt(toNullable(toIPv4('10.0.0.1')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));
SELECT ipcryptPrefixEncrypt(CAST(NULL AS Nullable(IPv4)), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));

-- String-path IPv4-mapped IPv6 literal (::ffff:x.x.x.x treated as IPv4 class in prefix-preserving mode)
SELECT 'string_ipv4_mapped';
SELECT ipcryptPrefixDecrypt(ipcryptPrefixEncrypt('::ffff:192.168.1.1', unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'));
SELECT toTypeName(ipcryptPrefixEncrypt('::ffff:192.168.1.1', unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')));

-- Known-answer vector for deterministic String input (pins serialization format)
SELECT 'det_string_known_answer';
SELECT ipcryptEncrypt('192.168.1.1', unhex('00112233445566778899aabbccddeeff'));

-- Nullable String pass-through for deterministic mode
SELECT 'det_nullable_string';
SELECT toTypeName(ipcryptEncrypt(toNullable('1.2.3.4'), unhex('00112233445566778899aabbccddeeff')));
SELECT ipcryptDecrypt(ipcryptEncrypt(toNullable('10.0.0.1'), unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'));
SELECT ipcryptEncrypt(CAST(NULL AS Nullable(String)), unhex('00112233445566778899aabbccddeeff'));
