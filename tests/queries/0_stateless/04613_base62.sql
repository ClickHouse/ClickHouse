SELECT base62Encode('Hold my beer...');

SELECT base62Encode('Hold my beer...', 'Second arg'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT base62Decode('Hold my beer...'); -- { serverError INCORRECT_DATA }

SELECT base62Decode(encoded) FROM (SELECT base62Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase62Decode(encoded) FROM (SELECT base62Encode(val) as encoded FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar', 'Hello world!']) val));
SELECT tryBase62Decode(val) FROM (SELECT arrayJoin(['Hold my beer', 'Hold another beer', '1e', 'And a wine', 'SAPP', 'And a lemonade', 'VytN8Wjy', 'And another wine']) val);

SELECT base62Encode(val) FROM (SELECT arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);
SELECT base62Decode(val) FROM (SELECT arrayJoin(['', '1e', '6ox', 'SAPP', '1sIyuo', '7kENWa1', 'VytN8Wjy', '']) val);

SELECT base62Encode(base62Decode('1agk8B30gH5Kj7')) == '1agk8B30gH5Kj7';
SELECT base62Encode('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65') == '1agk8B30gH5Kj7';

SELECT base62Encode(toFixedString('Hold my beer...', 15));
SELECT base62Decode(toFixedString('T8dgcjRGuYUueWht', 16));

SELECT base62Encode(val) FROM (SELECT arrayJoin([toFixedString('', 3), toFixedString('f', 3), toFixedString('fo', 3), toFixedString('foo', 3)]) val);
SELECT base62Decode(val) FROM (SELECT arrayJoin([toFixedString('1e', 2), toFixedString('6ox', 3), toFixedString('SAPP', 4)]) val);

SELECT base62Encode(reinterpretAsFixedString(byteSwap(toUInt256('256')))) == '00000000000000000000000000000048';
SELECT base62Encode(reinterpretAsString(byteSwap(toUInt256('256')))) == '0000000000000000000000000000001'; -- { reinterpretAsString drops the trailing null byte hence, encoded value is different than the FixedString version above }

-- The conversion is quadratic in the input length, so the input size is limited by the setting function_base62_max_input_size (10 KB by default, 0 disables the limit).
SELECT base62Encode(repeat('a', 10001)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT base62Decode(repeat('a', 10001)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT tryBase62Decode(repeat('a', 10001));
SELECT base62Decode(base62Encode(repeat('a', 10001))) == repeat('a', 10001) SETTINGS function_base62_max_input_size = 0;
