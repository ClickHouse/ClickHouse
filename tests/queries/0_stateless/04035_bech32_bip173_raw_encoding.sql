-- Tags: no-fasttest
-- Reason: `bech32Encode` / `bech32Decode` are gated behind `USE_BECH32`, which is not enabled in the Fast test build.
-- Verify BIP173 (original bech32) support via string encoding variant.
-- When 'bech32' or 'bech32m' is passed as the 3rd argument, no witness
-- version byte is prepended, enabling Cosmos SDK and other non-SegWit use cases.
-- See https://github.com/ClickHouse/ClickHouse/issues/98737

-- Test 1: Raw bech32 and bech32m produce DIFFERENT addresses (different checksums)
SELECT bech32Encode('inj', unhex('DEADBEEF'), 'bech32') != bech32Encode('inj', unhex('DEADBEEF'), 'bech32m');

-- Test 2: Raw bech32 does NOT prepend a witness version byte.
-- SegWit v0 of the same data starts with 'bc1q' (q = witness version 0).
-- Raw bech32 must (a) be exactly one character shorter (no witness version byte)
-- and (b) start with 'bc1w' (the first 5-bit group of 0x75 = 01110 = 14 = 'w'),
-- not 'bc1q'. Both invariants would regress if the witness version byte was still
-- being prepended.
SELECT
    length(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 'bech32')) + 1
        = length(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0))
    AND substring(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 'bech32'), 1, 4) = 'bc1w';

-- Test 3: Existing SegWit mode still works (witness version 0 = bech32, known reference value)
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0);

-- Test 4: Existing SegWit mode still works (witness version 1 = bech32m, default, known reference value)
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- Test 5: Decode a fixed external BIP-173 bech32 test vector that was NOT produced by `bech32Encode`
-- in this PR. From the BIP-173 specification (https://github.com/bitcoin/bips/blob/master/bip-0173.mediawiki),
-- the string `split1checkupstagehandshakeupstreamerranterredcaperred2y9e3w` is a valid bech32
-- with human-readable part `split`. In raw mode the data is preserved as-is (no first byte stripped).
SELECT bech32Decode('split1checkupstagehandshakeupstreamerranterredcaperred2y9e3w', 'raw').1;

-- Test 6: Roundtrip - raw encode then raw decode recovers original data
SELECT hex(tup.2) FROM (SELECT bech32Decode(bech32Encode('cosmos', unhex('DEADBEEF'), 'bech32'), 'raw') AS tup);

-- Test 7: Invalid encoding variant
SELECT bech32Encode('bc', unhex('DEADBEEF'), 'invalid'); -- { serverError BAD_ARGUMENTS }

-- Test 8: Invalid decode mode
SELECT bech32Decode('bc1test', 'invalid'); -- { serverError BAD_ARGUMENTS }
