-- Tags: no-fasttest
-- Verify BIP173 (original bech32) support via string encoding variant.
-- When 'bech32' or 'bech32m' is passed as the 3rd argument, no witness
-- version byte is prepended, enabling Cosmos SDK and other non-SegWit use cases.
-- See https://github.com/ClickHouse/ClickHouse/issues/98737

-- Test 1: Cosmos SDK address encoding with BIP173 (bech32 original)
SELECT bech32Encode('inj', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 'bech32');

-- Test 2: Same data with bech32m variant produces different checksum
SELECT bech32Encode('inj', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 'bech32m');

-- Test 3: Existing SegWit mode still works (witness version 0 = bech32)
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0);

-- Test 4: Existing SegWit mode still works (witness version 1 = bech32m, default)
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));

-- Test 5: Roundtrip with raw decode
SELECT hex(tup.2) FROM (SELECT bech32Decode(bech32Encode('cosmos', unhex('DEADBEEF'), 'bech32'), 'raw') AS tup);

-- Test 6: Invalid encoding variant
SELECT bech32Encode('bc', unhex('DEADBEEF'), 'invalid'); -- { serverError BAD_ARGUMENTS }

-- Test 7: Invalid decode mode
SELECT bech32Decode('bc1test', 'invalid'); -- { serverError BAD_ARGUMENTS }
