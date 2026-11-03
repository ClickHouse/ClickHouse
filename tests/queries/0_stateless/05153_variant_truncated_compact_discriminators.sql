-- Truncated `COMPACT` `Variant` discriminator streams must be rejected, not read past the buffer.
SELECT * FROM format(Native, 'v Variant(UInt8)', concat('\x01\x64\x01v\x0eVariant(UInt8)', unhex('01000000000000006400'))); -- { serverError CANNOT_READ_ALL_DATA }
SELECT * FROM format(Native, 'v Variant(UInt8)', concat('\x01\x02\x01v\x0eVariant(UInt8)', unhex('01000000000000000200ff'))); -- { serverError CANNOT_READ_ALL_DATA }
SELECT * FROM format(Native, 'v Variant(UInt8)', concat('\x01\x6c\x01v\x0eVariant(UInt8)', unhex('01000000000000000801ff6400ff'))); -- { serverError CANNOT_READ_ALL_DATA }

-- Complete streams still decode correctly.
SELECT * FROM format(Native, 'v Variant(UInt8)', concat('\x01\x02\x01v\x0eVariant(UInt8)', unhex('0100000000000000020000ff2a')));
SELECT * FROM format(Native, 'v Variant(UInt8)', concat('\x01\x04\x01v\x0eVariant(UInt8)', unhex('01000000000000000201ff020000ff2a')));

CREATE TABLE variant_compact_granules (id UInt8, v Variant(UInt8, String))
ENGINE = MergeTree ORDER BY id
SETTINGS use_compact_variant_discriminators_serialization = 1, index_granularity = 8,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO variant_compact_granules SELECT number, if(number % 2 = 0, number::UInt8, NULL) FROM numbers(8);
SELECT v FROM variant_compact_granules ORDER BY id SETTINGS max_block_size = 1, max_threads = 1;
SELECT v.UInt8 FROM variant_compact_granules ORDER BY id SETTINGS max_block_size = 1, max_threads = 1;
SELECT v.UInt8.null FROM variant_compact_granules ORDER BY id SETTINGS max_block_size = 1, max_threads = 1;
DROP TABLE variant_compact_granules;
