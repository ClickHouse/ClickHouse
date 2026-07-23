-- Tags: no-fasttest
-- Test that ULIDStringToDateTime rejects non-ASCII characters.
-- This is a regression test for a global-buffer-overflow in ulid_decode
-- where signed char values > 127 caused out-of-bounds reads into the
-- lookup table.

-- Valid ULID works
SELECT ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R') FORMAT Null;

-- Non-ASCII character (0xFF) at various positions
SELECT ULIDStringToDateTime(concat(repeat('0', 25), '\xFF')); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToDateTime(concat('\xFF', repeat('0', 25))); -- { serverError BAD_ARGUMENTS }

-- FixedString path: non-ASCII in FixedString(26)
SELECT ULIDStringToDateTime(toFixedString(concat('\xFF', repeat('0', 25)), 26)); -- { serverError BAD_ARGUMENTS }
