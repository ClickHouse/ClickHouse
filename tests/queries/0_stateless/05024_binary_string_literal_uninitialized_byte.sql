-- The buffer of a binary string literal is allocated for the worst case: ceil(bits / 8) bytes.
-- When the number of bits is not a multiple of eight, fewer bytes are written, and the rest of
-- the buffer must not end up in the result.

SELECT hex(b'000000001');
SELECT length(b'000000001');
SELECT hex(b'1');
SELECT hex(b'100000001');
SELECT hex(b'00000001');
SELECT hex(x'0001');
