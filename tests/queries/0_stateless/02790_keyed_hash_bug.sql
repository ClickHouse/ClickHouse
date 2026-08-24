--- previously caused MemorySanitizer: use-of-uninitialized-value, because we tried to read hash key from empty tuple column during interpretation
SELECT sipHash64Keyed((1111111111111111111, toUInt64(222222222222223))) group by toUInt64(222222222222223);
