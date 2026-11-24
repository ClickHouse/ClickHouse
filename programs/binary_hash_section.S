/* The section for the hash of the executable segments (USE_BINARY_HASH), filled in place by `hash-binary --write-section`.
   It is not allocated, so it is not a part of the hashed segments. */
.section .clickhouse.hash,"",%progbits
.fill 32, 1, 0x30
