#pragma once

#include <base/extended_types.h>

/// Sets `hashes[i]` to `sipHash128Padded` of string `i`, for `i` in [0, num_rows). On aarch64 with NEON and on x86 with
/// AVX2 it hashes several strings at a time, elsewhere one by one. The hashes do not depend on the platform. String `i`
/// is [chars + offsets[i - 1], chars + offsets[i]), as in a `ColumnString`, so `offsets[-1]` must be readable. `chars`
/// needs 8 readable bytes before the first string and 7 after the last one, as a `PaddedPODArray` has.
void sipHash128PaddedStrings(const char * chars, const UInt64 * offsets, size_t num_rows, UInt128 * hashes);

/// The same for `num_rows` strings of `n` bytes each, string `i` at `chars + i * n`, as in a `ColumnFixedString`.
void sipHash128PaddedFixedStrings(const char * chars, size_t n, size_t num_rows, UInt128 * hashes);

/// The same two with `sipHash64`.
void sipHash64PaddedStrings(const char * chars, const UInt64 * offsets, size_t num_rows, UInt64 * hashes);
void sipHash64PaddedFixedStrings(const char * chars, size_t n, size_t num_rows, UInt64 * hashes);

/// Sets `hashes[i]` to `sipHash128` of the 32 bytes of `hashes[i]` and `rhs[i]`, each stored little-endian, for `i` in
/// [0, num_rows): how `sipHash128` combines the hashes of several values.
void sipHash128Combine(UInt128 * hashes, const UInt128 * rhs, size_t num_rows);
void sipHash128CombineWith(UInt128 * hashes, UInt128 rhs, size_t num_rows);

/// The same with `sipHash64` of the 16 bytes of two 64-bit hashes.
void sipHash64Combine(UInt64 * hashes, const UInt64 * rhs, size_t num_rows);
void sipHash64CombineWith(UInt64 * hashes, UInt64 rhs, size_t num_rows);

/// Whether the functions above hash several rows at a time. Where they do not, collecting rows for them gains nothing.
bool sipHashManyRowsIsVectorized();
