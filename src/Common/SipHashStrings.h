#pragma once

#include <base/extended_types.h>

/// Sets `hashes[i]` to `sipHash128Padded` of string `i`, for `i` in [0, num_rows). On aarch64 with NEON and on x86 with
/// AVX2 it hashes several strings at a time, elsewhere one by one. The hashes do not depend on the platform. String `i`
/// is [chars + offsets[i - 1], chars + offsets[i]), as in a `ColumnString`, so `offsets[-1]` must be readable. `chars`
/// needs 8 readable bytes before the first string and 7 after the last one, as a `PaddedPODArray` has.
void sipHash128PaddedStrings(const char * chars, const UInt64 * offsets, size_t num_rows, UInt128 * hashes);

/// The same for `num_rows` strings of `n` bytes each, string `i` at `chars + i * n`, as in a `ColumnFixedString`.
void sipHash128PaddedFixedStrings(const char * chars, size_t n, size_t num_rows, UInt128 * hashes);

/// Like `sipHash128PaddedStrings`, but hashes only the strings `rows[0]`, ..., `rows[num_rows - 1]`: sets `hashes[rows[i]]`
/// for `i` in [0, num_rows) and leaves the other entries of `hashes` as they are.
void sipHash128PaddedStringRows(const char * chars, const UInt64 * offsets, const UInt32 * rows, size_t num_rows, UInt128 * hashes);

/// Like `sipHash128PaddedFixedStrings`, but hashes only the strings listed in `rows`, as `sipHash128PaddedStringRows` does.
void sipHash128PaddedFixedStringRows(const char * chars, size_t n, const UInt32 * rows, size_t num_rows, UInt128 * hashes);
