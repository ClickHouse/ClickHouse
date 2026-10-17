#pragma once

#include <Columns/IColumn.h>

#include <optional>


namespace DB
{

/** Replaces negative zeros with positive zeros in a floating point column,
  * recursively for `Nullable`, `Array`, `Tuple`, `Map`, `Variant` and `Dynamic` columns.
  *
  * Negative zero is equal to positive zero by the rules of comparison, but has a different binary
  * representation, and hash tables compare floating point values bitwise - see `normalizeNegativeZero`.
  * Hash methods also reinterpret floating point columns as integer columns of the same width
  * (a `Float64` key is looked up in a `UInt64` hash table, and composite keys are packed or serialized
  * as raw bytes), so by the time a key is hashed, the information that it is a floating point value
  * is already lost. That is why the canonicalization is done on the columns.
  *
  * Returns `nullptr` if the column contains no negative zeros, which is by far the most common case,
  * so that the caller can use the original column and avoid making a copy.
  */
ColumnPtr canonicalizeNegativeZero(const IColumn & column);

/** The same, but also for a top-level `LowCardinality` column, which is rebuilt with a new dictionary:
  * the dictionary can hold both zeros as different values, so it cannot be canonicalized in place.
  * It is for the callers that compare the values of a `LowCardinality` column by their raw bytes or by the hash
  * of its dictionary values, but have to keep the `LowCardinality` type of the column.
  */
ColumnPtr canonicalizeNegativeZeroThroughLowCardinality(const IColumn & column);

/// The same, in place, for a list of key columns.
/// The canonicalized columns are appended to `holder`, which has to outlive the usage of `key_columns`.
void canonicalizeNegativeZeroInKeyColumns(ColumnRawPtrs & key_columns, Columns & holder);

/** A value in the shared variant of a `Dynamic` column and in the shared data of a `JSON` column
  * is stored as the binary encoding of its type followed by the value in binary format
  * (see `SerializationDynamic::serializeBinary`), so it cannot be canonicalized in place -
  * it has to be decoded and encoded back.
  * Returns the canonicalized encoded value, or `nullopt` if the value contains no negative zeros.
  */
std::optional<String> canonicalizeNegativeZeroInEncodedValue(std::string_view encoded_value);

/** `IColumn::updateHashWithValue` agrees with `equals`, so it hashes `-0.` as `0.`. That is wrong where a hash
  * identifies a constant, e.g. the key of the cache of compiled expressions or of the query condition cache,
  * because the two zeros are different values: `1 / -0.` is `-inf`.
  *
  * Returns the serialized value of `column` at row `n` if it contains a negative zero, `nullopt` otherwise,
  * which is by far the most common case. A caller that hashes a value with `updateHashWithValue` also hashes
  * this representation, so that the two zeros hash differently and every other value hashes as it did before.
  * A caller that compares values with `compareAt` also compares this representation, for the same reason.
  */
std::optional<String> serializeValueIfContainsNegativeZero(const IColumn & column, size_t n);

/** `IColumn::updateHashWithValue` hashes negative zero as positive zero. This is the way it hashed values before
  * that, by the raw bits of the floating point values, recursively for `Nullable`, `Array`, `Tuple`, `Map`,
  * `LowCardinality`, `Variant`, `Dynamic`, `JSON` and `Const` columns. It is for the hashes that are a part of a data format, such as the
  * bucket of a key of a `Map` in a data part, so that the parts written by older versions remain readable.
  */
void updateHashWithRawValue(const IColumn & column, size_t n, SipHash & hash);

/** A value of a column can be represented in memory as a flat sequence of floating point values of
  * the same width: a `Float64` value, an `Array(Float32)` row, a `LowCardinality(Float64)` value.
  * Such a value is used as a hash table key by the raw bytes that `IColumn::getDataAt` returns, and
  * these bytes cannot be canonicalized in place, so a canonicalized copy has to be made.
  *
  * Returns the width of one floating point value in such a representation, or 0 if a value of this
  * column contains no floating point values, which is by far the most common case.
  */
size_t rawFloatValueWidth(const IColumn & column);

/// Copies `value` to `res`, replacing the negative zeros in it with positive zeros.
/// `width` is what `rawFloatValueWidth` returned for the column that `value` came from.
void canonicalizeNegativeZeroInRawValue(std::string_view value, size_t width, char * res);

}
