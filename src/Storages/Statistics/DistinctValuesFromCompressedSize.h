#pragma once

#include <base/types.h>

#include <optional>

namespace DB
{

/// Estimates the number of distinct values of a column of `rows` rows that takes `compressed_bytes` compressed
/// by a general purpose codec (LZ4, ZSTD), see the setting `estimate_distinct_values_from_metadata`.
///
/// The compressed size bounds the entropy H of the values: no codec stores them in less than H bits per row. So
/// `2^(compressed bits per row)` is about `2^H`, the effective number of distinct values (the number of equally
/// frequent values with the same entropy). It is the number of distinct values when they are equally frequent, and
/// less when a few of them make up most of the rows, which also makes rows with equal values more likely.
///
/// Measured on the ClickBench `hits` table, against the effective number of distinct values `1 / sum(p^2)` of each
/// column with less than 10 compressed bits per row: the median error is a factor of 1.9 and the 90th percentile 13.
/// At 10 bits per row and above, the codec sees only a block of the values at a time: a column of IP addresses
/// with 10 million distinct values takes 11 bits per row, as its values repeat within a block. Then there is no
/// estimate.
///
/// Returns nullopt for no rows or 10 compressed bits per row or more; otherwise a number in `[1, rows]`.
std::optional<UInt64> estimateDistinctValuesFromCompressedSize(UInt64 rows, UInt64 compressed_bytes);

}
