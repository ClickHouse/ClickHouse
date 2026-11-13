#pragma once

#include "config.h"

#if USE_PARQUET

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <Common/UnorderedMapWithMemoryTracking.h>

#include <optional>

namespace DB
{

class ReadBufferFromFileBase;
struct FormatSettings;
struct RelativePathWithMetadata;

/// What the metadata of a Parquet file says about one top-level column, without reading its data.
struct ParquetColumnSummary
{
    /// The rows of the row groups that have the column.
    UInt64 rows = 0;
    /// The sum of `total_compressed_size` of its column chunks, with the dictionary pages.
    UInt64 compressed_bytes = 0;
    /// The sum of `null_count` of its column chunks; nullopt if a chunk does not have it.
    std::optional<UInt64> nulls = 0;
    /// Every column chunk is dictionary-encoded in all its data pages (by `encoding_stats`), so its dictionary page has
    /// all its distinct values.
    bool fully_dictionary_encoded = true;
    /// The largest number of distinct values of the column estimated from a dictionary page of a column chunk whose
    /// header was read, see `estimateDistinctValuesFromSample`; nullopt if none was read.
    std::optional<UInt64> max_dictionary_values;
};

/// The number of equally frequent values `D` of a column of which a random sample of `sample_rows` rows has
/// `sample_distinct_values` distinct values: the solution of `D * (1 - exp(-sample_rows / D)) = sample_distinct_values`.
/// A sample with (almost) as many distinct values as rows tells no number, then the result is `max_distinct_values`.
/// Clamped to `[sample_distinct_values, max_distinct_values]`.
UInt64 estimateDistinctValuesFromSample(UInt64 sample_rows, UInt64 sample_distinct_values, UInt64 max_distinct_values);

struct ParquetFileSummary
{
    UInt64 rows = 0;
    /// By the column name, for the requested columns that are top-level primitive columns of the file.
    UnorderedMapWithMemoryTracking<String, ParquetColumnSummary> columns;
};

/// Reads the footer of the Parquet file in `buf` (through `ParquetMetadataCache` when enabled, keyed as the reader
/// keys it, so the read reuses it) and summarizes the columns `column_names`. Reads the headers of the dictionary
/// pages of at most `max_dictionary_pages_to_read` fully dictionary-encoded column chunks per column, one small read
/// each.
ParquetFileSummary summarizeParquetFile(
    ReadBufferFromFileBase & buf,
    const RelativePathWithMetadata & object,
    const FormatSettings & format_settings,
    const ContextPtr & context,
    const Names & column_names,
    size_t max_dictionary_pages_to_read);

}

#endif
