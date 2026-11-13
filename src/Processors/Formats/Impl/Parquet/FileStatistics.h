#pragma once

#include "config.h"

#if USE_PARQUET

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>
#include <unordered_map>

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
    /// The largest number of values of a dictionary page of the column chunks whose dictionary page header was read;
    /// nullopt if none was.
    std::optional<UInt64> max_dictionary_values;
};

struct ParquetFileSummary
{
    UInt64 rows = 0;
    /// By the column name, for the requested columns that are top-level primitive columns of the file.
    std::unordered_map<String, ParquetColumnSummary> columns;
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
