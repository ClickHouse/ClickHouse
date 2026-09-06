#pragma once

#include "config.h"

#if USE_YTSAURUS
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Core/YTsaurus/YTsaurusClient.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Storages/YTsaurus/YTsaurusSettings.h>
#include <QueryPipeline/Pipe.h>
#include <algorithm>
#include <cstddef>
#include <optional>
#include <memory>

namespace DB
{

/// Number of rows of the next `lookup_rows` request payload that starts at row `offset` of a lookup input block with
/// `total_rows` rows. `chunk_size == 0` means unlimited, i.e. all the remaining rows go into a single request.
/// Returns zero when there is nothing left to request, so an empty input never produces a request at all.
inline size_t lookupChunkRows(size_t total_rows, size_t offset, size_t chunk_size)
{
    if (offset >= total_rows)
        return 0;
    const size_t rows_left = total_rows - offset;
    return chunk_size ? std::min(chunk_size, rows_left) : rows_left;
}

struct YTsaurusTableSourceOptions
{
    YTsaurusSettings settings;
    /// The whole payload of a selective load. It is split into `lookup_max_rows_per_query` sized requests lazily by
    /// `YTsaurusTableSourceDynamicTableLookup`, so neither the number of pipeline sources nor the number of
    /// materialized `Block` objects depends on the chunk size.
    std::optional<Block> lookup_input_block = std::nullopt;
    std::optional<String> select_rows_columns = std::nullopt;
    YTsaurusTableLockPtr table_lock = nullptr;
    bool check_types_allow_nullable = false;
    ThrottlerPtr lookup_throttler = nullptr;
};

class YTsaurusTableSourceStaticTable final : public ISource
{
public:
    YTsaurusTableSourceStaticTable(
        YTsaurusClientPtr client_, const String & cypress_path, std::pair<size_t, size_t> rows_range_, const YTsaurusTableSourceOptions & source_options_, const SharedHeader & sample_block_, const UInt64 & max_block_size_);
    ~YTsaurusTableSourceStaticTable() override = default;

    String getName() const override { return "YTsaurusTableSourceStaticTable"; }

private:
    Chunk generate() override;

    YTsaurusClientPtr client;
    const String cypress_path;
    std::pair<size_t, size_t> rows_range;
    const YTsaurusTableSourceOptions source_options;
    const SharedHeader sample_block;
    const UInt64 max_block_size;

    std::unique_ptr<JSONEachRowRowInputFormat> json_row_format;
    ReadBufferPtr read_buffer;
};

class YTsaurusTableSourceDynamicTableSelect final : public ISource
{
public:
    YTsaurusTableSourceDynamicTableSelect(
        YTsaurusClientPtr client_,
        const String & cypress_path,
        const SharedHeader & sample_block_,
        const UInt64 & max_block_size_,
        bool format_skip_unknown_columns_,
        std::optional<String> select_rows_columns_,
        YTsaurusTableLockPtr table_lock_);
        ~YTsaurusTableSourceDynamicTableSelect() override = default;

    String getName() const override { return "YTsaurusTableSourceDynamicTableSelect"; }

private:
    Chunk generate() override;

    YTsaurusClientPtr client;
    const String cypress_path;
    const SharedHeader sample_block;
    UInt64 max_block_size;
    FormatSettings format_settings;
    std::optional<String> select_rows_columns;
    YTsaurusTableLockPtr table_lock;
    ReadBufferPtr read_buffer;
    std::unique_ptr<JSONEachRowRowInputFormat> json_row_format;
};

class YTsaurusTableSourceDynamicTableLookup final : public ISource
{
public:
    YTsaurusTableSourceDynamicTableLookup(
        YTsaurusClientPtr client_,
        const String & cypress_path,
        const SharedHeader & sample_block_,
        const UInt64 & max_block_size_,
        bool format_skip_unknown_columns_,
        Block lookup_input_block_,
        size_t lookup_max_rows_per_query_,
        ThrottlerPtr lookup_throttler_,
        YTsaurusTableLockPtr table_lock_);
    ~YTsaurusTableSourceDynamicTableLookup() override = default;

    String getName() const override { return "YTsaurusTableSourceDynamicTableLookup"; }

private:
    Chunk generate() override;

    YTsaurusClientPtr client;
    const String cypress_path;
    const SharedHeader sample_block;
    UInt64 max_block_size;
    FormatSettings format_settings;
    /// The whole lookup payload. A single source issues the `lookup_rows` requests for its slices of at most
    /// `lookup_max_rows_per_query` rows sequentially, cutting every slice out only right before its request, so that
    /// neither the number of pipeline sources nor the amount of memory held at once depends on the chunk size.
    Block lookup_input_block;
    size_t lookup_max_rows_per_query;
    size_t next_row = 0;
    ThrottlerPtr lookup_throttler;
    YTsaurusTableLockPtr table_lock;
    ReadBufferPtr read_buffer;
    std::unique_ptr<JSONEachRowRowInputFormat> json_row_format;
};

struct YTsaurusSourceFactory
{
    static Pipe
    createPipe(YTsaurusClientPtr client, const String & cypress_path, YTsaurusTableSourceOptions source_options, const SharedHeader & sample_block, UInt64 max_block_size, UInt64 max_streams);
};

}
#endif
