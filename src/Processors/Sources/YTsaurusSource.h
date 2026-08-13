#pragma once

#include <cstddef>
#include "config.h"

#if USE_YTSAURUS
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Core/YTsaurus/YTsaurusClient.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Storages/YTsaurus/YTsaurusSettings.h>
#include <QueryPipeline/Pipe.h>
#include <optional>
#include <memory>

namespace DB
{

struct YTsaurusTableSourceOptions
{
    YTsaurusSettings settings;
    std::optional<Block> lookup_input_block = std::nullopt;
    std::optional<String> select_rows_columns = std::nullopt;
    YTsaurusTableLockPtr table_lock = nullptr;
    bool check_types_allow_nullable = false;
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

class YTsaurusTableSourceDynamicTable final : public ISource
{
public:
    YTsaurusTableSourceDynamicTable(
        YTsaurusClientPtr client_, const String & cypress_path, const YTsaurusTableSourceOptions & source_options_, const SharedHeader & sample_block_, const UInt64 & max_block_size_);
    ~YTsaurusTableSourceDynamicTable() override = default;

    String getName() const override { return "YTsaurusTableSourceDynamicTable"; }

private:
    Chunk generate() override { return json_row_format->read(); }

    YTsaurusClientPtr client;
    const SharedHeader sample_block;
    UInt64 max_block_size;
    FormatSettings format_settings;
    bool use_lookups;
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
