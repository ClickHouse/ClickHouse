#pragma once

#include "config.h"

#if USE_YTSAURUS
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Core/YTsaurus/YTsaurusClient.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Storages/YTsaurus/YTsaurusSettings.h>
#include <optional>
#include <memory>

namespace DB
{

struct YTsaurusTableSourceOptions
{
    const String cypress_path;
    YTsaurusSettings settings;
    std::optional<Block> lookup_input_block = std::nullopt;
};

class YTsaurusTableSourceStaticTable final : public ISource
{
public:
    YTsaurusTableSourceStaticTable(
        YTsaurusClientPtr client_, const YTsaurusTableSourceOptions & source_options_, const SharedHeader & sample_block_, const UInt64 & max_block_size_);
    ~YTsaurusTableSourceStaticTable() override = default;

    String getName() const override { return "YTsaurusTableSourceStaticTable"; }

private:
    Chunk generate() override { return json_row_format->read(); }

    YTsaurusClientPtr client;
    const SharedHeader sample_block;
    UInt64 max_block_size;
    ReadBufferPtr read_buffer;
    std::unique_ptr<JSONEachRowRowInputFormat> json_row_format;
};

class YTsaurusTableSourceDynamicTable final : public ISource
{
public:
    YTsaurusTableSourceDynamicTable(
        YTsaurusClientPtr client_, const YTsaurusTableSourceOptions & source_options_, const SharedHeader & sample_block_, const UInt64 & max_block_size_);
    ~YTsaurusTableSourceDynamicTable() override = default;

    String getName() const override { return "YTsaurusTableSourceDynamicTable"; }

private:
    Chunk generate() override { return json_row_format->read(); }

    YTsaurusClientPtr client;
    const YTsaurusTableSourceOptions & source_options;
    const SharedHeader sample_block;
    UInt64 max_block_size;
    FormatSettings format_settings;
    ReadBufferPtr read_buffer;
    std::unique_ptr<JSONEachRowRowInputFormat> json_row_format;

    bool use_lookups;
};

struct YTsaurusSourceFactory
{
    static std::shared_ptr<ISource>
    createSource(YTsaurusClientPtr client, YTsaurusTableSourceOptions source_options, const SharedHeader & sample_block, UInt64 max_block_size);
};

}
#endif
