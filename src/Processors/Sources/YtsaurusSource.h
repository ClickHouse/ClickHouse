#pragma once

#include "config.h"

#if USE_YTSAURUS
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <Core/Ytsaurus/YtsaurusClient.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>


#include <memory>

namespace DB
{

class YtsaurusTableSource final : public ISource
{
public:
    YtsaurusTableSource(
        ytsaurus::YtsaurusClientPtr client_, const String & path, const Block & sample_block_, const UInt64 & max_block_size_, bool skip_unknown_columns = true)
        : ISource(sample_block_), client(std::move(client_)), sample_block(sample_block_), max_block_size(max_block_size_)
    {
        read_buffer = client->readTable(path);
        FormatSettings format_settings{.skip_unknown_fields = skip_unknown_columns};

        json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
            *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size = max_block_size}), format_settings, false);
    }

    ~YtsaurusTableSource() override { }

    String getName() const override { return "YtsaurusTableSource"; }

private:
    Chunk generate() override { return json_row_format->read(); }

    ytsaurus::YtsaurusClientPtr client;
    const Block sample_block;
    UInt64 max_block_size;
    ReadBufferPtr read_buffer;
    std::unique_ptr<JSONEachRowRowInputFormat> json_row_format;
};


struct YtsaurusSourceFactory
{
    static std::shared_ptr<ISource>
    createSource(ytsaurus::YtsaurusClientPtr client, const String & path, const Block & sample_block, const UInt64 & max_block_size)
    {
        auto yt_node_type = client->getNodeType(path);
        if (yt_node_type == ytsaurus::YtsaurusNodeType::STATIC_TABLE || yt_node_type == ytsaurus::YtsaurusNodeType::DYNAMIC_TABLE)
        {
            return std::make_shared<YtsaurusTableSource>(std::move(client), path, sample_block, max_block_size);
        }
        else 
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Node {} has unsupported type.", path);
        }
    }
};

}
#endif
