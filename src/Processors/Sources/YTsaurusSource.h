#pragma once

#include "config.h"

#if USE_YTSAURUS
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <Core/YTsaurus/YTsaurusClient.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>


#include <memory>

namespace DB
{

class YTsaurusTableSource final : public ISource
{
public:
    YTsaurusTableSource(
        YTsaurusClientPtr client_, const String & path, const Block & sample_block_, const UInt64 & max_block_size_, bool is_dynamic_table, bool skip_unknown_columns = true);
    ~YTsaurusTableSource() override = default;

    String getName() const override { return "YTsaurusTableSource"; }

private:
    Chunk generate() override { return json_row_format->read(); }

    YTsaurusClientPtr client;
    const Block sample_block;
    UInt64 max_block_size;
    ReadBufferPtr read_buffer;
    std::unique_ptr<JSONEachRowRowInputFormat> json_row_format;
};


struct YTsaurusSourceFactory
{
    static std::shared_ptr<ISource>
    createSource(YTsaurusClientPtr client, const String & path, const Block & sample_block, const UInt64 max_block_size);
};

}
#endif
