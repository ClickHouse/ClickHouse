#include "config.h"

#if USE_YTSAURUS
#include "YTsaurusSource.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

YTsaurusTableSource::YTsaurusTableSource(
    YTsaurusClientPtr client_, const String & path, const Block & sample_block_, const UInt64 & max_block_size_, const bool is_dynamic_table, const bool skip_unknown_columns)
    : ISource(sample_block_), client(std::move(client_)), sample_block(sample_block_), max_block_size(max_block_size_)
{
    read_buffer = (is_dynamic_table) ? client->selectRows(path) : client->readTable(path);
    FormatSettings format_settings{.skip_unknown_fields = skip_unknown_columns};

    json_row_format = std::make_unique<JSONEachRowRowInputFormat>(
        *read_buffer.get(), sample_block, IRowInputFormat::Params({.max_block_size = max_block_size}), format_settings, false);
}

std::shared_ptr<ISource> YTsaurusSourceFactory::createSource(YTsaurusClientPtr client, const String & path, const Block & sample_block, const UInt64 max_block_size)
{
    auto yt_node_type = client->getNodeType(path);
    if (yt_node_type == YTsaurusNodeType::STATIC_TABLE || yt_node_type == YTsaurusNodeType::DYNAMIC_TABLE)
    {
        return std::make_shared<YTsaurusTableSource>(std::move(client), path, sample_block, max_block_size, yt_node_type == YTsaurusNodeType::DYNAMIC_TABLE);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Node {} has unsupported type.", path);
    }
}

}
#endif
