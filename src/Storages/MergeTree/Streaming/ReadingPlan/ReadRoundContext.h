#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

#include <Core/Streaming/Settings.h>

#include <optional>

namespace DB
{

struct ReadRoundContext
{
    const MergeTreeData & storage;
    const SelectQueryInfo query_info;
    const StreamSettings stream_settings;
    const std::optional<FilterDAGInfo> row_level_filter;
    const std::optional<FilterDAGInfo> prewhere_filter;
    const ContextPtr context;
    const Names columns_to_read;
    const size_t requested_num_streams;
    const UInt64 max_block_size;
    const SharedHeader output_header;
};

ReadRoundContext makeReadRoundContext(
    const MergeTreeData & storage,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    Names user_requested_columns,
    size_t requested_num_streams,
    UInt64 max_block_size,
    SharedHeader output_header);

}
