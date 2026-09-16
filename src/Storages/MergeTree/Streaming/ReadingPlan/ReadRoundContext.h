#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

#include <Core/Streaming/Settings.h>

namespace DB
{

struct ReadRoundContext
{
    const MergeTreeData & storage;
    const SelectQueryInfo query_info;
    const PrewhereInfoPtr prewhere_info;
    const FilterDAGInfoPtr row_level_filter;
    const StreamSettings stream_settings;
    const ContextPtr context;
    const Names user_requested_columns;
    const size_t requested_num_streams;
    const UInt64 max_block_size;
    const SharedHeader output_header;
};

Names extendWithAuxiliaryColumns(
    Names columns,
    const StreamSettings & stream_settings,
    const FilterDAGInfoPtr & row_level_filter,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context);

ReadRoundContext makeReadRoundContext(
    const MergeTreeData & storage,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    Names user_requested_columns,
    size_t requested_num_streams,
    UInt64 max_block_size,
    SharedHeader output_header);

}
