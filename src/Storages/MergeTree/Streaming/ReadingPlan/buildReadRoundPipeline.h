#pragma once

#include <Storages/MergeTree/Streaming/ReadState.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

#include <Core/Streaming/Settings.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>

#include <Common/Logger.h>

#include <optional>

namespace DB
{

struct ReadRoundPipeline
{
    Pipe pipe;
    QueryPlanResourceHolder resources;
};

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

/// Builds the reading pipeline for the next reading round, covering all readable partitions.
std::optional<ReadRoundPipeline> buildReadRoundPipeline(
    const ReadRoundContext & reading_context,
    const StreamReadState & state,
    const std::map<String, Int64> & safe_block_numbers);

}
