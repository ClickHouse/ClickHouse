#pragma once

#include <Storages/MergeTree/Streaming/ReadingPlan/ReadRoundContext.h>
#include <Storages/MergeTree/Streaming/ReadState.h>

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

/// Builds the reading pipeline for the next reading round, covering all readable partitions.
std::optional<ReadRoundPipeline> buildReadRoundPipeline(
    const ReadRoundContext & reading_context,
    const ReadState & state,
    const std::map<String, Int64> & safe_block_numbers);

}
