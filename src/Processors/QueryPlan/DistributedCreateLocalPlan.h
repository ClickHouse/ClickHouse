#pragma once

#include <Core/QueryProcessingStage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ResizeProcessor.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

namespace DB
{

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t shard_num,
    size_t shard_count,
    size_t replica_num,
    size_t replica_count,
    std::shared_ptr<ParallelReplicasReadingCoordinator> coordinator);

}
