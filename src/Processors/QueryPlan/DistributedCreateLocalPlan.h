#pragma once

#include <Core/QueryProcessingStage.h>
#include <Core/UUID.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ResizeProcessor.h>

namespace DB
{

class PreparedSets;
using PreparedSetsPtr = std::shared_ptr<PreparedSets>;

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t shard_num,
    size_t shard_count,
    bool has_missing_objects);
}
