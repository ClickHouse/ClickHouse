#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/Streaming/CursorUtils.h>

namespace DB
{

/// Extends columns_to_read with queue mode columns.
Names extendColumnsWithStreamingAux(const Names & columns_to_read);

/// Drops queue mode columns that not needed in result of the query.
void addDropAuxColumnsStep(QueryPlan & query_plan, const Block & desired_header);

}
