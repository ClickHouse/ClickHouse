#pragma once

#include <Core/Block_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

namespace DB
{

RelationStats parseTableStatsHint(ContextPtr context, const String & table_name);
RelationStats parseTableStatsHint(const String & stats_hint_json, const String & table_name);
RelationStats getRandomizedStats(UInt64 seed, size_t relation_index, const String & table_name, const Block & header);

}
