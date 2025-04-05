#pragma once

#include <Core/Joins.h>
#include <memory>
#include <vector>
#include <base/defines.h>

namespace DB
{


// struct ColumnStats
// {
//     size_t num_distinct_values;
// };

struct RelationStats
{
    size_t estimated_rows;
    // std::unordered_map<String, ColumnStats> column_stats;
};

struct DPJoinEntry;
struct JoinOperator;

size_t estimateJoinCardinality(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity,
    JoinKind join_kind = JoinKind::Cross);

double computeJoinCost(const std::shared_ptr<DPJoinEntry> & left, const std::shared_ptr<DPJoinEntry> & right, double selectivity);

double estimateJoinSelectivity(const JoinOperator & join_operator, const std::vector<RelationStats> & relation_stats);

}
