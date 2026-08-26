#pragma once

#include <Interpreters/JoinExpressionActions.h>

#include <utility>
#include <vector>

namespace DB
{

struct JoinOperator;

namespace QueryPlanOptimizations
{

using JoinActionRefPair = std::pair<JoinActionRef, JoinActionRef>;

/// Equi-key pairs of a JOIN, left expression first, skipping pairs whose sides have different types
std::vector<JoinActionRefPair> getJoiningKeysForJoinStep(const JoinOperator & join_operator);

}

}
