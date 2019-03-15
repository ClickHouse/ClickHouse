#pragma once

#include <Core/QueryProcessingStage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
 * to_stage
 * - the stage to which the query is to be executed. By default - till to the end.
 *   You can perform till the intermediate aggregation state, which are combined from different servers for distributed query processing.
 *
 * subquery_depth
 * - to control the limit on the depth of nesting of subqueries. For subqueries, a value that is incremented by one is passed;
 *   for INSERT SELECT, a value 1 is passed instead of 0.
 *
 * only_analyze
 * - the object was created only for query analysis.
 */
struct SelectQueryOptions
{
    QueryProcessingStage::Enum to_stage = QueryProcessingStage::Complete;
    size_t subquery_depth = 0;
    bool only_analyze = false;
    bool modify_inplace = false;

    static SelectQueryOptions run(QueryProcessingStage::Enum stage = QueryProcessingStage::Complete, size_t depth = 0)
    {
        return {stage, depth, false, false};
    }

    static SelectQueryOptions analyze(QueryProcessingStage::Enum stage = QueryProcessingStage::Complete, size_t depth = 0)
    {
        return {stage, depth, true, false};
    }

    static SelectQueryOptions analyzeModify(QueryProcessingStage::Enum stage, size_t depth = 0)
    {
        return {stage, depth, true, true};
    }

    const SelectQueryOptions & queryOptions() const { return *this; }

    SelectQueryOptions subqueryOptions(QueryProcessingStage::Enum stage) const
    {
        return SelectQueryOptions{stage, subquery_depth + 1, only_analyze, modify_inplace};
    }

    const SelectQueryOptions & checkZeroSubquery() const
    {
        if (subquery_depth)
            throw Exception("Logical error: zero subquery depth expected", ErrorCodes::LOGICAL_ERROR);
        return *this;
    }
};

}
