#pragma once

#include <Core/QueryProcessingStage.h>

namespace DB
{

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
 *
 * is_subquery
 * - there could be some specific for subqueries. Ex. there's no need to pass duplicated columns in results, cause of indirect results.
 */
struct SelectQueryOptions
{
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;
    bool only_analyze;
    bool modify_inplace;
    bool remove_duplicates;

    SelectQueryOptions(QueryProcessingStage::Enum stage = QueryProcessingStage::Complete, size_t depth = 0)
        : to_stage(stage)
        , subquery_depth(depth)
        , only_analyze(false)
        , modify_inplace(false)
        , remove_duplicates(false)
    {}

    SelectQueryOptions copy() const { return *this; }

    SelectQueryOptions subquery() const
    {
        SelectQueryOptions out = *this;
        out.to_stage = QueryProcessingStage::Complete;
        ++out.subquery_depth;
        return out;
    }

    SelectQueryOptions & analyze(bool value = true)
    {
        only_analyze = value;
        return *this;
    }

    SelectQueryOptions & modify(bool value = true)
    {
        modify_inplace = value;
        return *this;
    }

    SelectQueryOptions & noModify() { return modify(false); }

    SelectQueryOptions & removeDuplicates(bool value = true)
    {
        remove_duplicates = value;
        return *this;
    }

    SelectQueryOptions & noSubquery()
    {
        subquery_depth = 0;
        return *this;
    }
};

}
