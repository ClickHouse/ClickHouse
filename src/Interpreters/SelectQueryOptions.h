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
 *
 * is_internal
 * - the object was created only for internal queries.
 */
struct SelectQueryOptions
{
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;
    bool only_analyze = false;
    bool modify_inplace = false;
    bool remove_duplicates = false;
    bool ignore_quota = false;
    bool ignore_limits = false;
    bool is_internal = false;
    bool is_subquery = false; // non-subquery can also have subquery_depth > 0, e.g. insert select

    SelectQueryOptions(QueryProcessingStage::Enum stage = QueryProcessingStage::Complete, size_t depth = 0, bool is_subquery_ = false)
        : to_stage(stage), subquery_depth(depth), is_subquery(is_subquery_)
    {
    }

    SelectQueryOptions copy() const { return *this; }

    SelectQueryOptions subquery() const
    {
        SelectQueryOptions out = *this;
        out.to_stage = QueryProcessingStage::Complete;
        ++out.subquery_depth;
        out.is_subquery = true;
        return out;
    }

    SelectQueryOptions & analyze(bool dry_run = true)
    {
        only_analyze = dry_run;
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

    SelectQueryOptions & ignoreLimits(bool value = true)
    {
        ignore_limits = value;
        return *this;
    }

    SelectQueryOptions & setInternal(bool value = false)
    {
        is_internal = value;
        return *this;
    }
};

}
