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
class SelectQueryOptions
{
public:
    SelectQueryOptions(QueryProcessingStage::Enum stage = QueryProcessingStage::Complete, size_t depth = 0)
        : to_stage(stage)
        , subquery_depth(depth)
        , only_analyze(false)
        , modify_inplace(false)
    {}

    const SelectQueryOptions & queryOptions() const { return *this; }

    SelectQueryOptions subqueryOptions(QueryProcessingStage::Enum stage) const
    {
        SelectQueryOptions out = *this;
        out.to_stage = stage;
        ++out.subquery_depth;
        return out;
    }

    friend SelectQueryOptions analyze(const SelectQueryOptions & src, bool value = true)
    {
        SelectQueryOptions out = src;
        out.only_analyze = value;
        return out;
    }

    friend SelectQueryOptions modify(const SelectQueryOptions & src, bool value = true)
    {
        SelectQueryOptions out = src;
        out.modify_inplace = value;
        return out;
    }

    friend SelectQueryOptions noSubquery(const SelectQueryOptions & src)
    {
        SelectQueryOptions out = src;
        out.subquery_depth = 0;
        return out;
    }

    friend SelectQueryOptions noModify(const SelectQueryOptions & src) { return modify(src, false); }
    friend bool isSubquery(const SelectQueryOptions & opt) { return opt.subquery_depth; }

protected:
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;
    bool only_analyze;
    bool modify_inplace;
};

}
