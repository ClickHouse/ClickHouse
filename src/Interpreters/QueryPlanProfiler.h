#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
namespace DB
{

class QueryPlanProfiler
{
public:
    static bool canEnableProfiler(const ContextPtr & context, bool internal);

    String renderAsciiPlan(size_t max_description_length) const;

    void buildPrettyNames();

    void setQueryPlan(QueryPlan plan_) {
        query_plan.emplace(std::move(plan_));
    }

    QueryPlan& getQueryPlan() {
        chassert(query_plan.has_value());
        return query_plan.value();
    }

private:
    std::optional<QueryPlan> query_plan;
    std::optional<PrettyNamesPerPlan> pretty_names;
};
}
