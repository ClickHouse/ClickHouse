#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
namespace DB
{

class QueryPlanProfiler
{
public:
    /// Returns nullptr when capture is off, so every downstream call site is a null check.
    static std::shared_ptr<QueryPlanProfiler> createIfEnabled(const ContextPtr & context, bool internal);

    void setQueryPlan(QueryPlan plan_) {
        query_plan.emplace(std::move(plan_));
    }
    bool hasQueryPlan() const { return query_plan.has_value(); }
    String renderAsciiPlan(size_t max_description_length) const;

private:
    std::optional<QueryPlan> query_plan;
};
}
