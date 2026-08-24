#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
namespace DB
{

class QueryPipeline;

class QueryPlanProfiler
{
public:
    static bool canEnableProfiler(const ContextPtr & context, bool internal);

    void buildPrettyNames();

    void setQueryPlan(QueryPlan plan_) { query_plan.emplace(std::move(plan_)); }

    QueryPlan & getQueryPlan()
    {
        chassert(query_plan.has_value());
        return query_plan.value();
    }

    bool hasQueryPlan() const { return query_plan.has_value(); }

    /// The plan rendered for the log. Prefers the version produced at pipeline-finalize time,
    /// which carries per-step statistics; queries that never reached that point (failures during
    /// or before execution) are rendered here without them.
    String getRenderedPlan() const
    {
      if (rendered_plan)
          return *rendered_plan;
      return renderAsciiPlan();
    }

    /// Renders the plan with per-step runtime statistics and keeps the result. Must be called
    /// while the pipeline is alive: AnalyzeStepsStats reads the processors, and their reports
    /// are only reachable before the pipeline is reset.
    void renderWithStats(const QueryPipeline & pipeline, UInt64 execution_time_ns);

    void setMaxDescriptionLength(size_t max_length) { max_description_length = max_length; }

private:

    String renderAsciiPlan() const;

    size_t max_description_length {0};
    std::optional<QueryPlan> query_plan;
    std::optional<PrettyNamesPerPlan> pretty_names;
    std::optional<String> rendered_plan;
};
}
