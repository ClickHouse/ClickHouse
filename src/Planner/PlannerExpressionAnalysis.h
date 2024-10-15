#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/InterpolateDescription.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Interpreters/ActionsDAG.h>

#include <Planner/PlannerContext.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerWindowFunctions.h>
#include <Planner/PlannerQueryProcessingInfo.h>

namespace DB
{

struct ProjectionAnalysisResult
{
    ActionsAndProjectInputsFlagPtr projection_actions;
    Names projection_column_names;
    NamesWithAliases projection_column_names_with_display_aliases;
    ActionsAndProjectInputsFlagPtr project_names_actions;
};

struct FilterAnalysisResult
{
    ActionsAndProjectInputsFlagPtr filter_actions;
    std::string filter_column_name;
    bool remove_filter_column = false;
};

struct AggregationAnalysisResult
{
    ActionsAndProjectInputsFlagPtr before_aggregation_actions;
    Names aggregation_keys;
    AggregateDescriptions aggregate_descriptions;
    GroupingSetsParamsList grouping_sets_parameters_list;
    bool group_by_with_constant_keys = false;
};

struct WindowAnalysisResult
{
    ActionsAndProjectInputsFlagPtr before_window_actions;
    std::vector<WindowDescription> window_descriptions;
};

struct SortAnalysisResult
{
    ActionsAndProjectInputsFlagPtr before_order_by_actions;
    bool has_with_fill = false;
};

struct LimitByAnalysisResult
{
    ActionsAndProjectInputsFlagPtr before_limit_by_actions;
    Names limit_by_column_names;
};

class PlannerExpressionsAnalysisResult
{
public:
    explicit PlannerExpressionsAnalysisResult(ProjectionAnalysisResult projection_analysis_result_)
        : projection_analysis_result(std::move(projection_analysis_result_))
    {}

    ProjectionAnalysisResult & getProjection()
    {
        return projection_analysis_result;
    }

    bool hasWhere() const
    {
        return where_analysis_result.filter_actions != nullptr;
    }

    FilterAnalysisResult & getWhere()
    {
        return where_analysis_result;
    }

    void addWhere(FilterAnalysisResult where_analysis_result_)
    {
        where_analysis_result = std::move(where_analysis_result_);
    }

    bool hasAggregation() const
    {
        return !aggregation_analysis_result.aggregation_keys.empty() || !aggregation_analysis_result.aggregate_descriptions.empty();
    }

    AggregationAnalysisResult & getAggregation()
    {
        return aggregation_analysis_result;
    }

    void addAggregation(AggregationAnalysisResult aggregation_analysis_result_)
    {
        aggregation_analysis_result = std::move(aggregation_analysis_result_);
    }

    bool hasHaving() const
    {
        return having_analysis_result.filter_actions != nullptr;
    }

    FilterAnalysisResult & getHaving()
    {
        return having_analysis_result;
    }

    void addHaving(FilterAnalysisResult having_analysis_result_)
    {
        having_analysis_result = std::move(having_analysis_result_);
    }

    bool hasWindow() const
    {
        return !window_analysis_result.window_descriptions.empty();
    }

    WindowAnalysisResult & getWindow()
    {
        return window_analysis_result;
    }

    void addWindow(WindowAnalysisResult window_analysis_result_)
    {
        window_analysis_result = std::move(window_analysis_result_);
    }

    bool hasQualify() const
    {
        return qualify_analysis_result.filter_actions != nullptr;
    }

    FilterAnalysisResult & getQualify()
    {
        return qualify_analysis_result;
    }

    void addQualify(FilterAnalysisResult qualify_analysis_result_)
    {
        qualify_analysis_result = std::move(qualify_analysis_result_);
    }

    bool hasSort() const
    {
        return sort_analysis_result.before_order_by_actions != nullptr;
    }

    SortAnalysisResult & getSort()
    {
        return sort_analysis_result;
    }

    void addSort(SortAnalysisResult sort_analysis_result_)
    {
        sort_analysis_result = std::move(sort_analysis_result_);
    }

    bool hasLimitBy() const
    {
        return limit_by_analysis_result.before_limit_by_actions != nullptr;
    }

    LimitByAnalysisResult & getLimitBy()
    {
        return limit_by_analysis_result;
    }

    void addLimitBy(LimitByAnalysisResult limit_by_analysis_result_)
    {
        limit_by_analysis_result = std::move(limit_by_analysis_result_);
    }

private:
    ProjectionAnalysisResult projection_analysis_result;
    FilterAnalysisResult where_analysis_result;
    AggregationAnalysisResult aggregation_analysis_result;
    FilterAnalysisResult having_analysis_result;
    WindowAnalysisResult window_analysis_result;
    FilterAnalysisResult qualify_analysis_result;
    SortAnalysisResult sort_analysis_result;
    LimitByAnalysisResult limit_by_analysis_result;
};

/// Build expression analysis result for query tree, join tree input columns and planner context
PlannerExpressionsAnalysisResult buildExpressionAnalysisResult(const QueryTreeNodePtr & query_tree,
    const ColumnsWithTypeAndName & join_tree_input_columns,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & planner_query_processing_info);

}
