#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Interpreters/Context.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/Stopwatch.h>

#include <Core/Settings.h>

#include <fmt/format.h>
#include <numeric>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Parse EXPLAIN WHATIF settings from the AST.
struct WhatIfSettings
{
    bool empirical = true;
    UInt64 max_sample_parts = 10;
    UInt64 max_marks_sampled = 10000;
    UInt64 max_time_ms = 200;
    UInt64 sampling_seed = 0;
    String sample_strategy = "random";
    bool json = false;

    static WhatIfSettings fromAST(const ASTPtr & settings_ast)
    {
        WhatIfSettings result;
        if (!settings_ast)
            return result;

        const auto * set_query = settings_ast->as<ASTSetQuery>();
        if (!set_query)
            return result;

        for (const auto & change : set_query->changes)
        {
            if (change.name == "empirical")
                result.empirical = change.value.safeGet<UInt64>() != 0;
            else if (change.name == "max_sample_parts")
                result.max_sample_parts = change.value.safeGet<UInt64>();
            else if (change.name == "max_marks_sampled")
                result.max_marks_sampled = change.value.safeGet<UInt64>();
            else if (change.name == "max_time_ms")
                result.max_time_ms = change.value.safeGet<UInt64>();
            else if (change.name == "sampling_seed")
                result.sampling_seed = change.value.safeGet<UInt64>();
            else if (change.name == "sample_strategy")
                result.sample_strategy = change.value.safeGet<String>();
            else if (change.name == "json")
                result.json = change.value.safeGet<UInt64>() != 0;
        }
        return result;
    }
};

/// Find all ReadFromMergeTree steps in the query plan.
void collectReadSteps(const QueryPlan::Node * node, std::vector<ReadFromMergeTree *> & steps)
{
    if (!node)
        return;

    if (auto * read_step = dynamic_cast<ReadFromMergeTree *>(node->step.get()))
        steps.push_back(read_step);

    for (const auto & child : node->children)
        collectReadSteps(child, steps);
}

/// Evaluate a single hypothetical index against the baseline.
/// Checks applicability via createIndexCondition + alwaysUnknownOrTrue.
/// Full empirical evaluation (reading part column data, building in-memory granules)
/// is planned for Phase 2; this version reports the condition's filtering capability
/// and provides a stat-based estimate.
WhatIfIndexEstimator::IndexResult evaluateIndex(
    const IndexDescription & index_desc,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const WhatIfSettings & /* settings */,
    ContextPtr context)
{
    WhatIfIndexEstimator::IndexResult result;
    result.index_name = index_desc.name;
    result.index_type = index_desc.type;
    result.total_parts = analysis.selected_parts;
    result.total_marks = analysis.selected_marks;

    /// Create the index helper from the factory.
    MergeTreeIndexPtr index_helper;
    try
    {
        index_helper = MergeTreeIndexFactory::instance().get(index_desc);
    }
    catch (...)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Failed to create index: " + getCurrentExceptionMessage(false);
        return result;
    }

    /// Get the filter DAG from the read step.
    const auto & filter_dag = read_step->getFilterActionsDAG();
    if (!filter_dag)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Query has no filter predicate";
        return result;
    }

    /// Build the index condition from the filter predicate.
    MergeTreeIndexConditionPtr condition;
    try
    {
        condition = index_helper->createIndexCondition(filter_dag->getOutputs().front(), context);
    }
    catch (...)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Cannot build index condition: " + getCurrentExceptionMessage(false);
        return result;
    }

    if (!condition || condition->alwaysUnknownOrTrue())
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Index cannot filter this predicate (always unknown or true)";
        return result;
    }

    result.status = WhatIfIndexEstimator::IndexResult::Applicable;

    /// For the MVP, report that the index is applicable with the condition description.
    /// Full empirical evaluation (Mode B) with actual column data reading is Phase 2.
    /// For now, provide a stat-based placeholder noting that empirical mode is not yet implemented.
    result.empirical_status = WhatIfIndexEstimator::IndexResult::Unsupported;
    result.estimate_source = "statistical";
    result.confidence = "LOW";

    /// Conservative stat-based estimate.
    /// Without column statistics, we can only report applicability.
    /// The index condition was successfully built, meaning the predicate IS filterable.
    result.estimated_marks = analysis.selected_marks;
    result.estimated_parts = analysis.selected_parts;
    result.skip_ratio = 0.0;

    result.warnings.push_back(
        "Empirical estimation (Mode B) requires reading part column data and is not yet implemented. "
        "The index IS applicable to this predicate. Use ADD STATISTICS + MATERIALIZE STATISTICS "
        "for column-level stats, or wait for Phase 2 with full empirical support.");

    return result;
}

} // anonymous namespace


WhatIfIndexEstimator::Result WhatIfIndexEstimator::run(
    const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings)
{
    auto settings = WhatIfSettings::fromAST(explain_settings);

    /// Build the query plan to get the baseline.
    SelectQueryOptions query_options;
    query_options.setExplain();
    QueryPlan plan;
    ContextPtr plan_context = context;

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(select_query, context, query_options);
        plan_context = interpreter.getContext();
        plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter(select_query, context, query_options);
        plan_context = interpreter.getContext();
        interpreter.buildQueryPlan(plan);
    }

    /// Build the pipeline to trigger index analysis and populate AnalysisResult.
    auto builder = plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(plan_context),
        BuildQueryPipelineSettings(plan_context));

    /// Find ReadFromMergeTree steps.
    std::vector<ReadFromMergeTree *> read_steps;
    collectReadSteps(plan.getRootNode(), read_steps);

    if (read_steps.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "EXPLAIN WHATIF requires a query reading from a MergeTree family table");

    if (read_steps.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "EXPLAIN WHATIF currently supports only single-table queries (found {} read steps)",
            read_steps.size());

    auto * read_step = read_steps[0];
    const auto & analysis = read_step->getAnalysisResult();
    const auto & data = read_step->getMergeTreeData();

    Result result;
    result.database = data.getStorageID().getDatabaseName();
    result.table = data.getStorageID().getTableName();
    result.baseline_parts = analysis.selected_parts;
    result.baseline_marks = analysis.selected_marks;

    /// Estimate baseline bytes.
    if (analysis.selected_rows > 0)
    {
        auto total_bytes = data.getTotalActiveSizeInBytes();
        auto total_rows = data.getTotalActiveSizeInRows();
        if (total_rows > 0)
            result.baseline_est_bytes = static_cast<UInt64>(
                static_cast<double>(total_bytes) / total_rows * analysis.selected_rows);
    }

    /// Get hypothetical indexes for this table.
    const auto & store = context->getHypotheticalIndexStore();
    auto hypo_indexes = store.getForTable(data.getStorageID());

    if (hypo_indexes.empty())
    {
        IndexResult no_index;
        no_index.index_name = "(none)";
        no_index.status = IndexResult::NotApplicable;
        no_index.not_applicable_reason = "No hypothetical indexes defined for this table. "
            "Use CREATE HYPOTHETICAL INDEX to define one.";
        result.index_results.push_back(std::move(no_index));
        return result;
    }

    /// Evaluate each hypothetical index independently against baseline.
    for (const auto & index_desc : hypo_indexes)
    {
        auto index_result = evaluateIndex(index_desc, read_step, analysis, settings, context);
        result.index_results.push_back(std::move(index_result));
    }

    return result;
}


void WhatIfIndexEstimator::Result::format(WriteBuffer & out) const
{
    writeCString("Baseline (after PK + partition + existing indexes):\n", out);
    writeString(fmt::format("  table:       {}.{}\n", database, table), out);
    writeString(fmt::format("  parts:       {}\n", baseline_parts), out);
    writeString(fmt::format("  marks:       {}\n", baseline_marks), out);
    if (baseline_est_bytes > 0)
    {
        double gb = static_cast<double>(baseline_est_bytes) / (1024.0 * 1024.0 * 1024.0);
        if (gb >= 0.01)
            writeString(fmt::format("  est_bytes:   {:.2f} GB\n", gb), out);
        else
            writeString(fmt::format("  est_bytes:   {} B\n", baseline_est_bytes), out);
    }
    writeCString("\n", out);

    for (const auto & idx : index_results)
    {
        if (!idx.index_type.empty())
            writeString(fmt::format("With {} ({}, hypothetical):\n", idx.index_name, idx.index_type), out);
        else
            writeString(fmt::format("{}:\n", idx.index_name), out);

        if (idx.status == IndexResult::NotApplicable)
        {
            writeCString("  status:       not_applicable\n", out);
            writeString(fmt::format("  reason:       {}\n", idx.not_applicable_reason), out);
            writeCString("\n", out);
            continue;
        }

        writeCString("  status:       applicable\n", out);
        writeString(fmt::format("  marks:        {}\n", idx.estimated_marks), out);

        if (baseline_marks > 0 && baseline_est_bytes > 0)
        {
            UInt64 hypo_bytes = static_cast<UInt64>(
                static_cast<double>(baseline_est_bytes) * idx.estimated_marks / baseline_marks);
            double gb = static_cast<double>(hypo_bytes) / (1024.0 * 1024.0 * 1024.0);
            if (gb >= 0.01)
                writeString(fmt::format("  est_bytes:    {:.2f} GB\n", gb), out);
            else
                writeString(fmt::format("  est_bytes:    {} B\n", hypo_bytes), out);
        }

        writeString(fmt::format("  skip_ratio:   {:.1f}%\n", idx.skip_ratio * 100.0), out);
        writeCString("\n", out);

        writeString(fmt::format("Confidence: {}\n", idx.confidence), out);
        writeCString("\n", out);

        writeCString("Estimation:\n", out);
        writeString(fmt::format("  source:           {}\n", idx.estimate_source), out);

        String empirical_status_str;
        switch (idx.empirical_status)
        {
            case IndexResult::Ok: empirical_status_str = "ok"; break;
            case IndexResult::Timeout: empirical_status_str = "timeout"; break;
            case IndexResult::Unsupported: empirical_status_str = "unsupported"; break;
            case IndexResult::Disabled: empirical_status_str = "disabled"; break;
        }
        writeString(fmt::format("  empirical_status: {}\n", empirical_status_str), out);

        if (idx.empirical_status != IndexResult::Disabled)
        {
            writeString(fmt::format("  sampled_parts:    {} / {}\n", idx.sampled_parts, idx.total_parts), out);
            writeString(fmt::format("  sampled_marks:    {} / {}\n", idx.sampled_marks, idx.total_marks), out);
            writeString(fmt::format("  elapsed_ms:       {} / {}\n", idx.elapsed_ms, idx.budget_ms), out);
        }
        writeCString("\n", out);

        writeCString("Cost:\n", out);
        writeString(fmt::format("  storage_estimate_bytes:   {}\n", idx.storage_estimate_bytes), out);
        writeString(fmt::format("  cpu_check_cost_score:     {}\n", idx.cpu_check_cost_score), out);
        writeString(fmt::format("  maintenance_cost_score:   {}\n", idx.maintenance_cost_score), out);
        writeCString("\n", out);

        if (!idx.warnings.empty())
        {
            writeCString("Warnings:\n", out);
            for (const auto & warning : idx.warnings)
                writeString(fmt::format("  {}\n", warning), out);
            writeCString("\n", out);
        }
    }
}

}
