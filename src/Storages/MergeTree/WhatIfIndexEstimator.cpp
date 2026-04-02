#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Interpreters/Context.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <Common/Stopwatch.h>
#include <Core/Settings.h>

#include <fmt/format.h>

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

void collectReadSteps(const QueryPlan::Node * node, std::vector<ReadFromMergeTree *> & steps)
{
    if (!node)
        return;

    if (auto * read_step = dynamic_cast<ReadFromMergeTree *>(node->step.get()))
        steps.push_back(read_step);

    for (const auto & child : node->children)
        collectReadSteps(child, steps);
}

/// Estimate skip ratio from column statistics (row-level selectivity as upper bound)
bool tryEstimateWithStatistics(
    WhatIfIndexEstimator::IndexResult & result,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & parts,
    const ActionsDAG::Node * filter_node,
    ContextPtr context)
{
    auto metadata = read_step->getStorageMetadata();

    if (!metadata->hasStatistics())
        return false;

    if (parts.empty())
        return false;

    ConditionSelectivityEstimatorBuilder builder(context);
    bool has_any_stats = false;

    for (const auto & part : parts)
    {
        try
        {
            auto stats = part.data_part->loadStatistics();
            if (!stats.empty())
            {
                builder.markDataPart(part.data_part);
                for (const auto & [column_name, stat] : stats)
                    builder.addStatistics(column_name, stat);
                has_any_stats = true;
            }
        }
        catch (...)
        {
        }
    }

    if (!has_any_stats)
        return false;

    auto estimator = builder.getEstimator();
    if (!estimator)
        return false;

    auto profile = estimator->estimateRelationProfile(metadata, filter_node);
    auto unfiltered = estimator->estimateRelationProfile();
    if (unfiltered.rows == 0)
        return false;

    /// Row-level selectivity as upper bound for granule-level skip ratio
    double selectivity = std::min(1.0, static_cast<double>(profile.rows) / static_cast<double>(unfiltered.rows));
    result.skip_ratio = 1.0 - selectivity;
    result.estimated_marks = std::max<UInt64>(1, static_cast<UInt64>(static_cast<double>(analysis.selected_marks) * selectivity));
    result.estimated_parts = analysis.selected_parts;
    result.estimate_source = "statistical";
    return true;
}

/// Build the index in memory for baseline marks only and check each granule — 100% accurate
bool tryEstimateEmpirical(
    WhatIfIndexEstimator::IndexResult & result,
    const MergeTreeIndexPtr & index_helper,
    const MergeTreeIndexConditionPtr & condition,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    ContextPtr /* context */)
{
    const auto & data = read_step->getMergeTreeData();
    auto storage_snapshot = read_step->getStorageSnapshot();

    Names index_columns = index_helper->getColumnsRequiredForIndexCalc();
    if (index_columns.empty())
        return false;

    UInt64 total_index_granules = 0;
    UInt64 skipped_index_granules = 0;
    Stopwatch watch;

    const size_t skip_index_granularity = index_helper->index.granularity;

    for (const auto & part_with_ranges : saved_parts)
    {
        auto part = part_with_ranges.data_part;
        const auto & mark_ranges = part_with_ranges.ranges;

        if (mark_ranges.empty())
            continue;

        RangesInDataPart part_for_read(part, nullptr, 0, 0, mark_ranges);

        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            data,
            storage_snapshot,
            std::move(part_for_read),
            std::make_shared<AlterConversions>(),
            nullptr,
            index_columns,
            mark_ranges,
            std::make_shared<std::atomic<size_t>>(0),
            false,
            false,
            false);

        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        /// Sequential source produces one block per data granule;
        /// accumulate skip_index_granularity blocks per index granule
        auto aggregator = index_helper->createIndexAggregator();
        size_t data_granules_in_current = 0;

        Block block;
        while (executor.pull(block))
        {
            if (block.rows() == 0)
                continue;

            size_t pos = 0;
            aggregator->update(block, &pos, block.rows());
            ++data_granules_in_current;

            if (data_granules_in_current >= skip_index_granularity)
            {
                auto granule = aggregator->getGranuleAndReset();
                ++total_index_granules;
                if (!condition->mayBeTrueOnGranule(granule, {}))
                    ++skipped_index_granules;

                aggregator = index_helper->createIndexAggregator();
                data_granules_in_current = 0;
            }
        }

        if (!aggregator->empty())
        {
            auto granule = aggregator->getGranuleAndReset();
            ++total_index_granules;
            if (!condition->mayBeTrueOnGranule(granule, {}))
                ++skipped_index_granules;
        }
    }

    if (total_index_granules == 0)
        return false;

    result.skip_ratio = static_cast<double>(skipped_index_granules) / static_cast<double>(total_index_granules);
    result.estimated_marks = total_index_granules - skipped_index_granules;
    result.estimated_parts = analysis.selected_parts;
    result.estimate_source = "empirical";
    result.empirical_status = WhatIfIndexEstimator::IndexResult::Ok;
    result.sampled_parts = saved_parts.size();
    result.sampled_marks = analysis.selected_marks;
    result.elapsed_ms = watch.elapsedMilliseconds();

    return true;
}

/// Check applicability, then try empirical → statistical → applicability_only
WhatIfIndexEstimator::IndexResult evaluateIndex(
    const IndexDescription & index_desc,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    const WhatIfSettings & /* settings */,
    ContextPtr context)
{
    WhatIfIndexEstimator::IndexResult result;
    result.index_name = index_desc.name;
    result.index_type = index_desc.type;
    result.total_parts = analysis.selected_parts;
    result.total_marks = analysis.selected_marks;

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

    const auto & filter_dag = read_step->getFilterActionsDAG();
    if (!filter_dag)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Query has no filter predicate";
        return result;
    }

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

    /// Empirical first — build index in memory, check each granule
    if (tryEstimateEmpirical(result, index_helper, condition, read_step, analysis, saved_parts, context))
        return result;

    /// Fall back to column statistics
    result.empirical_status = WhatIfIndexEstimator::IndexResult::Unsupported;
    if (tryEstimateWithStatistics(result, read_step, analysis, saved_parts, filter_dag->getOutputs().front(), context))
        return result;

    /// No estimation available
    result.estimate_source = "applicability_only";
    result.estimated_marks = analysis.selected_marks;
    result.estimated_parts = analysis.selected_parts;
    result.skip_ratio = 0.0;

    return result;
}

} // anonymous namespace


WhatIfIndexEstimator::Result WhatIfIndexEstimator::run(
    const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings)
{
    auto settings = WhatIfSettings::fromAST(explain_settings);

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
    const auto & data = read_step->getMergeTreeData();

    /// Build pipeline to trigger filter pushdown and index analysis
    auto builder = plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(plan_context),
        BuildQueryPipelineSettings(plan_context));

    auto analysis_ptr = read_step->getAnalyzedResult();
    if (!analysis_ptr)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EXPLAIN WHATIF: query analysis result is not available");
    const auto & analysis = *analysis_ptr;

    /// Pipeline build moved parts_with_ranges out of the analysis;
    /// re-run selectRangesToRead to get a fresh copy of the filtered parts
    read_step->setAnalyzedResult(nullptr);
    auto fresh_analysis = read_step->selectRangesToRead();
    RangesInDataParts baseline_parts;
    if (fresh_analysis)
        baseline_parts = std::move(fresh_analysis->parts_with_ranges);

    Result result;
    result.database = data.getStorageID().getDatabaseName();
    result.table = data.getStorageID().getTableName();
    result.baseline_parts = analysis.selected_parts;
    result.baseline_marks = analysis.selected_marks;

    if (analysis.selected_rows > 0)
    {
        auto total_bytes = data.getTotalActiveSizeInBytes();
        auto total_rows = data.getTotalActiveSizeInRows();
        if (total_rows > 0)
            result.baseline_est_bytes = static_cast<UInt64>(
                static_cast<double>(total_bytes) / static_cast<double>(total_rows) * static_cast<double>(analysis.selected_rows));
    }

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

    for (const auto & index_desc : hypo_indexes)
    {
        auto index_result = evaluateIndex(index_desc, read_step, analysis, baseline_parts, settings, context);
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
                static_cast<double>(baseline_est_bytes) * static_cast<double>(idx.estimated_marks) / static_cast<double>(baseline_marks));
            double gb = static_cast<double>(hypo_bytes) / (1024.0 * 1024.0 * 1024.0);
            if (gb >= 0.01)
                writeString(fmt::format("  est_bytes:    {:.2f} GB\n", gb), out);
            else
                writeString(fmt::format("  est_bytes:    {} B\n", hypo_bytes), out);
        }

        writeString(fmt::format("  skip_ratio:   {:.1f}%\n", idx.skip_ratio * 100.0), out);
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

        if (idx.storage_estimate_bytes > 0 || !idx.cpu_check_cost_score.empty() || !idx.maintenance_cost_score.empty())
        {
            writeCString("Cost:\n", out);
            writeString(fmt::format("  storage_estimate_bytes:   {}\n", idx.storage_estimate_bytes), out);
            writeString(fmt::format("  cpu_check_cost_score:     {}\n", idx.cpu_check_cost_score), out);
            writeString(fmt::format("  maintenance_cost_score:   {}\n", idx.maintenance_cost_score), out);
            writeCString("\n", out);
        }

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
