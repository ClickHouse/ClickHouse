#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <Columns/ColumnSparse.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/Stopwatch.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>

#include <fmt/format.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool use_skip_indexes;
    extern const SettingsBool use_skip_indexes_if_final;
    extern const SettingsBool use_skip_indexes_for_disjunctions;
    extern const SettingsString ignore_data_skipping_indices;
    extern const SettingsString force_data_skipping_indices;
    extern const SettingsUInt64 merge_tree_min_rows_for_seek;
    extern const SettingsUInt64 merge_tree_min_bytes_for_seek;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
}

namespace ErrorCodes
{
    extern const int INDEX_NOT_USED;
    extern const int INVALID_SETTING_VALUE;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
    extern const int UNKNOWN_SETTING;
}

namespace
{

struct WhatIfSettings
{
    bool empirical = true;

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
            {
                if (change.value.getType() != Field::Types::UInt64)
                    throw Exception(
                        ErrorCodes::INVALID_SETTING_VALUE,
                        "Invalid type {} for setting '{}' in EXPLAIN WHATIF, expected an integer 0 or 1",
                        change.value.getTypeName(),
                        change.name);

                auto value = change.value.safeGet<UInt64>();
                if (value > 1)
                    throw Exception(
                        ErrorCodes::INVALID_SETTING_VALUE,
                        "Invalid value {} for setting '{}' in EXPLAIN WHATIF, expected 0 or 1",
                        value,
                        change.name);

                result.empirical = value != 0;
            }
            else
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_SETTING,
                    "Unknown setting \"{}\" for EXPLAIN WHATIF query. Supported settings: empirical",
                    change.name);
            }
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

/// Strip nested SELECT `SETTINGS` that WHATIF must control: `force_data_skipping_indices`
/// (collected into `removed_force` for later re-validation, else it throws `INDEX_NOT_USED`
/// on the index-less baseline) and `enable_parallel_replicas` with its alias (the estimate
/// is session-local and must stay on a local plan).
void stripWhatIfControlledSettings(IAST * node, std::vector<String> & removed_force)
{
    if (!node)
        return;

    if (auto * select = node->as<ASTSelectQuery>())
    {
        if (auto settings_ast = select->settings())
        {
            if (auto * set_query = settings_ast->as<ASTSetQuery>())
                std::erase_if(set_query->changes, [&](const auto & change)
                {
                    if (change.name == "force_data_skipping_indices")
                    {
                        removed_force.push_back(change.value.template safeGet<String>());
                        return true;
                    }
                    return change.name == "enable_parallel_replicas"
                        || change.name == "allow_experimental_parallel_reading_from_replicas";
                });
        }
    }

    for (const auto & child : node->children)
        stripWhatIfControlledSettings(child.get(), removed_force);
}

void collectFilterInputColumns(const ActionsDAG::Node * node, NameSet & out)
{
    if (!node)
        return;
    if (node->type == ActionsDAG::ActionType::INPUT)
        out.insert(node->result_name);
    for (const auto * child : node->children)
        collectFilterInputColumns(child, out);
}

/// True if an `or` has the candidate's column in one branch and a non-index column
/// in another. A real read can combine the candidate with an existing skip index on
/// that other column (`use_skip_indexes_for_disjunctions`); the estimator cannot, so
/// these cases are reported unsupported rather than understating the benefit.
bool disjunctionMixesIndexAndOtherColumns(const ActionsDAG::Node * node, const NameSet & index_columns)
{
    if (!node)
        return false;

    if (node->type == ActionsDAG::ActionType::FUNCTION
        && node->function_base
        && node->function_base->getName() == "or")
    {
        bool branch_has_index = false;
        bool branch_has_other = false;
        for (const auto * child : node->children)
        {
            NameSet cols;
            collectFilterInputColumns(child, cols);
            for (const auto & col : cols)
            {
                if (index_columns.contains(col))
                    branch_has_index = true;
                else
                    branch_has_other = true;
            }
        }
        if (branch_has_index && branch_has_other)
            return true;
    }

    for (const auto * child : node->children)
        if (disjunctionMixesIndexAndOtherColumns(child, index_columns))
            return true;
    return false;
}

/// Estimate skip ratio from column statistics (row-level selectivity as upper bound)
bool tryEstimateWithStatistics(
    WhatIfIndexEstimator::IndexResult & result,
    const MergeTreeIndexPtr & index_helper,
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

    /// Only estimate when the filter is purely on the index's columns, otherwise
    /// other columns' selectivity would inflate the hypothetical index's skip ratio.
    NameSet index_columns_set;
    for (const auto & col : index_helper->getColumnsRequiredForIndexCalc())
        index_columns_set.insert(col);

    NameSet filter_input_columns;
    collectFilterInputColumns(filter_node, filter_input_columns);

    for (const auto & col : filter_input_columns)
        if (!index_columns_set.contains(col))
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
        catch (...) /// Ok — statistical estimation is best-effort
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
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

/// Build the index in memory for baseline marks only and check each granule — accurate
bool tryEstimateEmpirical(
    WhatIfIndexEstimator::IndexResult & result,
    const MergeTreeIndexPtr & index_helper,
    const MergeTreeIndexConditionPtr & condition,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & storage_snapshot = read_step->getStorageSnapshot();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    Names index_columns = index_helper->getColumnsRequiredForIndexCalc();
    if (index_columns.empty())
        return false;

    /// The empirical counter treats each surviving granule independently. With
    /// non-zero seek-gap settings the real read coalesces nearby ranges, so the
    /// estimate would diverge — fall back to statistical/applicability instead.
    if (context->getSettingsRef()[Setting::merge_tree_min_rows_for_seek] != 0
        || context->getSettingsRef()[Setting::merge_tree_min_bytes_for_seek] != 0)
        return false;

    UInt64 total_data_granules = 0;
    UInt64 skipped_data_granules = 0;
    Stopwatch watch;

    /// The whole-part scan is not the normal read pipeline, so enforce the query's
    /// read limits explicitly (max_execution_time is handled by the process-list element).
    const auto & limit_settings = context->getSettingsRef();
    const SizeLimits read_limits(
        limit_settings[Setting::max_rows_to_read],
        limit_settings[Setting::max_bytes_to_read],
        limit_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    const size_t skip_index_granularity = index_helper->index.granularity;
    auto index_expression = index_helper->index.expression;

    for (const auto & part_with_ranges : saved_parts)
    {
        auto part = part_with_ranges.data_part;
        const auto & mark_ranges = part_with_ranges.ranges;

        if (mark_ranges.empty())
            continue;

        const auto & part_index_granularity = part->index_granularity;
        const size_t total_marks = part_index_granularity->getMarksCountWithoutFinal();

        std::vector<bool> in_baseline(part->getMarksCount(), false);
        for (const auto & range : mark_ranges)
            for (size_t m = range.begin; m < range.end && m < in_baseline.size(); ++m)
                in_baseline[m] = true;

        /// Read the whole part: non-zero-start mark_ranges trip a LOGICAL_ERROR
        /// in MergeTreeSequentialSource on adaptive granularity. TODO: fix that
        /// and read only baseline-aligned ranges
        RangesInDataPart part_for_read(part);

        /// Apply patch parts / on-the-fly mutations so the empirical scan sees
        /// the same values a real read would, instead of stale raw part data
        auto alter_conversions = mutations_snapshot
            ? MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context)
            : std::make_shared<AlterConversions>();

        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            data,
            storage_snapshot,
            std::move(part_for_read),
            std::move(alter_conversions),
            nullptr,
            index_columns,
            std::nullopt,
            std::make_shared<std::atomic<size_t>>(0),
            false,
            false,
            false);

        QueryPipeline pipeline(std::move(pipe));
        /// Account the scan against the query so max_execution_time / max_*_to_read apply.
        pipeline.setProcessListElement(context->getProcessListElement());
        pipeline.setProgressCallback(context->getProgressCallback());
        PullingPipelineExecutor executor(pipeline);

        auto aggregator = index_helper->createIndexAggregator();
        size_t current_mark = 0;
        size_t rows_remaining_in_mark = total_marks > 0 ? part_index_granularity->getMarkRows(0) : 0;
        size_t data_granules_in_window = 0;
        size_t baseline_marks_in_window = 0;

        auto flush_window = [&]
        {
            if (baseline_marks_in_window == 0)
                return;
            auto granule = aggregator->getGranuleAndReset();
            total_data_granules += baseline_marks_in_window;
            if (!condition->mayBeTrueOnGranule(granule, {}))
                skipped_data_granules += baseline_marks_in_window;
        };

        auto on_mark_finished = [&]
        {
            ++data_granules_in_window;
            if (current_mark < in_baseline.size() && in_baseline[current_mark])
                ++baseline_marks_in_window;

            if (data_granules_in_window >= skip_index_granularity)
            {
                flush_window();
                aggregator = index_helper->createIndexAggregator();
                data_granules_in_window = 0;
                baseline_marks_in_window = 0;
            }

            ++current_mark;
            rows_remaining_in_mark = current_mark < total_marks
                ? part_index_granularity->getMarkRows(current_mark)
                : 0;
        };

        Block block;
        while (executor.pull(block))
        {
            if (block.rows() == 0)
                continue;

            total_rows_read += block.rows();
            total_bytes_read += block.bytes();
            /// `throw` mode raises here; `break` mode returns false (the scan would be
            /// partial, so don't report it as a complete empirical estimate — fall back).
            if (!read_limits.check(total_rows_read, total_bytes_read, "rows or bytes to read",
                                   ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
                return false;

            /// Evaluate the index expression so the aggregator sees what a real
            /// MATERIALIZE INDEX would see (e.g. lower(s) instead of raw s)
            if (index_expression)
                index_expression->execute(block);

            /// Index aggregators require full columns; sparse-serialized parts
            /// would otherwise trip `getRawData` (matches the real index writer).
            for (auto & column : block)
                column.column = recursiveRemoveSparse(column.column);

            size_t pos = 0;
            aggregator->update(block, &pos, block.rows());

            if (block.rows() <= rows_remaining_in_mark)
                rows_remaining_in_mark -= block.rows();
            else
                rows_remaining_in_mark = 0;

            if (rows_remaining_in_mark == 0 && current_mark < total_marks)
                on_mark_finished();
        }

        if (!aggregator->empty())
            flush_window();
    }

    if (total_data_granules == 0)
        return false;

    result.skip_ratio = static_cast<double>(skipped_data_granules) / static_cast<double>(total_data_granules);
    result.estimated_marks = total_data_granules - skipped_data_granules;
    result.estimated_parts = analysis.selected_parts;
    result.estimate_source = "empirical";
    result.empirical_status = WhatIfIndexEstimator::IndexResult::Ok;
    result.sampled_parts = analysis.selected_parts;
    result.sampled_marks = analysis.selected_marks;
    result.elapsed_us = watch.elapsedMicroseconds();

    return true;
}

/// Check applicability, then try empirical → statistical → applicability_only
WhatIfIndexEstimator::IndexResult evaluateIndex(
    const IndexDescription & index_desc,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    const WhatIfSettings & settings,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();

    WhatIfIndexEstimator::IndexResult result;
    result.index_name = index_desc.name;
    result.index_type = index_desc.type;
    result.total_parts = data.getActivePartsCount();
    result.total_marks = data.getTotalMarksCount();

    /// `context` here is the effective query context (after inner SELECT `SETTINGS`),
    /// so these gates mirror what a real read sees.
    if (!context->getSettingsRef()[Setting::use_skip_indexes])
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Skip indexes are disabled by `use_skip_indexes = 0`";
        return result;
    }

    /// Mirror the real read's `ignore_data_skipping_indices`: parse when the setting is
    /// changed (an empty value throws `CANNOT_PARSE_TEXT`, exactly as a real read), then
    /// drop the candidate if its exact name is listed.
    {
        const auto & user_settings = context->getSettingsRef();
        if (user_settings[Setting::ignore_data_skipping_indices].changed)
        {
            auto ignored_names = parseIdentifiersOrStringLiteralsToSet(
                user_settings[Setting::ignore_data_skipping_indices].toString(), user_settings);
            if (ignored_names.contains(index_desc.name))
            {
                result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
                result.not_applicable_reason = "Index '" + index_desc.name + "' is listed in `ignore_data_skipping_indices`";
                return result;
            }
        }
    }

    /// Rebuild against current metadata so a schema change since CREATE either
    /// yields a fresh descriptor or surfaces as `not_applicable`.
    IndexDescription fresh_index_desc;
    try
    {
        auto metadata = read_step->getStorageMetadata();
        fresh_index_desc = IndexDescription::getIndexFromAST(
            index_desc.definition_ast,
            metadata->getColumns(),
            /* is_implicitly_created = */ false,
            /* escape_filenames = */ true,
            context);
    }
    catch (...)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Hypothetical index no longer matches the current table schema: "
            + getCurrentExceptionMessage(false);
        return result;
    }

    MergeTreeIndexPtr index_helper;
    try
    {
        index_helper = MergeTreeIndexFactory::instance().get(fresh_index_desc);
    }
    catch (...)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Failed to create index: " + getCurrentExceptionMessage(false);
        return result;
    }

    /// The descriptor was authorized at CREATE, but the empirical/statistical scan reads
    /// these columns now — re-check column-level SELECT against the current grants so a
    /// grant revoked since CREATE denies the estimate, exactly as a real read would.
    context->checkAccess(AccessType::SELECT, data.getStorageID(), index_helper->getColumnsRequiredForIndexCalc());

    /// Text indexes need a tokenized block layout the empirical pipeline doesn't build. That's a TODO
    if (index_desc.type == "text")
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "EXPLAIN WHATIF does not yet support empirical estimation for text indexes";
        return result;
    }

    const auto & filter_dag = read_step->getFilterActionsDAG();
    if (!filter_dag)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Query has no filter predicate";
        return result;
    }

    if (context->getSettingsRef()[Setting::use_skip_indexes_for_disjunctions])
    {
        NameSet index_columns_set;
        for (const auto & col : index_helper->getColumnsRequiredForIndexCalc())
            index_columns_set.insert(col);
        if (disjunctionMixesIndexAndOtherColumns(filter_dag->getOutputs().front(), index_columns_set))
        {
            result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
            result.not_applicable_reason = "EXPLAIN WHATIF does not model combining the candidate with an existing "
                                           "skip index under a disjunction (use_skip_indexes_for_disjunctions)";
            return result;
        }
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
    if (settings.empirical)
    {
        if (tryEstimateEmpirical(result, index_helper, condition, read_step, analysis, saved_parts, context))
            return result;
        result.empirical_status = WhatIfIndexEstimator::IndexResult::Unsupported;
    }
    else
    {
        result.empirical_status = WhatIfIndexEstimator::IndexResult::Disabled;
    }

    /// Fall back to column statistics
    if (tryEstimateWithStatistics(result, index_helper, read_step, analysis, saved_parts, filter_dag->getOutputs().front(), context))
        return result;

    /// No estimation available
    result.estimate_source = "applicability_only";
    result.estimated_marks = analysis.selected_marks;
    result.estimated_parts = analysis.selected_parts;
    result.skip_ratio = 0.0;

    return result;
}

}


WhatIfIndexEstimator::Result WhatIfIndexEstimator::run(
    const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings)
{
    auto settings = WhatIfSettings::fromAST(explain_settings);

    /// Lock down inner `SETTINGS` so baseline contract stays deterministic
    auto local_context = Context::createCopy(context);
    local_context->setSetting("enable_parallel_replicas", Field{UInt64{0}});
    local_context->setSetting("use_skip_indexes_on_data_read", Field{UInt64{0}});
    /// Capture the forced skip-index names (session + inner-query), then drop them for
    /// baseline planning — the contract is re-validated after candidates are evaluated.
    std::vector<String> forced_strings;
    if (context->getSettingsRef()[Setting::force_data_skipping_indices].changed)
        forced_strings.push_back(context->getSettingsRef()[Setting::force_data_skipping_indices]);
    local_context->resetSettingsToDefaultValue({"force_data_skipping_indices"});

    auto select_query_copy = select_query->clone();
    stripWhatIfControlledSettings(select_query_copy.get(), forced_strings);

    SelectQueryOptions query_options;
    query_options.setExplain();
    QueryPlan plan;
    ContextPtr plan_context = local_context;

    if (local_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(select_query_copy, local_context, query_options);
        plan_context = interpreter.getContext();
        plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter(select_query_copy, local_context, query_options);
        plan_context = interpreter.getContext();
        interpreter.buildQueryPlan(plan);
    }

    /// Build pipeline to trigger filter pushdown and index analysis
    auto builder = plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(plan_context),
        BuildQueryPipelineSettings(plan_context));

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

    /// FINAL prevents skip indexes from pruning granules (the merge needs every
    /// granule), so a hypothetical index can't help. Report not_applicable
    const bool query_with_final = read_step->isQueryWithFinal();

    /// Mirror the effective skip-index state of a real read (`ReadFromMergeTree`):
    /// `use_skip_indexes` on the effective query context (after inner SELECT `SETTINGS`),
    /// and disabled under FINAL unless `use_skip_indexes_if_final` is set.
    const auto & effective_settings = plan_context->getSettingsRef();
    const bool effective_use_skip_indexes = effective_settings[Setting::use_skip_indexes]
        && !(query_with_final && !effective_settings[Setting::use_skip_indexes_if_final]);

    /// The real read only honors `force_data_skipping_indices` when skip indexes are
    /// effectively enabled; otherwise the forced list is ignored and not even parsed.
    NameSet forced_indices;
    if (effective_use_skip_indexes)
    {
        /// Parse every changed value (including ""): the parser throws `CANNOT_PARSE_TEXT`
        /// on an unparseable force list exactly as a real read does.
        for (const auto & forced_string : forced_strings)
            for (const auto & name : parseIdentifiersOrStringLiteralsToSet(forced_string, effective_settings))
                forced_indices.insert(name);
    }

    auto analysis_ptr = read_step->getAnalyzedResult();
    if (!analysis_ptr)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EXPLAIN WHATIF: query analysis result is not available");
    const auto & analysis = *analysis_ptr;

    /// A parent-table hypothetical index isn't materialized on projection parts,
    /// so applying it to a projection-driven plan would mis-attribute pruning
    if (analysis.readFromProjection())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "EXPLAIN WHATIF is not supported when the query is served from a projection");

    /// Pipeline build moved parts_with_ranges out of the analysis
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

    /// Honor `force_data_skipping_indices`: each forced name must be a useful real
    /// skip index (from the baseline analysis) or an applicable hypothetical candidate,
    /// otherwise throw `INDEX_NOT_USED` exactly as a real read would.
    auto validate_forced_indices = [&]
    {
        if (forced_indices.empty())
            return;
        NameSet satisfied;
        for (const auto & stat : analysis.index_stats)
            if (stat.type == ReadFromMergeTree::IndexType::Skip)
                satisfied.insert(stat.name);
        for (const auto & idx : result.index_results)
            if (idx.status == IndexResult::Applicable)
                satisfied.insert(idx.index_name);
        for (const auto & name : forced_indices)
            if (!satisfied.contains(name))
                throw Exception(
                    ErrorCodes::INDEX_NOT_USED,
                    "Index {} is not used and setting 'force_data_skipping_indices' contains it",
                    backQuoteIfNeed(name));
    };

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
        validate_forced_indices();
        return result;
    }

    for (const auto & index_desc : hypo_indexes)
    {
        if (query_with_final)
        {
            IndexResult r;
            r.index_name = index_desc.name;
            r.index_type = index_desc.type;
            r.status = IndexResult::NotApplicable;
            r.not_applicable_reason = "EXPLAIN WHATIF cannot accurately model skip-index pruning under FINAL "
                                      "(PrimaryKeyExpand may re-include granules selected by skip indexes)";
            result.index_results.push_back(std::move(r));
            continue;
        }

        auto index_result = evaluateIndex(index_desc, read_step, analysis, baseline_parts, settings, plan_context);
        result.index_results.push_back(std::move(index_result));
    }

    validate_forced_indices();
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

        if (idx.empirical_status == IndexResult::Ok)
        {
            writeString(fmt::format("  sampled_parts:    {} / {}\n", idx.sampled_parts, idx.total_parts), out);
            writeString(fmt::format("  sampled_marks:    {} / {}\n", idx.sampled_marks, idx.total_marks), out);
            writeString(fmt::format("  elapsed_us:       {}\n", idx.elapsed_us), out);
        }
        writeCString("\n", out);

        if (idx.storage_estimate_bytes > 0)
        {
            writeCString("Cost:\n", out);
            writeString(fmt::format("  storage_estimate_bytes:   {}\n", idx.storage_estimate_bytes), out);
            writeCString("\n", out);
        }
    }
}

}
