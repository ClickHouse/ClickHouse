#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/JoinedTables.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <Columns/ColumnSparse.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
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

/// Resolve the source table from the query
StoragePtr tryResolveSingleTable(const ASTPtr & query, const ContextPtr & context)
{
    const auto * union_query = query->as<ASTSelectWithUnionQuery>();
    if (!union_query || !union_query->list_of_selects || union_query->list_of_selects->children.size() != 1)
        return nullptr;
    const auto * select = union_query->list_of_selects->children.front()->as<ASTSelectQuery>();
    if (!select)
        return nullptr;
    return JoinedTables(context, *select).getLeftTableStorage();
}

/// Empty table, nothing to scan, just mark every candidate not-applicable
WhatIfIndexEstimator::Result buildEmptyTableResult(const MergeTreeData & data, const HypotheticalIndexStore & store)
{
    WhatIfIndexEstimator::Result result;
    result.database = data.getStorageID().getDatabaseName();
    result.table = data.getStorageID().getTableName();
    for (const auto & index_desc : store.getForTable(data.getStorageID()))
    {
        WhatIfIndexEstimator::IndexResult r;
        r.index_name = index_desc.name;
        r.index_type = index_desc.type;
        r.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        r.not_applicable_reason = "Table is empty, so there is no data to estimate a benefit";
        result.index_results.push_back(std::move(r));
    }
    if (result.index_results.empty())
    {
        WhatIfIndexEstimator::IndexResult none;
        none.index_name = "(none)";
        none.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        none.not_applicable_reason = "No hypothetical indexes defined for this table.";
        result.index_results.push_back(std::move(none));
    }
    return result;
}

/// Drop the inner-SELECT settings we pin for a deterministic local baseline
/// `force_data_skipping_indices` is collected into `removed_force` so we can re-check it later
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
                    /// keep the estimate local, use_skip_indexes_on_data_read: avoid over-reporting marks
                    return change.name == "enable_parallel_replicas"
                        || change.name == "allow_experimental_parallel_reading_from_replicas"
                        || change.name == "use_skip_indexes_on_data_read";
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

/// TODO(yariks5s): an OR with the candidate's column on one side and another column on the other
/// The real read can combine them (use_skip_indexes_for_disjunctions) we can't, so we bail on those
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

    /// Only when filter touches just the index's columns, else other columns'
    /// selectivity leaks into the skip ratio
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
        catch (const Exception &) /// Ok — statistical estimation is best-effort
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
    result.estimate_source = "statistical";
    return true;
}

/// Expand each baseline mark range to the full skip-index windows it touches (a window is
/// `granularity` data granules) and merge them into ranges
MarkRanges skipIndexWindowsOverlapping(const MarkRanges & baseline, size_t granularity, size_t total_marks)
{
    MarkRanges windows;
    if (granularity == 0 || total_marks == 0)
        return windows;

    for (const auto & range : baseline)
    {
        const size_t last = std::min(range.end, total_marks);
        if (range.begin >= last)
            continue;

        const size_t begin = range.begin / granularity * granularity;
        const size_t end = std::min((last - 1) / granularity * granularity + granularity, total_marks);
        if (!windows.empty() && begin <= windows.back().end)
            windows.back().end = std::max(windows.back().end, end);
        else
            windows.emplace_back(begin, end);
    }
    return windows;
}

/// Build the candidate index in memory over the baseline marks and check each granule
bool tryEstimateEmpirical(
    WhatIfIndexEstimator::IndexResult & result,
    const MergeTreeIndexPtr & index_helper,
    const MergeTreeIndexConditionPtr & condition,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    std::vector<UInt8> * surviving_marks,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & storage_snapshot = read_step->getStorageSnapshot();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    Names index_columns = index_helper->getColumnsRequiredForIndexCalc();
    if (index_columns.empty())
        return false;

    /// With non-zero seek gaps a real read coalesces ranges, so our per-granule count would diverge
    if (context->getSettingsRef()[Setting::merge_tree_min_rows_for_seek] != 0
        || context->getSettingsRef()[Setting::merge_tree_min_bytes_for_seek] != 0)
        return false;

    UInt64 total_data_granules = 0;
    UInt64 skipped_data_granules = 0;
    Stopwatch watch;

    /// The whole-part scan is not the normal read pipeline, so enforce the query's
    /// read limits explicitly (max_execution_time is handled by the process-list element)
    const auto & limit_settings = context->getSettingsRef();
    const SizeLimits read_limits(
        limit_settings[Setting::max_rows_to_read],
        limit_settings[Setting::max_bytes_to_read],
        limit_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    const size_t skip_index_granularity = index_helper->index.granularity;
    auto index_expression = index_helper->index.expression;

    /// Position of the next baseline mark, gives every candidate's bitmap the same coordinates
    size_t baseline_mark_pos = 0;

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

        /// Read only the skip-index windows overlapping the baseline instead of the whole part
        const MarkRanges read_ranges = skipIndexWindowsOverlapping(mark_ranges, skip_index_granularity, total_marks);
        if (read_ranges.empty())
            continue;

        /// Apply patch parts / on-the-fly mutations so we see the up-to-date values
        auto alter_conversions = mutations_snapshot
            ? MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context)
            : std::make_shared<AlterConversions>();

        /// aggregate each skip-index granule and count how many
        /// baseline granules the candidate would skip. Returns false if a read limit was hit
        auto scan_range = [&](const MarkRange & read_range) -> bool
        {
            RangesInDataPart part_for_read(part);

            Pipe pipe = createMergeTreeSequentialSource(
                MergeTreeSequentialSourceType::Merge,
                data,
                storage_snapshot,
                std::move(part_for_read),
                alter_conversions,
                nullptr,
                index_columns,
                MarkRanges{read_range},
                std::make_shared<std::atomic<size_t>>(0),
                false,
                false,
                false);

            /// Apply the query's execution-speed limits here too (size is the explicit check below)
            if (auto query_limits = read_step->getQueryInfo().storage_limits)
            {
                auto speed_limits = std::make_shared<StorageLimitsList>(*query_limits);
                for (auto & entry : *speed_limits)
                {
                    entry.local_limits.size_limits = {};
                    entry.leaf_limits = {};
                    entry.local_limits.speed_limits.max_execution_time = {};
                }
                for (const auto & processor : pipe.getProcessors())
                    processor->setStorageLimits(speed_limits);
            }

            QueryPipeline pipeline(std::move(pipe));
            /// Tie the scan to the query so quota / speed / time limits apply
            pipeline.setProcessListElement(context->getProcessListElement());
            pipeline.setProgressCallback(context->getProgressCallback());
            pipeline.setQuota(context->getQuota());
            PullingPipelineExecutor executor(pipeline);

            auto aggregator = index_helper->createIndexAggregator();
            size_t current_mark = read_range.begin;
            size_t rows_remaining_in_mark = current_mark < total_marks
                ? part_index_granularity->getMarkRows(current_mark)
                : 0;
            size_t data_granules_in_window = 0;
            size_t baseline_marks_in_window = 0;
            std::vector<size_t> window_baseline_marks;

            auto flush_window = [&]
            {
                if (baseline_marks_in_window == 0)
                    return;
                auto granule = aggregator->getGranuleAndReset();
                total_data_granules += baseline_marks_in_window;
                if (!condition->mayBeTrueOnGranule(granule, {}))
                    skipped_data_granules += baseline_marks_in_window;
                else if (surviving_marks)
                    for (size_t pos : window_baseline_marks)
                        (*surviving_marks)[pos] = 1;
            };

            auto on_mark_finished = [&]
            {
                ++data_granules_in_window;
                if (current_mark < in_baseline.size() && in_baseline[current_mark])
                {
                    ++baseline_marks_in_window;
                    if (surviving_marks)
                        window_baseline_marks.push_back(baseline_mark_pos++);
                }

                if (data_granules_in_window >= skip_index_granularity)
                {
                    flush_window();
                    aggregator = index_helper->createIndexAggregator();
                    data_granules_in_window = 0;
                    baseline_marks_in_window = 0;
                    window_baseline_marks.clear();
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
                /// throw mode raises here; break mode returns false so we don't pass off a partial scan as done
                if (!read_limits.check(total_rows_read, total_bytes_read, "rows or bytes to read",
                                       ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
                    return false;

                /// Evaluate the index expression so the aggregator sees what a real
                /// MATERIALIZE INDEX would see (lower(s) instead of raw s)
                if (index_expression)
                    index_expression->execute(block);

                /// Index aggregators require full columns, sparse-serialized parts
                /// would otherwise trip getRawData (matches the real index writer)
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
            return true;
        };

        for (const auto & read_range : read_ranges)
            if (!scan_range(read_range))
                return false;
    }

    if (total_data_granules == 0)
        return false;

    result.skip_ratio = static_cast<double>(skipped_data_granules) / static_cast<double>(total_data_granules);
    result.estimated_marks = total_data_granules - skipped_data_granules;
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
    std::vector<UInt8> * surviving_marks,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();

    WhatIfIndexEstimator::IndexResult result;
    result.index_name = index_desc.name;
    result.index_type = index_desc.type;
    result.total_parts = data.getActivePartsCount();
    result.total_marks = data.getTotalMarksCount();

    /// `context` already has the inner-SELECT settings applied, so these checks match a real read
    if (!context->getSettingsRef()[Setting::use_skip_indexes])
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Skip indexes are disabled by `use_skip_indexes = 0`";
        return result;
    }

    /// parse ignore_data_skipping_indices when changed (an empty value throws
    /// CANNOT_PARSE_TEXT) and skip the candidate if it's named
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

    /// Rebuild from current metadata, so a schema change since CREATE turns into not_applicable
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
    catch (const Exception &)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Hypothetical index no longer matches the current table schema: "
            + getCurrentExceptionMessage(false);
        return result;
    }

    MergeTreeIndexPtr index_helper;
    try
    {
        index_helper = MergeTreeIndexFactory::instance().get(read_step->getStorageMetadata(),fresh_index_desc, *data.getSettings());
    }
    catch (const Exception &)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Failed to create index: " + getCurrentExceptionMessage(false);
        return result;
    }

    /// CREATE checked these columns, but the scan reads them now, so re-check SELECT against
    /// current grants, a grant revoked since CREATE should deny the estimate
    context->checkAccess(AccessType::SELECT, data.getStorageID(), index_helper->getColumnsRequiredForIndexCalc());

    /// TODO(yariks5s): text indexes need a tokenized block layout the empirical pipeline doesn't build
    /// also, add a whitelist index types so the logic will not be broken by a new type
    if (index_helper->isTextIndex())
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

    /// Canonicalize the predicate (push NOT down, drop aliases) the way the read path does,
    /// so the condition can pick up a standalone conjunct out of a mixed AND/OR
    ActionsDAGWithInversionPushDown predicate_dag(filter_dag->getOutputs().front(), context, /* boolean_context */ true);
    const ActionsDAG::Node * predicate = predicate_dag.predicate;

    MergeTreeIndexConditionPtr condition;
    try
    {
        condition = index_helper->createIndexCondition(predicate, context);
    }
    catch (const Exception &)
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        result.not_applicable_reason = "Cannot build index condition: " + getCurrentExceptionMessage(false);
        return result;
    }

    /// Let the condition decide first, a standalone conjunct can still be usable inside a mixed
    /// OR. Only fall through to the disjunction case when it can't prune on its own
    if (!condition || condition->alwaysUnknownOrTrue())
    {
        result.status = WhatIfIndexEstimator::IndexResult::NotApplicable;
        if (predicate && context->getSettingsRef()[Setting::use_skip_indexes_for_disjunctions])
        {
            NameSet index_columns_set;
            for (const auto & col : index_helper->getColumnsRequiredForIndexCalc())
                index_columns_set.insert(col);
            if (disjunctionMixesIndexAndOtherColumns(predicate, index_columns_set))
            {
                result.not_applicable_reason = "EXPLAIN WHATIF does not model combining the candidate with an existing "
                                               "skip index under a disjunction (use_skip_indexes_for_disjunctions)";
                return result;
            }
        }
        result.not_applicable_reason = "Index cannot filter this predicate (always unknown or true)";
        return result;
    }

    result.status = WhatIfIndexEstimator::IndexResult::Applicable;

    if (settings.empirical)
    {
        if (tryEstimateEmpirical(result, index_helper, condition, read_step, analysis, saved_parts, surviving_marks, context))
            return result;
        result.empirical_status = WhatIfIndexEstimator::IndexResult::Unsupported;
    }
    else
    {
        result.empirical_status = WhatIfIndexEstimator::IndexResult::Disabled;
    }

    if (tryEstimateWithStatistics(result, index_helper, read_step, analysis, saved_parts, predicate, context))
        return result;

    result.estimate_source = "applicability_only";
    result.estimated_marks = analysis.selected_marks;
    result.skip_ratio = 0.0;

    return result;
}

}


WhatIfIndexEstimator::Result WhatIfIndexEstimator::run(
    const ASTPtr & select_query, ContextPtr context, const ASTPtr & explain_settings)
{
    auto settings = WhatIfSettings::fromAST(explain_settings);

    /// Lock down inner `SETTINGS` so baseline stays deterministic
    auto local_context = Context::createCopy(context);
    local_context->setSetting("enable_parallel_replicas", Field{UInt64{0}});
    local_context->setSetting("use_skip_indexes_on_data_read", Field{UInt64{0}});
    /// Grab the forced index names, drop them for baseline planning, re-check them at the end
    local_context->resetSettingsToDefaultValue({"force_data_skipping_indices"});

    auto select_query_copy = select_query->clone();
    std::vector<String> forced_strings;
    stripWhatIfControlledSettings(select_query_copy.get(), forced_strings);

    if (forced_strings.empty() && context->getSettingsRef()[Setting::force_data_skipping_indices].changed)
        forced_strings.push_back(context->getSettingsRef()[Setting::force_data_skipping_indices]);

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

    plan.optimize(QueryPlanOptimizationSettings(plan_context));

    std::vector<ReadFromMergeTree *> read_steps;
    collectReadSteps(plan.getRootNode(), read_steps);

    if (read_steps.empty())
    {
        /// Empty table -> ReadNothing, no read step, report a zero baseline
        auto storage = tryResolveSingleTable(select_query, local_context);
        if (const auto * mt = dynamic_cast<const MergeTreeData *>(storage.get());
            mt && mt->getActivePartsCount() == 0)
            return buildEmptyTableResult(*mt, local_context->getHypotheticalIndexStore());

        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "EXPLAIN WHATIF requires a query reading from a MergeTree family table");
    }

    if (read_steps.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "EXPLAIN WHATIF currently supports only single-table queries (found {} read steps)",
            read_steps.size());

    auto * read_step = read_steps[0];
    const auto & data = read_step->getMergeTreeData();

    /// TODO(yariks5s): FINAL prevents skip indexes from pruning granules (the merge needs every
    /// granule), so a hypothetical index can't help. Report not_applicable
    const bool query_with_final = read_step->isQueryWithFinal();

    /// Mirror a real read's skip-index state, use_skip_indexes, off under FINAL unless use_skip_indexes_if_final
    const auto & effective_settings = plan_context->getSettingsRef();
    const bool effective_use_skip_indexes = effective_settings[Setting::use_skip_indexes]
        && !(query_with_final && !effective_settings[Setting::use_skip_indexes_if_final]);

    /// force_data_skipping_indices only matters when skip indexes are actually on
    NameSet forced_indices;
    if (effective_use_skip_indexes)
    {
        /// Parse every changed value, incl. "": a bad list throws CANNOT_PARSE_TEXT, same as a real read
        for (const auto & forced_string : forced_strings)
            for (const auto & name : parseIdentifiersOrStringLiteralsToSet(forced_string, effective_settings))
                forced_indices.insert(name);
    }

    auto analysis_ptr = read_step->getAnalyzedResult();
    if (!analysis_ptr)
        analysis_ptr = read_step->selectRangesToRead();
    if (!analysis_ptr)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EXPLAIN WHATIF: query analysis result is not available");
    const auto & analysis = *analysis_ptr;

    /// Can't model a projection-served read, the hypothetical index isn't on projection parts
    if (analysis.readFromProjection())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "EXPLAIN WHATIF is not supported when the query is served from a projection");

    const RangesInDataParts & baseline_parts = analysis.parts_with_ranges;

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

    /// Every forced name must be a useful real skip index or an applicable candidate,
    /// otherwise throw INDEX_NOT_USED like a real read
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

    /// Only track per-candidate surviving marks when a combined row could actually be produced
    const bool want_combined = settings.empirical && !query_with_final
        && hypo_indexes.size() >= 2 && result.baseline_marks > 0;

    std::vector<UInt8> combined_surviving_marks;
    bool combined_started = false;
    std::vector<String> combined_names;
    UInt64 combined_total_parts = 0;
    UInt64 combined_total_marks = 0;

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

        std::vector<UInt8> surviving_marks;
        if (want_combined)
            surviving_marks.assign(result.baseline_marks, 0);
        auto index_result = evaluateIndex(
            index_desc, read_step, analysis, baseline_parts, settings, want_combined ? &surviving_marks : nullptr, plan_context);

        /// push empirically-evaluated candidates in a per-mark survival set we can intersect
        if (want_combined && index_result.status == IndexResult::Applicable && index_result.estimate_source == "empirical")
        {
            if (!combined_started)
            {
                combined_surviving_marks = std::move(surviving_marks);
                combined_started = true;
            }
            else
                for (size_t m = 0; m < combined_surviving_marks.size(); ++m)
                    combined_surviving_marks[m] &= surviving_marks[m];
            combined_names.push_back(index_result.index_name);
            combined_total_parts = index_result.total_parts;
            combined_total_marks = index_result.total_marks;
        }

        result.index_results.push_back(std::move(index_result));
    }

    validate_forced_indices();

    /// what pruning ALL the empirically-modelled candidates together would achieve
    if (combined_names.size() >= 2 && result.baseline_marks > 0)
    {
        UInt64 survivors = 0;
        for (UInt8 m : combined_surviving_marks)
            survivors += m;
        survivors = std::min<UInt64>(survivors, result.baseline_marks);

        IndexResult combined;
        String joined;
        for (size_t i = 0; i < combined_names.size(); ++i)
            joined += (i ? ", " : "") + combined_names[i];
        combined.index_name = "(combined: " + joined + ")";
        combined.status = IndexResult::Applicable;
        combined.empirical_status = IndexResult::Ok;
        combined.estimate_source = "empirical";
        combined.estimated_marks = survivors;
        combined.skip_ratio = static_cast<double>(result.baseline_marks - survivors) / static_cast<double>(result.baseline_marks);
        combined.sampled_parts = analysis.selected_parts;
        combined.sampled_marks = analysis.selected_marks;
        combined.total_parts = combined_total_parts;
        combined.total_marks = combined_total_marks;
        result.index_results.push_back(std::move(combined));
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
        writeString(fmt::format("  est_bytes:   {}\n", ReadableSize(baseline_est_bytes)), out);
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
            writeString(fmt::format("  est_bytes:    {}\n", ReadableSize(hypo_bytes)), out);
        }

        writeString(fmt::format("  skip_ratio:   {:.1f}%\n", idx.skip_ratio * 100.0), out);
        writeCString("\n", out);

        writeCString("Estimation:\n", out);
        writeString(fmt::format("  source:           {}\n", idx.estimate_source), out);

        String empirical_status_str;
        switch (idx.empirical_status)
        {
            case IndexResult::Ok: empirical_status_str = "ok"; break;
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
    }
}

}
