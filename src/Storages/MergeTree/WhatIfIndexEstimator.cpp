#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/HypotheticalObjectStore.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/JoinedTables.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/parseIdentifiersOrStringLiteralsWithSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Parsers/ASTAlterQuery.h>
#include <Interpreters/InterpreterHypotheticalObjectQuery.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/MergeTree/WhatIfEmpiricalEstimator.h>
#include <Storages/MergeTree/WhatIfFilterAnalysis.h>
#include <Storages/MergeTree/WhatIfSettings.h>
#include <Storages/MergeTree/WhatIfStatisticalEstimator.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>

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
}

namespace ErrorCodes
{
    extern const int INDEX_NOT_USED;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

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
    JoinedTables joined_tables(context, *select);
    if (joined_tables.tablesCount() != 1)
        return nullptr;
    return joined_tables.getLeftTableStorage();
}

/// projections are stored but not estimated yet, so report them instead of dropping them.
/// whoever adds the estimate must also require SELECT on the projection's columns, the way
/// evaluateIndex does, since it will read them
void appendProjectionCandidates(
    WhatIfResult & result, const HypotheticalObjectStore & store, const MergeTreeData & data, const ContextPtr & context)
{
    auto metadata = data.getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ false);
    for (const auto & projection : store.getProjectionsForTable(data.getStorageID()))
    {
        WhatIfCandidateResult r;
        r.name = projection.name;
        r.type = projection.type == ProjectionDescription::Type::Aggregate ? "projection (aggregate)" : "projection (normal)";
        r.status = WhatIfCandidateResult::NotApplicable;
        r.not_applicable_reason = "EXPLAIN WHATIF does not estimate hypothetical projections yet";

        /// re-run the same ADD PROJECTION validation as CREATE did, so both a dropped column and a
        /// later MODIFY SETTING that disables the projection's features surface as drift
        try
        {
            checkHypotheticalProjectionIsAddable(data, metadata, projection.definition_ast, /*if_not_exists=*/false, context);
        }
        catch (const Exception &)
        {
            r.not_applicable_reason = "Hypothetical projection can no longer be added to this table: "
                + getCurrentExceptionMessage(false);
        }

        result.candidates.push_back(std::move(r));
    }
}

/// only when the store held nothing for this table
void appendNoCandidatesRow(WhatIfResult & result)
{
    WhatIfCandidateResult none;
    none.name = "(none)";
    none.status = WhatIfCandidateResult::NotApplicable;
    none.not_applicable_reason = "No hypothetical indexes or projections defined for this table. "
        "Use CREATE HYPOTHETICAL INDEX or CREATE HYPOTHETICAL PROJECTION to define one.";
    result.candidates.push_back(std::move(none));
}

/// nothing was scanned, so every candidate gets the same reason
WhatIfResult buildResultWithoutScan(
    const MergeTreeData & data, const HypotheticalObjectStore & store, const String & reason, const ContextPtr & context)
{
    WhatIfResult result;
    result.database = data.getStorageID().getDatabaseName();
    result.table = data.getStorageID().getTableName();
    for (const auto & index_desc : store.getForTable(data.getStorageID()))
    {
        WhatIfCandidateResult r;
        r.name = index_desc.name;
        r.type = index_desc.type;
        r.status = WhatIfCandidateResult::NotApplicable;
        r.not_applicable_reason = reason;
        result.candidates.push_back(std::move(r));
    }
    appendProjectionCandidates(result, store, data, context);
    if (result.candidates.empty())
        appendNoCandidatesRow(result);
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

/// Check applicability, then try empirical → statistical → applicability_only
WhatIfCandidateResult evaluateIndex(
    const IndexDescription & index_desc,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    const WhatIfSettings & settings,
    std::vector<UInt8> * surviving_marks,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();

    WhatIfCandidateResult result;
    result.name = index_desc.name;
    result.type = index_desc.type;
    result.total_parts = data.getActivePartsCount();
    result.total_marks = data.getTotalMarksCount();

    /// `context` already has the inner-SELECT settings applied, so these checks match a real read
    if (!context->getSettingsRef()[Setting::use_skip_indexes])
    {
        result.status = WhatIfCandidateResult::NotApplicable;
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
                result.status = WhatIfCandidateResult::NotApplicable;
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
        result.status = WhatIfCandidateResult::NotApplicable;
        result.not_applicable_reason = "Hypothetical index no longer matches the current table schema: "
            + getCurrentExceptionMessage(false);
        return result;
    }

    MergeTreeIndexPtr index_helper;
    try
    {
        /// validate before get, same as CREATE: creators read their arguments unguarded
        const auto & merge_tree_settings = *data.getSettings();
        MergeTreeIndexFactory::instance().validate(fresh_index_desc, /* attach = */ false, merge_tree_settings);
        index_helper = MergeTreeIndexFactory::instance().get(read_step->getStorageMetadata(), fresh_index_desc, merge_tree_settings);
    }
    catch (const Exception &)
    {
        result.status = WhatIfCandidateResult::NotApplicable;
        result.not_applicable_reason = "Failed to create index: " + getCurrentExceptionMessage(false);
        return result;
    }

    /// CREATE checked these columns, but the scan reads them now, so re-check SELECT against
    /// current grants, a grant revoked since CREATE should deny the estimate
    context->checkAccess(AccessType::SELECT, data.getStorageID(), index_helper->getColumnsRequiredForIndexCalc());

    const auto & filter_dag = read_step->getFilterActionsDAG();
    if (!filter_dag)
    {
        result.status = WhatIfCandidateResult::NotApplicable;
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
        result.status = WhatIfCandidateResult::NotApplicable;
        result.not_applicable_reason = "Cannot build index condition: " + getCurrentExceptionMessage(false);
        return result;
    }

    /// Let the condition decide first, a standalone conjunct can still be usable inside a mixed
    /// OR. Only fall through to the disjunction case when it can't prune on its own
    if (!condition || condition->alwaysUnknownOrTrue())
    {
        result.status = WhatIfCandidateResult::NotApplicable;
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

    result.status = WhatIfCandidateResult::Applicable;

    if (settings.empirical)
    {
        if (tryEstimateEmpirical(result, index_helper, condition, read_step, analysis, saved_parts, surviving_marks, context))
            return result;
        result.empirical_status = WhatIfCandidateResult::Unsupported;
    }
    else
    {
        result.empirical_status = WhatIfCandidateResult::Disabled;
    }

    if (tryEstimateWithStatistics(result, index_helper, read_step, analysis, saved_parts, predicate, context))
        return result;

    result.estimate_source = WhatIfCandidateResult::ApplicabilityOnly;
    result.estimated_marks = analysis.selected_marks;
    result.skip_ratio = 0.0;

    return result;
}

}


WhatIfResult estimateHypotheticalIndexes(
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
        auto storage = tryResolveSingleTable(select_query, local_context);
        const auto & store = local_context->getHypotheticalObjectStore();
        if (const auto * mt = dynamic_cast<const MergeTreeData *>(storage.get()))
        {
            /// Empty table -> ReadNothing, report a zero baseline
            if (mt->getActivePartsCount() == 0)
                return buildResultWithoutScan(*mt, store, "Table is empty, so there is no data to estimate a benefit", local_context);

            /// The plan answers the query without reading the table's parts at all: a trivial
            /// count, a minmax_count or exact-count projection, or a projection that selected no
            /// ranges. No index on those parts would be read
            /// a forced name can never be satisfied here, so throw like a real read would, but
            /// only when skip indexes are on, matching the scanning path below. FINAL always keeps
            /// its read step, so use_skip_indexes_if_final cannot apply on this path
            const auto & effective_settings = plan_context->getSettingsRef();
            if (effective_settings[Setting::use_skip_indexes])
            {
                for (const auto & forced_string : forced_strings)
                {
                    auto forced = parseIdentifiersOrStringLiteralsToSet(forced_string, effective_settings);
                    if (!forced.empty())
                        throw Exception(
                            ErrorCodes::INDEX_NOT_USED,
                            "Index {} is not used and setting 'force_data_skipping_indices' contains it",
                            backQuoteIfNeed(*forced.begin()));
                }
            }

            return buildResultWithoutScan(
                *mt, store, "The query is answered without reading the table's parts, so an index on them would not be read",
                local_context);
        }

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

    const RangesInDataParts & baseline_parts = analysis.parts_with_ranges;

    WhatIfResult result;
    result.database = data.getStorageID().getDatabaseName();
    result.table = data.getStorageID().getTableName();
    result.baseline_parts = analysis.selected_parts;
    result.baseline_marks = analysis.selected_marks;

    /// The average row size is the parent table's, so it says nothing about rows selected from a
    /// projection. Leave it at 0 and the formatter omits the line rather than printing a wrong one
    if (analysis.selected_rows > 0 && !analysis.readFromProjection())
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
        for (const auto & idx : result.candidates)
            if (idx.status == WhatIfCandidateResult::Applicable)
                satisfied.insert(idx.name);
        for (const auto & name : forced_indices)
            if (!satisfied.contains(name))
                throw Exception(
                    ErrorCodes::INDEX_NOT_USED,
                    "Index {} is not used and setting 'force_data_skipping_indices' contains it",
                    backQuoteIfNeed(name));
    };

    const auto & store = context->getHypotheticalObjectStore();
    auto hypo_indexes = store.getForTable(data.getStorageID());

    String blanket_not_applicable_reason;
    if (query_with_final)
        blanket_not_applicable_reason = "EXPLAIN WHATIF cannot accurately model skip-index pruning under FINAL "
                                        "(PrimaryKeyExpand may re-include granules selected by skip indexes)";
    else if (analysis.readFromProjection())
        blanket_not_applicable_reason = "The query is served from projection '"
            + baseline_parts.front().data_part->name + "', so an index on the base table's parts would not be read";

    /// Only track per-candidate surviving marks when a combined row could actually be produced
    const bool want_combined = settings.empirical && blanket_not_applicable_reason.empty()
        && hypo_indexes.size() >= 2 && result.baseline_marks > 0;

    std::vector<UInt8> combined_surviving_marks;
    bool combined_started = false;
    std::vector<String> combined_names;
    UInt64 combined_total_parts = 0;
    UInt64 combined_total_marks = 0;

    for (const auto & index_desc : hypo_indexes)
    {
        if (!blanket_not_applicable_reason.empty())
        {
            WhatIfCandidateResult r;
            r.name = index_desc.name;
            r.type = index_desc.type;
            r.status = WhatIfCandidateResult::NotApplicable;
            r.not_applicable_reason = blanket_not_applicable_reason;
            result.candidates.push_back(std::move(r));
            continue;
        }

        std::vector<UInt8> surviving_marks;
        if (want_combined)
            surviving_marks.assign(result.baseline_marks, 0);
        auto index_result = evaluateIndex(
            index_desc, read_step, analysis, baseline_parts, settings, want_combined ? &surviving_marks : nullptr, plan_context);

        /// push empirically-evaluated candidates in a per-mark survival set we can intersect
        if (want_combined && index_result.status == WhatIfCandidateResult::Applicable && index_result.estimate_source == WhatIfCandidateResult::Empirical)
        {
            if (!combined_started)
            {
                combined_surviving_marks = std::move(surviving_marks);
                combined_started = true;
            }
            else
                for (size_t m = 0; m < combined_surviving_marks.size(); ++m)
                    combined_surviving_marks[m] &= surviving_marks[m];
            combined_names.push_back(index_result.name);
            combined_total_parts = index_result.total_parts;
            combined_total_marks = index_result.total_marks;
        }

        result.candidates.push_back(std::move(index_result));
    }

    validate_forced_indices();

    /// what pruning ALL the empirically-modelled candidates together would achieve
    if (combined_names.size() >= 2 && result.baseline_marks > 0)
    {
        UInt64 survivors = 0;
        for (UInt8 m : combined_surviving_marks)
            survivors += m;
        survivors = std::min<UInt64>(survivors, result.baseline_marks);

        WhatIfCandidateResult combined;
        String joined;
        for (size_t i = 0; i < combined_names.size(); ++i)
            joined += (i ? ", " : "") + combined_names[i];
        combined.name = "(combined: " + joined + ")";
        combined.status = WhatIfCandidateResult::Applicable;
        combined.empirical_status = WhatIfCandidateResult::Ok;
        combined.estimate_source = WhatIfCandidateResult::Empirical;
        combined.estimated_marks = survivors;
        combined.skip_ratio = static_cast<double>(result.baseline_marks - survivors) / static_cast<double>(result.baseline_marks);
        combined.sampled_parts = analysis.selected_parts;
        combined.sampled_marks = analysis.selected_marks;
        combined.total_parts = combined_total_parts;
        combined.total_marks = combined_total_marks;
        result.candidates.push_back(std::move(combined));
    }

    appendProjectionCandidates(result, store, data, context);

    if (result.candidates.empty())
        appendNoCandidatesRow(result);

    return result;
}

}
