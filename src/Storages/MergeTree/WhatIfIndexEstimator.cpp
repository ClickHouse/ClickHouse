#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/JoinedTables.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
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

}
