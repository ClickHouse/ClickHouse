#include <Interpreters/buildInsertReturningPipeline.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/QueryMetadataCache.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/StreamLocalLimits.h>

#include <string_view>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    /// The INSERT and RETURNING phases share one query, one `ProcessListElement`, one thread-group `MemoryTracker`,
    /// one query/user `TemporaryDataOnDiskScope`, the user/all-user network throttlers and the per-query read/write
    /// throttlers, all established once from the outer INSERT-phase settings: the user/all-user throttlers are created
    /// by `ProcessList::insert`, and the per-query throttlers are copied by `Context::createCopy` (the getters only
    /// create one when the pointer is null, so the RETURNING context reuses the outer ones). Query-global resource and
    /// execution limits therefore cannot be re-applied for the RETURNING phase: memory, temporary-data-on-disk and
    /// network/bandwidth limits would have no effect, and the query time limit is measured from INSERT registration (so
    /// it cannot bound the subquery alone). Rather than silently ignore such settings when they appear in the RETURNING
    /// subquery's `SETTINGS` clause, reject them explicitly. Settings enforceable on the result pipeline
    /// (`max_result_rows`, `max_result_bytes`, `result_overflow_mode`) remain supported.
    void rejectUnsupportedReturningSettings(const ASTPtr & returning_select)
    {
        static const std::unordered_set<std::string_view> unsupported_settings = {
            "max_memory_usage",
            "max_memory_usage_for_user",
            "max_execution_time",
            "timeout_overflow_mode",
            "max_temporary_data_on_disk_size_for_query",
            "max_temporary_data_on_disk_size_for_user",
            "max_network_bandwidth_for_user",
            "max_network_bandwidth_for_all_users",
            "max_remote_read_network_bandwidth",
            "max_remote_write_network_bandwidth",
            "max_local_read_bandwidth",
            "max_local_write_bandwidth",
        };

        const auto * select_with_union = returning_select->as<ASTSelectWithUnionQuery>();
        if (!select_with_union || !select_with_union->list_of_selects)
            return;

        const auto & selects = select_with_union->list_of_selects->children;
        if (selects.empty())
            return;

        /// Only the last `SELECT`'s `SETTINGS` are applied to the subquery context (see `applySettingsFromQuery`).
        const auto * last_select = selects.back()->as<ASTSelectQuery>();
        if (!last_select || !last_select->settings())
            return;

        for (const auto & change : last_select->settings()->as<ASTSetQuery &>().changes)
        {
            if (unsupported_settings.contains(change.name))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Setting '{}' is not supported in the SETTINGS clause of an INSERT ... RETURNING subquery",
                    change.name);
        }
    }
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool enable_global_with_statement;
    extern const SettingsBool enable_shared_storage_snapshot_in_query;
    extern const SettingsBool enforce_strict_identifier_format;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsUInt64 max_ast_depth;
    extern const SettingsUInt64 max_ast_elements;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsOverflowMode result_overflow_mode;
    extern const SettingsSetOperationMode union_default_mode;
}

ContextMutablePtr makeReturningSelectContext(const ASTPtr & returning_select, ContextPtr context)
{
    auto returning_context = Context::createCopy(context);
    /// `Context::createCopy` gives the subquery an independent `QueryAccessInfo`, so the tables and columns read by
    /// the RETURNING subquery would be missing from `system.query_log`. Share the outer query's access info (the same
    /// approach as the materialized-view path in `InsertDependenciesBuilder`) so the accesses are recorded.
    returning_context->setQueryAccessInfo(context->getQueryAccessInfoPtr());
    InterpreterSetQuery::applySettingsFromQuery(returning_select, returning_context);
    return returning_context;
}

QueryPipeline buildReturningSelectPipeline(
    const ASTPtr & returning_select,
    ContextPtr context,
    QueryMetadataCachePtr & out_metadata_cache)
{
    rejectUnsupportedReturningSettings(returning_select);
    auto returning_context = makeReturningSelectContext(returning_select, context);

    /// `executeQueryImpl` detaches the RETURNING subquery from the INSERT before running the global AST visitors, so
    /// they do not normalize it with the outer INSERT settings. Run the same normalization passes here using the
    /// subquery's own settings, so the subquery is normalized exactly as the equivalent standalone `SELECT` would be
    /// (for example, `union_default_mode` in the subquery's `SETTINGS` decides how its top-level `UNION` is resolved).
    ASTPtr select_to_interpret = returning_select;
    const auto & returning_settings = returning_context->getSettingsRef();
    if (returning_settings[Setting::enable_global_with_statement])
        ApplyWithGlobalVisitor::visit(select_to_interpret);
    {
        SelectIntersectExceptQueryVisitor::Data data{
            returning_settings[Setting::intersect_default_mode], returning_settings[Setting::except_default_mode]};
        SelectIntersectExceptQueryVisitor{data}.visit(select_to_interpret);
    }
    {
        NormalizeSelectWithUnionQueryVisitor::Data data{returning_settings[Setting::union_default_mode]};
        NormalizeSelectWithUnionQueryVisitor{data}.visit(select_to_interpret);
    }

    /// `executeQueryImpl` skips these pre-execution checks for the detached RETURNING subquery (so they run with the
    /// subquery's own settings, not the outer INSERT's). Re-run them here, exactly as for a standalone `SELECT`.
    validateAnalyzerSettingsForReturning(select_to_interpret, returning_settings[Setting::allow_experimental_analyzer]);
    if (returning_settings[Setting::enforce_strict_identifier_format])
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings enforce_strict_identifier_format_settings(true);
        enforce_strict_identifier_format_settings.enforce_strict_identifier_format = true;
        select_to_interpret->format(buf, enforce_strict_identifier_format_settings);
    }
    if (returning_settings[Setting::max_ast_depth])
        select_to_interpret->checkDepth(returning_settings[Setting::max_ast_depth]);
    if (returning_settings[Setting::max_ast_elements])
        select_to_interpret->checkSize(returning_settings[Setting::max_ast_elements]);

    /// The RETURNING subquery must read each table as of *after* the INSERT, not the pre-INSERT snapshot the INSERT
    /// phase may have pinned. With `enable_shared_storage_snapshot_in_query`, storage snapshots are cached per table
    /// for the query's lifetime in the query context's `QueryMetadataCache`. That member is a weak pointer, so the
    /// cache must be owned for the whole pipeline lifetime by whoever drives the query (see `executeQueryImpl`, which
    /// stores it in `BlockIO::query_metadata_cache`). Install a fresh cache on the query context and hand ownership to
    /// the caller via `out_metadata_cache`, so it survives both planning and execution of the subquery and the
    /// subquery sees a new, post-INSERT snapshot. The INSERT phase has finished, so the previous cache is not needed.
    if (returning_settings[Setting::enable_shared_storage_snapshot_in_query] && returning_context->hasQueryContext())
    {
        out_metadata_cache = std::make_shared<QueryMetadataCache>();
        returning_context->getQueryContext()->setQueryMetadataCache(out_metadata_cache);
    }

    const auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete);
    if (returning_settings[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(select_to_interpret, returning_context, select_query_options);
        return QueryPipelineBuilder::getPipeline(interpreter.buildQueryPipeline());
    }

    InterpreterSelectWithUnionQuery interpreter(select_to_interpret, returning_context, select_query_options);
    return QueryPipelineBuilder::getPipeline(interpreter.buildQueryPipeline());
}

void setupPullingQueryPipeline(
    QueryPipeline & pipeline,
    ContextPtr context,
    QueryProcessingStage::Enum stage,
    const ASTPtr & returning_select)
{
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setProcessListElement(context->getProcessListElement());

    if (stage == QueryProcessingStage::Complete && pipeline.pulling())
    {
        const auto limits_context = returning_select ? makeReturningSelectContext(returning_select, context) : context;
        const auto & settings = limits_context->getSettingsRef();
        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT;
        limits.size_limits = SizeLimits(
            settings[Setting::max_result_rows],
            settings[Setting::max_result_bytes],
            settings[Setting::result_overflow_mode]);
        pipeline.setLimitsAndQuota(limits, context->getQuota());
    }
}

bool replacePipelineWithInsertReturningAfterPush(
    BlockIO & io,
    const ASTInsertQuery & insert_query,
    ContextPtr context,
    QueryProcessingStage::Enum stage)
{
    if (!insert_query.returning_select)
        return false;

    io.pipeline.reset();
    io.pipeline = buildReturningSelectPipeline(insert_query.returning_select, context, io.query_metadata_cache);
    setupPullingQueryPipeline(io.pipeline, context, stage, insert_query.returning_select);
    if (io.finish_callback_state)
        io.finish_callback_state->insert_returning_result_as_select = true;
    return true;
}

QueryPipeline buildInsertReturningPipeline(
    QueryPipeline insert_pipeline,
    const ASTPtr & returning_select,
    ContextPtr context,
    QueryMetadataCachePtr & out_metadata_cache)
{
    if (insert_pipeline.pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "INSERT pipeline must be completed before wrapping with RETURNING");

    /// Run INSERT to completion before building the RETURNING `SELECT` pipeline, so semantic errors during
    /// subquery planning (unknown identifiers, etc.) do not happen before persisted insert side effects — same ordering
    /// as the native-protocol push path (`replacePipelineWithInsertReturningAfterPush`).
    insert_pipeline.setProcessListElement(context->getProcessListElement());
    insert_pipeline.setProgressCallback(context->getProgressCallback());

    CompletedPipelineExecutor insert_executor(insert_pipeline);
    /// Honor interactive cancellation (client Ctrl+C / `Cancel` packet) while the INSERT phase runs, like the normal
    /// completed-pipeline paths in `TCPHandler` and `LocalConnection`. `setProcessListElement` alone only reacts once
    /// the query is already marked killed; the cancel callback is what polls for the client's cancel request.
    if (auto callback = context->getInteractiveCancelCallback())
        insert_executor.setCancelCallback(
            std::move(callback), context->getSettingsRef()[Setting::interactive_delay] / 1000);
    insert_executor.execute();

    return buildReturningSelectPipeline(returning_select, context, out_metadata_cache);
}

}
