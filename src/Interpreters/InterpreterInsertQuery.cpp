#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Access/EnabledQuota.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnNullable.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Processors/Transforms/buildPushingToViewsChain.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/processColumnTransformers.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/CheckConstraintsTransform.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/WindowView/StorageWindowView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/logger_useful.h>
#include <Common/ThreadStatus.h>
#include <Common/checkStackSize.h>
#include <Common/ProfileEvents.h>
#include "base/defines.h"


namespace ProfileEvents
{
    extern const Event InsertQueriesWithSubqueries;
    extern const Event QueriesWithSubqueries;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool distributed_foreground_insert;
    extern const SettingsBool insert_null_as_default;
    extern const SettingsBool optimize_trivial_insert_select;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_insert_threads;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 preferred_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsString insert_deduplication_token;
    extern const SettingsBool parallel_view_processing;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 parallel_distributed_insert_select;
    extern const SettingsBool enable_parsing_to_custom_serialization;
}

namespace ServerSetting
{
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
    extern const int QUERY_IS_PROHIBITED;
}

InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, ContextPtr context_, bool allow_materialized_, bool no_squash_, bool no_destination_, bool async_insert_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
    , allow_materialized(allow_materialized_)
    , no_squash(no_squash_)
    , no_destination(no_destination_)
    , async_insert(async_insert_)
{
    checkStackSize();
    if (auto quota = getContext()->getQuota())
        quota->checkExceeded(QuotaType::WRITTEN_BYTES);
}


StoragePtr InterpreterInsertQuery::getTable(ASTInsertQuery & query)
{
    auto current_context = getContext();

    if (query.table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        TableFunctionPtr table_function_ptr = factory.get(query.table_function, current_context);

        /// If table function needs structure hint from select query
        /// we can create a temporary pipeline and get the header.
        if (query.select && table_function_ptr->needStructureHint())
        {
            Block header_block;
            auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);

            if (current_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                InterpreterSelectQueryAnalyzer interpreter_select(query.select, current_context, select_query_options);
                header_block = interpreter_select.getSampleBlock();
            }
            else
            {
                InterpreterSelectWithUnionQuery interpreter_select{
                    query.select, current_context, select_query_options};
                auto tmp_pipeline = interpreter_select.buildQueryPipeline();
                header_block = tmp_pipeline.getHeader();
            }

            ColumnsDescription structure_hint{header_block.getNamesAndTypesList()};
            table_function_ptr->setStructureHint(structure_hint);
        }

        return table_function_ptr->execute(query.table_function, current_context, table_function_ptr->getName(),
                                           /* cached_columns */ {}, /* use_global_context */ false, /* is_insert_query */true);
    }

    if (query.table_id)
    {
        query.table_id = current_context->resolveStorageID(query.table_id);
    }
    else
    {
        /// Insert query parser does not fill table_id because table and
        /// database can be parameters and be filled after parsing.
        StorageID local_table_id(query.getDatabase(), query.getTable());
        query.table_id = current_context->resolveStorageID(local_table_id);
    }

    return DatabaseCatalog::instance().getTable(query.table_id, current_context);
}

Block InterpreterInsertQuery::getSampleBlock(
    const ASTInsertQuery & query,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_,
    bool no_destination,
    bool allow_materialized)
{
    /// If the query does not include information about columns
    if (!query.columns)
    {
        if (auto * window_view = dynamic_cast<StorageWindowView *>(table.get()))
            return window_view->getInputHeader();
        if (no_destination)
            return metadata_snapshot->getSampleBlockWithVirtuals(table->getVirtualsList());
        return metadata_snapshot->getSampleBlockNonMaterialized();
    }

    /// Form the block based on the column names from the query
    const auto columns_ast = processColumnTransformers(context_->getCurrentDatabase(), table, metadata_snapshot, query.columns);
    Names names;
    names.reserve(columns_ast->children.size());
    for (const auto & identifier : columns_ast->children)
    {
        std::string current_name = identifier->getColumnName();
        names.emplace_back(std::move(current_name));
    }

    return getSampleBlockImpl(names, table, metadata_snapshot, no_destination, allow_materialized);
}

std::optional<Names> InterpreterInsertQuery::getInsertColumnNames() const
{
    auto const * insert_query = query_ptr->as<ASTInsertQuery>();
    if (!insert_query || !insert_query->columns)
        return std::nullopt;

    auto table = DatabaseCatalog::instance().getTable(getDatabaseTable(), getContext());
    const auto columns_ast = processColumnTransformers(getContext()->getCurrentDatabase(), table, table->getInMemoryMetadataPtr(), insert_query->columns);
    Names names;
    names.reserve(columns_ast->children.size());
    for (const auto & identifier : columns_ast->children)
    {
        std::string current_name = identifier->getColumnName();
        names.emplace_back(std::move(current_name));
    }

    return names;
}

Block InterpreterInsertQuery::getSampleBlockImpl(
    const Names & names,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    bool no_destination,
    bool allow_materialized)
{
    Block table_sample_physical = metadata_snapshot->getSampleBlock();
    Block table_sample_virtuals;
    if (no_destination)
        table_sample_virtuals = table->getVirtualsHeader();

    Block table_sample_insertable = metadata_snapshot->getSampleBlockInsertable();
    Block res;
    for (const auto & current_name : names)
    {
        if (res.has(current_name))
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column {} in table {} specified more than once", current_name, table->getStorageID().getNameForLogs());

        /// Column is not ordinary or ephemeral
        if (!table_sample_insertable.has(current_name))
        {
            /// Column is materialized
            if (table_sample_physical.has(current_name))
            {
                if (!allow_materialized)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert column {}, because it is MATERIALIZED column", current_name);
                res.insert(ColumnWithTypeAndName(table_sample_physical.getByName(current_name).type, current_name));
            }
            else if (table_sample_virtuals.has(current_name))
            {
                res.insert(ColumnWithTypeAndName(table_sample_virtuals.getByName(current_name).type, current_name));
            }
            else
            {
                /// The table does not have a column with that name
                throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "No such column {} in table {}",
                    current_name, table->getStorageID().getNameForLogs());
            }
        }
        else
            res.insert(ColumnWithTypeAndName(table_sample_insertable.getByName(current_name).type, current_name));
    }
    return res;
}

static bool hasAggregateFunctions(const IAST * ast)
{
    if (const auto * func = typeid_cast<const ASTFunction *>(ast))
        if (AggregateUtils::isAggregateFunction(*func))
            return true;

    for (const auto & child : ast->children)
        if (hasAggregateFunctions(child.get()))
            return true;

    return false;
}
/** A query that just reads all data without any complex computations or filetering.
  * If we just pipe the result to INSERT, we don't have to use too many threads for read.
  */
static bool isTrivialSelect(const ASTPtr & select)
{
    if (auto * select_query = select->as<ASTSelectQuery>())
    {
        const auto & tables = select_query->tables();

        if (!tables)
            return false;

        const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();

        if (tables_in_select_query.children.size() != 1)
            return false;

        const auto & child = tables_in_select_query.children.front();
        const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
        const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

        if (table_expr.subquery)
            return false;

        /// Note: how to write it in more generic way?
        return (!select_query->distinct
            && !select_query->limit_with_ties
            && !select_query->prewhere()
            && !select_query->where()
            && !select_query->groupBy()
            && !select_query->having()
            && !select_query->orderBy()
            && !select_query->limitBy()
            && !hasAggregateFunctions(select_query));
    }
    /// This query is ASTSelectWithUnionQuery subquery
    return false;
}

Chain InterpreterInsertQuery::buildChain(
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & columns,
    ThreadStatusesHolderPtr thread_status_holder,
    std::atomic_uint64_t * elapsed_counter_ms,
    bool check_access)
{
    IInterpreter::checkStorageSupportsTransactionsIfNeeded(table, getContext());

    ProfileEvents::increment(ProfileEvents::InsertQueriesWithSubqueries);
    ProfileEvents::increment(ProfileEvents::QueriesWithSubqueries);

    ThreadGroupPtr running_group;
    if (current_thread)
        running_group = current_thread->getThreadGroup();
    if (!running_group)
        running_group = std::make_shared<ThreadGroup>(getContext());

    auto sample = getSampleBlockImpl(columns, table, metadata_snapshot, no_destination, allow_materialized);
    if (check_access)
        getContext()->checkAccess(AccessType::INSERT, table->getStorageID(), sample.getNames());

    Chain sink = buildSink(table, metadata_snapshot, thread_status_holder, running_group, elapsed_counter_ms);
    Chain chain = buildPreSinkChain(sink.getInputHeader(), table, metadata_snapshot, sample);

    chain.appendChain(std::move(sink));
    return chain;
}

Chain InterpreterInsertQuery::buildSink(
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    ThreadStatusesHolderPtr thread_status_holder,
    ThreadGroupPtr running_group,
    std::atomic_uint64_t * elapsed_counter_ms)
{
    ThreadStatus * thread_status = current_thread;

    if (!thread_status_holder)
        thread_status = nullptr;

    auto context_ptr = getContext();

    Chain out;

    /// Keep a reference to the context to make sure it stays alive until the chain is executed and destroyed
    out.addInterpreterContext(context_ptr);

    /// NOTE: we explicitly ignore bound materialized views when inserting into Kafka Storage.
    ///       Otherwise we'll get duplicates when MV reads same rows again from Kafka.
    if (table->noPushingToViews() && !no_destination)
    {
        auto sink = table->write(query_ptr, metadata_snapshot, context_ptr, async_insert);
        sink->setRuntimeData(thread_status, elapsed_counter_ms);
        out.addSource(std::move(sink));
    }
    else
    {
        out = buildPushingToViewsChain(table, metadata_snapshot, context_ptr,
            query_ptr, no_destination,
            thread_status_holder, running_group, elapsed_counter_ms, async_insert);
    }

    return out;
}

bool InterpreterInsertQuery::shouldAddSquashingFroStorage(const StoragePtr & table) const
{
    auto context_ptr = getContext();
    const Settings & settings = context_ptr->getSettingsRef();

    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    return !(settings[Setting::distributed_foreground_insert] && table->isRemote()) && !async_insert && !no_squash;
}

Chain InterpreterInsertQuery::buildPreSinkChain(
    const Block & subsequent_header,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & query_sample_block)
{
    auto context_ptr = getContext();

    const ASTInsertQuery * query = nullptr;
    if (query_ptr)
        query = query_ptr->as<ASTInsertQuery>();

    bool null_as_default = query && query->select && context_ptr->getSettingsRef()[Setting::insert_null_as_default];

    /// We create a pipeline of several streams, into which we will write data.
    Chain out;

    auto input_header = [&]() -> const Block &
    {
        return out.empty() ? subsequent_header : out.getInputHeader();
    };

    /// Note that we wrap transforms one on top of another, so we write them in reverse of data processing order.

    /// Checking constraints. It must be done after calculation of all defaults, so we can check them on calculated columns.
    if (const auto & constraints = metadata_snapshot->getConstraints(); !constraints.empty())
        out.addSource(std::make_shared<CheckConstraintsTransform>(
            table->getStorageID(), input_header(), metadata_snapshot->getConstraints(), context_ptr));

    auto adding_missing_defaults_dag = addMissingDefaults(
        query_sample_block,
        input_header().getNamesAndTypesList(),
        metadata_snapshot->getColumns(),
        context_ptr,
        null_as_default);

    auto adding_missing_defaults_actions = std::make_shared<ExpressionActions>(std::move(adding_missing_defaults_dag));

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    out.addSource(std::make_shared<ConvertingTransform>(query_sample_block, adding_missing_defaults_actions));

    return out;
}

std::pair<std::vector<Chain>, std::vector<Chain>> InterpreterInsertQuery::buildPreAndSinkChains(size_t presink_streams, size_t sink_streams, StoragePtr table, const StorageMetadataPtr & metadata_snapshot, const Block & query_sample_block)
{
    chassert(presink_streams > 0);
    chassert(sink_streams > 0);

    ThreadGroupPtr running_group;
    if (current_thread)
        running_group = current_thread->getThreadGroup();
    if (!running_group)
        running_group = std::make_shared<ThreadGroup>(getContext());

    std::vector<Chain> sink_chains;
    std::vector<Chain> presink_chains;

    for (size_t i = 0; i < sink_streams; ++i)
    {
        auto out = buildSink(table, metadata_snapshot, /* thread_status_holder= */ nullptr,
            running_group, /* elapsed_counter_ms= */ nullptr);

        sink_chains.emplace_back(std::move(out));
    }

    for (size_t i = 0; i < presink_streams; ++i)
    {
        auto out = buildPreSinkChain(sink_chains[0].getInputHeader(), table, metadata_snapshot, query_sample_block);
        presink_chains.emplace_back(std::move(out));
    }

    return {std::move(presink_chains), std::move(sink_chains)};
}


QueryPipeline InterpreterInsertQuery::buildInsertSelectPipeline(ASTInsertQuery & query, StoragePtr table)
{
    const Settings & settings = getContext()->getSettingsRef();

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, getContext(), no_destination, allow_materialized);

    bool is_trivial_insert_select = false;

    if (settings[Setting::optimize_trivial_insert_select])
    {
        const auto & select_query = query.select->as<ASTSelectWithUnionQuery &>();
        const auto & selects = select_query.list_of_selects->children;
        const auto & union_modes = select_query.list_of_modes;

        /// ASTSelectWithUnionQuery is not normalized now, so it may pass some queries which can be Trivial select queries
        const auto mode_is_all = [](const auto & mode) { return mode == SelectUnionMode::UNION_ALL; };

        is_trivial_insert_select =
            std::all_of(union_modes.begin(), union_modes.end(), std::move(mode_is_all))
            && std::all_of(selects.begin(), selects.end(), isTrivialSelect);
    }

    ContextPtr select_context = getContext();

    if (is_trivial_insert_select)
    {
        /** When doing trivial INSERT INTO ... SELECT ... FROM table,
            * don't need to process SELECT with more than max_insert_threads
            * and it's reasonable to set block size for SELECT to the desired block size for INSERT
            * to avoid unnecessary squashing.
            */

        Settings new_settings = select_context->getSettingsCopy();

        new_settings[Setting::max_threads] = std::max<UInt64>(1, settings[Setting::max_insert_threads]);

        if (table->prefersLargeBlocks())
        {
            if (settings[Setting::min_insert_block_size_rows])
                new_settings[Setting::max_block_size] = settings[Setting::min_insert_block_size_rows];
            if (settings[Setting::min_insert_block_size_bytes])
                new_settings[Setting::preferred_block_size_bytes] = settings[Setting::min_insert_block_size_bytes];
        }

        auto context_for_trivial_select = Context::createCopy(context);
        context_for_trivial_select->setSettings(new_settings);
        context_for_trivial_select->setInsertionTable(getContext()->getInsertionTable(), getContext()->getInsertionTableColumnNames());

        select_context = context_for_trivial_select;
    }

    QueryPipelineBuilder pipeline;

    {
        auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);

        if (settings[Setting::allow_experimental_analyzer])
        {
            InterpreterSelectQueryAnalyzer interpreter_select_analyzer(query.select, select_context, select_query_options);
            pipeline = interpreter_select_analyzer.buildQueryPipeline();
        }
        else
        {
            InterpreterSelectWithUnionQuery interpreter_select(query.select, select_context, select_query_options);
            pipeline = interpreter_select.buildQueryPipeline();
        }
    }

    pipeline.dropTotalsAndExtremes();

    /// Allow to insert Nullable into non-Nullable columns, NULL values will be added as defaults values.
    if (getContext()->getSettingsRef()[Setting::insert_null_as_default])
    {
        const auto & input_columns = pipeline.getHeader().getColumnsWithTypeAndName();
        const auto & query_columns = query_sample_block.getColumnsWithTypeAndName();
        const auto & output_columns = metadata_snapshot->getColumns();

        if (input_columns.size() == query_columns.size())
        {
            for (size_t col_idx = 0; col_idx < query_columns.size(); ++col_idx)
            {
                /// Change query sample block columns to Nullable to allow inserting nullable columns, where NULL values will be substituted with
                /// default column values (in AddingDefaultsTransform), so all values will be cast correctly.
                if (isNullableOrLowCardinalityNullable(input_columns[col_idx].type)
                    && !isNullableOrLowCardinalityNullable(query_columns[col_idx].type)
                    && !isVariant(query_columns[col_idx].type)
                    && !isDynamic(query_columns[col_idx].type)
                    && output_columns.has(query_columns[col_idx].name))
                {
                    query_sample_block.setColumn(
                        col_idx,
                        ColumnWithTypeAndName(
                            makeNullableOrLowCardinalityNullable(query_columns[col_idx].column),
                            makeNullableOrLowCardinalityNullable(query_columns[col_idx].type),
                            query_columns[col_idx].name));
                }
            }
        }
    }

    auto actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(),
            query_sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
    auto actions = std::make_shared<ExpressionActions>(std::move(actions_dag), ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));

    pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(in_header, actions);
    });

    /// We need to convert Sparse columns to full if the destination storage doesn't support them.
    pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
    {
        return std::make_shared<MaterializingTransform>(in_header, !table->supportsSparseSerialization());
    });

    pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
    {
        auto context_ptr = getContext();
        auto counting = std::make_shared<CountingTransform>(in_header, nullptr, context_ptr->getQuota());
        counting->setProcessListElement(context_ptr->getProcessListElement());
        counting->setProgressCallback(context_ptr->getProgressCallback());

        return counting;
    });

    size_t num_select_threads = pipeline.getNumThreads();

    pipeline.resize(1);

    if (shouldAddSquashingFroStorage(table))
    {
        pipeline.addSimpleTransform(
            [&](const Block & in_header) -> ProcessorPtr
            {
                return std::make_shared<PlanSquashingTransform>(
                    in_header,
                    table->prefersLargeBlocks() ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
                    table->prefersLargeBlocks() ? settings[Setting::min_insert_block_size_bytes] : 0ULL);
            });
    }

    pipeline.addSimpleTransform([&](const Block &in_header) -> ProcessorPtr
    {
        return std::make_shared<DeduplicationToken::AddTokenInfoTransform>(in_header);
    });

    if (!settings[Setting::insert_deduplication_token].value.empty())
    {
        pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
        {
            return std::make_shared<DeduplicationToken::SetUserTokenTransform>(settings[Setting::insert_deduplication_token].value, in_header);
        });

        pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
        {
            return std::make_shared<DeduplicationToken::SetSourceBlockNumberTransform>(in_header);
        });
    }

    /// Number of streams works like this:
    ///  * For the SELECT, use `max_threads`, or `max_insert_threads`, or whatever
    ///    InterpreterSelectQuery ends up with.
    ///  * Use `max_insert_threads` streams for various insert-preparation steps, e.g.
    ///    materializing and squashing (too slow to do in one thread). That's `presink_chains`.
    ///  * If the table supports parallel inserts, use max_insert_threads for writing to IStorage.
    ///    Otherwise ResizeProcessor them down to 1 stream.

    size_t presink_streams_size = std::max<size_t>(settings[Setting::max_insert_threads], pipeline.getNumStreams());
    if (settings[Setting::max_insert_threads].changed)
        presink_streams_size = std::max<size_t>(1, settings[Setting::max_insert_threads]);

    size_t sink_streams_size = table->supportsParallelInsert() ? std::max<size_t>(1, settings[Setting::max_insert_threads]) : 1;

    size_t views_involved =  table->isView() || !DatabaseCatalog::instance().getDependentViews(table->getStorageID()).empty();
    if (!settings[Setting::parallel_view_processing] && views_involved)
    {
        sink_streams_size = 1;
    }

    auto [presink_chains, sink_chains] = buildPreAndSinkChains(
        presink_streams_size, sink_streams_size,
        table, metadata_snapshot, query_sample_block);

    pipeline.resize(presink_chains.size());

    if (shouldAddSquashingFroStorage(table))
    {
        pipeline.addSimpleTransform(
            [&](const Block & in_header) -> ProcessorPtr
            {
                return std::make_shared<ApplySquashingTransform>(
                    in_header,
                    table->prefersLargeBlocks() ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
                    table->prefersLargeBlocks() ? settings[Setting::min_insert_block_size_bytes] : 0ULL);
            });
    }

    for (auto & chain : presink_chains)
        pipeline.addResources(chain.detachResources());
    pipeline.addChains(std::move(presink_chains));

    pipeline.resize(sink_streams_size);

    for (auto & chain : sink_chains)
        pipeline.addResources(chain.detachResources());
    pipeline.addChains(std::move(sink_chains));

    if (!settings[Setting::parallel_view_processing] && views_involved)
    {
        /// Don't use more threads for INSERT than for SELECT to reduce memory consumption.
        if (pipeline.getNumThreads() > num_select_threads)
            pipeline.setMaxThreads(num_select_threads);
    }

    pipeline.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
    {
        return std::make_shared<EmptySink>(cur_header);
    });

    return QueryPipelineBuilder::getPipeline(std::move(pipeline));
}


QueryPipeline InterpreterInsertQuery::buildInsertPipeline(ASTInsertQuery & query, StoragePtr table)
{
    const Settings & settings = getContext()->getSettingsRef();

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, getContext(), no_destination, allow_materialized);

    Chain chain;

    {
        auto [presink_chains, sink_chains] = buildPreAndSinkChains(
            /* presink_streams */1, /* sink_streams */1,
            table, metadata_snapshot, query_sample_block);

        chain = std::move(presink_chains.front());
        chain.appendChain(std::move(sink_chains.front()));
    }

    if (!settings[Setting::insert_deduplication_token].value.empty())
    {
        chain.addSource(std::make_shared<DeduplicationToken::SetSourceBlockNumberTransform>(chain.getInputHeader()));
        chain.addSource(std::make_shared<DeduplicationToken::SetUserTokenTransform>(
            settings[Setting::insert_deduplication_token].value, chain.getInputHeader()));
    }

    chain.addSource(std::make_shared<DeduplicationToken::AddTokenInfoTransform>(chain.getInputHeader()));

    if (shouldAddSquashingFroStorage(table))
    {
        bool table_prefers_large_blocks = table->prefersLargeBlocks();

        auto squashing = std::make_shared<ApplySquashingTransform>(
            chain.getInputHeader(),
            table_prefers_large_blocks ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
            table_prefers_large_blocks ? settings[Setting::min_insert_block_size_bytes] : 0ULL);

        chain.addSource(std::move(squashing));

        auto balancing = std::make_shared<PlanSquashingTransform>(
            chain.getInputHeader(),
            table_prefers_large_blocks ? settings[Setting::min_insert_block_size_rows] : settings[Setting::max_block_size],
            table_prefers_large_blocks ? settings[Setting::min_insert_block_size_bytes] : 0ULL);

        chain.addSource(std::move(balancing));
    }

    auto context_ptr = getContext();
    auto counting = std::make_shared<CountingTransform>(chain.getInputHeader(), nullptr, context_ptr->getQuota());
    counting->setProcessListElement(context_ptr->getProcessListElement());
    counting->setProgressCallback(context_ptr->getProgressCallback());
    chain.addSource(std::move(counting));

    QueryPipeline pipeline = QueryPipeline(std::move(chain));

    pipeline.setNumThreads(std::min<size_t>(pipeline.getNumThreads(), settings[Setting::max_threads]));
    pipeline.setConcurrencyControl(settings[Setting::use_concurrency_control]);

    if (query.hasInlinedData() && !async_insert)
    {
        auto format = getInputFormatFromASTInsertQuery(query_ptr, true, query_sample_block, getContext(), nullptr);

        for (auto & buffer : owned_buffers)
            format->addBuffer(std::move(buffer));

        if (settings[Setting::enable_parsing_to_custom_serialization])
            format->setSerializationHints(table->getSerializationHints());

        auto pipe = getSourceFromInputFormat(query_ptr, std::move(format), getContext(), nullptr);
        pipeline.complete(std::move(pipe));
    }

    return pipeline;
}


BlockIO InterpreterInsertQuery::execute()
{
    const Settings & settings = getContext()->getSettingsRef();
    auto & query = query_ptr->as<ASTInsertQuery &>();

    if (getContext()->getServerSettings()[ServerSetting::disable_insertion_and_mutation]
        && query.table_id.database_name != DatabaseCatalog::SYSTEM_DATABASE)
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Insert queries are prohibited");

    StoragePtr table = getTable(query);
    checkStorageSupportsTransactionsIfNeeded(table, getContext());

    if (query.partition_by && !table->supportsPartitionBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PARTITION BY clause is not supported by storage");

    auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings[Setting::lock_acquire_timeout]);

    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot, getContext(), no_destination, allow_materialized);

    /// For table functions we check access while executing
    /// getTable() -> ITableFunction::execute().
    if (!query.table_function)
        getContext()->checkAccess(AccessType::INSERT, query.table_id, query_sample_block.getNames());

    if (!allow_materialized)
    {
        for (const auto & column : metadata_snapshot->getColumns())
            if (column.default_desc.kind == ColumnDefaultKind::Materialized && query_sample_block.has(column.name))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert column {}, because it is MATERIALIZED column.", column.name);
    }

    BlockIO res;

    if (query.select)
    {
        if (settings[Setting::parallel_distributed_insert_select])
        {
            auto distributed = table->distributedWrite(query, getContext());
            if (distributed)
            {
                res.pipeline = std::move(*distributed);
            }
            else
            {
                res.pipeline = buildInsertSelectPipeline(query, table);
            }
        }
        else
        {
            res.pipeline = buildInsertSelectPipeline(query, table);
        }
    }
    else
    {
        res.pipeline = buildInsertPipeline(query, table);
    }

    res.pipeline.addStorageHolder(table);

    if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table.get()))
        res.pipeline.addStorageHolder(mv->getTargetTable());

    LOG_TEST(getLogger("InterpreterInsertQuery"), "Pipeline could use up to {} thread", res.pipeline.getNumThreads());

    return res;
}


StorageID InterpreterInsertQuery::getDatabaseTable() const
{
    return query_ptr->as<ASTInsertQuery &>().table_id;
}

void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, ContextPtr context_)
{
    const auto & insert_table = context_->getInsertionTable();
    if (!insert_table.empty())
    {
        elem.query_databases.insert(insert_table.getDatabaseName());
        elem.query_tables.insert(insert_table.getFullNameNotQuoted());
    }
}


void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr context_) const
{
    extendQueryLogElemImpl(elem, context_);
}


void registerInterpreterInsertQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterInsertQuery>(
            args.query,
            args.context,
            args.allow_materialized,
            /* no_squash */false,
            /* no_destination */false,
            /* async_insert */false);
    };
    factory.registerInterpreter("InterpreterInsertQuery", create_fn);
}


}
