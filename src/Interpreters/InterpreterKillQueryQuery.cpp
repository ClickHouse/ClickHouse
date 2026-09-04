#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterKillQueryQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/TransactionLog.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRowPolicies.h>
#include <Analyzer/TableNode.h>
#include <Columns/ColumnString.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/ISource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageValues.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/quoteString.h>
#include <thread>
#include <cstddef>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


static const char * cancellationCodeToStatus(CancellationCode code)
{
    switch (code)
    {
        case CancellationCode::NotFound:
            return "finished";
        case CancellationCode::QueryIsNotInitializedYet:
            return "pending";
        case CancellationCode::CancelCannotBeSent:
            return "cant_cancel";
        case CancellationCode::CancelSent:
            return "waiting";
        default:
            return "unknown_status";
    }
}


struct QueryDescriptor
{
    String query_id;
    String user;
    size_t source_num;
    bool processed = false;

    QueryDescriptor(String query_id_, String user_, size_t source_num_, bool processed_ = false)
        : query_id(std::move(query_id_)), user(std::move(user_)), source_num(source_num_), processed(processed_) {}
};

using QueryDescriptors = std::vector<QueryDescriptor>;


static void insertResultRow(size_t n, CancellationCode code, const Block & source, const Block & header, MutableColumns & columns)
{
    columns[0]->insert(cancellationCodeToStatus(code));

    for (size_t col_num = 1, size = columns.size(); col_num < size; ++col_num)
        columns[col_num]->insertFrom(*source.getByName(header.getByPosition(col_num).name).column, n);
}

static QueryDescriptors extractQueriesExceptMeAndCheckAccess(const Block & processes_block, ContextPtr context)
{
    QueryDescriptors res;
    size_t num_processes = processes_block.rows();
    res.reserve(num_processes);

    const ColumnString & query_id_col = typeid_cast<const ColumnString &>(*processes_block.getByName("query_id").column);
    const ColumnString & user_col = typeid_cast<const ColumnString &>(*processes_block.getByName("user").column);
    const ClientInfo & my_client = context->getProcessListElement()->getClientInfo();

    bool access_denied = false;
    std::optional<bool> is_kill_query_granted_value;
    auto is_kill_query_granted = [&]() -> bool
    {
        if (!is_kill_query_granted_value)
        {
            is_kill_query_granted_value = context->getAccess()->isGranted(AccessType::KILL_QUERY);
            if (!*is_kill_query_granted_value)
                access_denied = true;
        }
        return *is_kill_query_granted_value;
    };

    String query_user;

    for (size_t i = 0; i < num_processes; ++i)
    {
        if ((my_client.current_query_id == query_id_col.getDataAt(i))
            && (my_client.current_user == user_col.getDataAt(i)))
            continue;

        std::string query_id{query_id_col.getDataAt(i)};
        query_user = user_col.getDataAt(i);

        if ((my_client.current_user != query_user) && !is_kill_query_granted())
            continue;

        res.emplace_back(std::move(query_id), query_user, i, false);
    }

    if (res.empty() && access_denied)
        throw Exception(ErrorCodes::ACCESS_DENIED, "User {} attempts to kill query created by {}", my_client.current_user, query_user);

    return res;
}


class SyncKillQuerySource final : public ISource
{
public:
    SyncKillQuerySource(ProcessList & process_list_, QueryDescriptors && processes_to_stop_, Block && processes_block_,
                             SharedHeader res_sample_block_)
        : ISource(res_sample_block_)
        , process_list(process_list_)
        , processes_to_stop(std::move(processes_to_stop_))
        , processes_block(std::make_shared<const Block>(std::move(processes_block_)))
        , res_sample_block(res_sample_block_)
    {
        addTotalRowsApprox(processes_to_stop.size());
    }

    String getName() const override
    {
        return "SynchronousQueryKiller";
    }

    Chunk generate() override
    {
        size_t num_result_queries = processes_to_stop.size();

        if (num_processed_queries >= num_result_queries)
            return {};

        MutableColumns columns = res_sample_block->cloneEmptyColumns();

        do
        {
            for (auto & curr_process : processes_to_stop)
            {
                if (curr_process.processed)
                    continue;

                LOG_DEBUG(getLogger("KillQuery"), "Will kill query {} (synchronously)", curr_process.query_id);

                auto code = process_list.sendCancelToQuery(curr_process.query_id, curr_process.user);

                if (code != CancellationCode::QueryIsNotInitializedYet && code != CancellationCode::CancelSent)
                {
                    curr_process.processed = true;
                    insertResultRow(curr_process.source_num, code, *processes_block, *res_sample_block, columns);
                    ++num_processed_queries;
                }
                /// Wait if CancelSent
            }

            /// KILL QUERY could be killed also
            if (isCancelled())
                break;

            /// Sleep if there are unprocessed queries
            if (num_processed_queries < num_result_queries)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));

        /// Don't produce empty block
        } while (columns.empty() || columns[0]->empty());

        size_t num_rows = columns.empty() ? 0 : columns.front()->size();
        return Chunk(std::move(columns), num_rows);
    }

    ProcessList & process_list;
    QueryDescriptors processes_to_stop;
    SharedHeader processes_block;
    SharedHeader res_sample_block;
    size_t num_processed_queries = 0;
};


/// Executes a `SELECT` written by this interpreter and returns its whole result as a single block. One
/// logical read can arrive as several blocks (per-row subquery, small `max_block_size`, parallel reads),
/// so they are concatenated.
static Block runInternalSelect(const String & select_query, ContextMutablePtr query_context)
{
    auto io = executeQuery(select_query, std::move(query_context), QueryFlags{ .internal = true }).second;

    Blocks blocks;
    io.executeWithCallbacks([&]()
    {
        PullingPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
        {
            if (!block.empty())
                blocks.push_back(std::move(block));
        }
    });

    Block res = concatenateBlocks(blocks);

    /// Materialize const columns, because callers use typeid_cast to concrete column types.
    materializeBlockInplace(res);

    return res;
}

/// Filtering a `ColumnLowCardinality` keeps the dictionary of the column it came from, so a surviving
/// row's index depends on rows the caller cannot see. Every column is rebuilt, because a dictionary
/// nested in a `Map` or an `Array` carries the same dependency.
static void rebuildLowCardinalityDictionaries(Block & block)
{
    for (auto & elem : block)
    {
        if (!elem.column)
            continue;

        auto rebuilt = elem.type->createColumn();
        rebuilt->insertRangeFrom(*elem.column, 0, elem.column->size());
        elem.column = std::move(rebuilt);
    }
}

/// Reads the queries the caller is allowed to kill, under a context with full access.
static Block readKillableProcesses(const ContextPtr & context, const StoragePtr & storage)
{
    /// The predicate is evaluated against this block instead of the table, so every column it may name
    /// has to be here. `SELECT *` omits the ALIAS and the virtual columns, and its width depends on
    /// `asterisk_include_alias_columns`, which is the caller's to set.
    auto metadata = storage->getInMemoryMetadataPtr(context, false);
    NamesAndTypesList columns_to_read = metadata->getColumnsWithVirtuals().getAll();

    String select_query = "SELECT ";
    bool first = true;
    for (const auto & column : columns_to_read)
    {
        if (!first)
            select_query += ", ";
        first = false;
        select_query += backQuoteIfNeed(column.name);
    }
    select_query += " FROM system.processes";

    /// `system.processes.user` and the ownership test in `extractQueriesExceptMeAndCheckAccess` are the
    /// same `ClientInfo::current_user` field, so this filter and that test accept the same rows.
    select_query += " WHERE user = " + quoteString(context->getProcessListElement()->getClientInfo().current_user);

    /// A copy of the global context has no bound user, so this read is not subject to a grant. The
    /// caller's settings are not replayed into it: the query text above is fixed, and
    /// `additional_table_filters` would add an expression of theirs to a read that has full access.
    auto inner_context = Context::createCopy(context->getGlobalContext());
    inner_context->makeQueryContext();
    inner_context->setCurrentQueryId("");

    Block res;
    {
        /// The scan sees every user's row before the filter above, so it runs in a thread group of its
        /// own: its rows are neither charged to the caller's memory, quota and profile events nor
        /// reported to them. The thread keeps its name, only the group it accounts to changes.
        auto scan_group = ThreadGroup::createForQuery(inner_context);
        ThreadGroupSwitcher switcher(scan_group, getThreadName(), /*allow_existing_group=*/ true);

        /// The scan must not be accounted to the caller's group, and the switcher's constructor cannot throw.
        if (getCurrentThreadGroup() != scan_group)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Could not isolate the `system.processes` scan from the calling query's thread group");

        res = runInternalSelect(select_query, std::move(inner_context));
    }
    rebuildLowCardinalityDictionaries(res);
    return res;
}

/// Applies the caller's effective `system.processes` row policy to an already materialized block.
static void applyProcessesRowPolicy(Block & block, const ContextPtr & context, const StoragePtr & storage)
{
    auto row_policy_filter = context->getRowPolicyFilter("system", "processes", RowPolicyFilterType::SELECT_FILTER);
    if (!row_policy_filter || row_policy_filter->isAlwaysTrue())
        return;

    /// `system.query_log.used_row_policies` is filled from the query context, so a policy that governed
    /// the statement has to be registered there to appear in it.
    if (context->hasQueryContext())
    {
        for (const auto & row_policy : row_policy_filter->policies)
            context->getQueryContext()->addUsedRowPolicy(row_policy->getFullName().toString());
    }

    /// A policy commonly spells `user = currentUser()`, which resolves against the context it is
    /// compiled under, so it has to be the caller's and not the full-access one used for the read.
    auto policy_context = Context::createCopy(context);
    auto planner_context = std::make_shared<PlannerContext>(
        policy_context,
        std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}),
        SelectQueryOptions{});

    /// The real `system.processes` identity, so that `user`, `processes.user` and
    /// `system.processes.user` bind exactly as they do when the policy is applied to a plain `SELECT`.
    auto table_expression = std::make_shared<TableNode>(storage, policy_context);

    /// Naming the block's columns here keeps `buildFilterInfo` from looking up table expression data
    /// that only a full planner run registers.
    auto block_names = block.getNameSet();

    /// The policy expression is owned by the access cache and shared by every query the policy governs,
    /// so this call is given a copy of its own.
    auto filter_info
        = buildFilterInfo(row_policy_filter->expression->clone(), table_expression, planner_context, std::move(block_names));

    /// A set over a subquery is only buildable once it has a query plan, and compiling the filter
    /// registers such a subquery without planning it. `buildFilterExpression` below builds the sets.
    for (const auto & subquery : planner_context->getPreparedSets().getSubqueries())
    {
        auto subquery_options = SelectQueryOptions{}.subquery();
        subquery_options.forceMaterializeCTE();
        Planner subquery_planner(
            subquery->detachQueryTree(),
            subquery_options,
            std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}));
        subquery_planner.buildQueryPlanIfNeeded();
        subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan()));
    }

    auto actions = VirtualColumnUtils::buildFilterExpression(std::move(filter_info.actions), policy_context);
    VirtualColumnUtils::filterBlockWithExpression(actions, block);
    rebuildLowCardinalityDictionaries(block);
}

/// The relation below carries the rows under the table's name alone, so that is the longest qualifier
/// resolvable there, while a predicate may name a column with any qualifier the table itself accepts.
static void unqualifyProcessesColumns(ASTPtr & ast)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        const auto & parts = identifier->name_parts;
        if (parts.size() > 2 && parts[0] == "system" && parts[1] == "processes")
        {
            auto unqualified = make_intrusive<ASTIdentifier>(std::vector<String>(parts.begin() + 1, parts.end()));
            unqualified->setAlias(identifier->tryGetAlias());
            ast = std::move(unqualified);
        }
        return;
    }

    for (auto & child : ast->children)
        unqualifyProcessesColumns(child);
}

/// Runs the caller's predicate over the materialized block, under the caller's own rights.
static Block selectFromKillableProcesses(const ContextPtr & context, const ASTPtr & where_expression, Block block)
{
    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    ColumnsDescription columns{block.getNamesAndTypesList()};
    auto creator = [&](const StorageID & table_id) -> StoragePtr { return std::make_shared<StorageValues>(table_id, columns, block); };

    String table_name = "_kill_query_processes_" + toString(UUIDHelpers::generateV4());
    query_context->addExternalTable(table_name, TemporaryTableHolder(query_context, creator));

    /// Aliased `processes`, so that a predicate spelled `processes.<column>` resolves the way it does
    /// against the table. An alias and not a subquery: a subquery would add a relation of its own for
    /// the caller's limits and filter settings to apply to.
    String select_query = "SELECT query_id, user, query FROM " + backQuoteIfNeed(table_name) + " AS processes";
    if (where_expression)
    {
        /// A copy: the statement's AST is shared and outlives this read.
        auto predicate = where_expression->clone();
        unqualifyProcessesColumns(predicate);
        select_query += " WHERE " + predicate->formatWithSecretsOneLine();
    }

    return runInternalSelect(select_query, std::move(query_context));
}

/// The `system.processes` read for a caller who cannot select from that table: authorize the rows
/// first, then let the caller's predicate choose among the rows that survived.
static Block getKillableProcesses(const ContextPtr & context, const ASTPtr & where_expression)
{
    auto storage = DatabaseCatalog::instance().getTable(StorageID{"system", "processes"}, context);

    Block block = readKillableProcesses(context, storage);
    /// A read that yielded no block at all has no columns, and `StorageValues` cannot resolve a name in one.
    if (block.rows() == 0)
        return {};

    applyProcessesRowPolicy(block, context, storage);
    return selectFromKillableProcesses(context, where_expression, std::move(block));
}


BlockIO InterpreterKillQueryQuery::execute()
{
    const auto & query = query_ptr->as<ASTKillQueryQuery &>();

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    BlockIO res_io;
    switch (query.type)
    {
    case ASTKillQueryQuery::Type::Query:
    {
        /// The read below always names these three columns and every referenced column needs its own
        /// `SELECT` grant, so a caller missing any of them cannot read `system.processes` at all. A
        /// table-wide check would instead divert callers who hold only some of the columns.
        static const Strings kill_query_columns{"query_id", "user", "query"};
        bool can_read_processes
            = getContext()->getAccess()->isGranted(AccessType::SELECT, "system", "processes", kill_query_columns);

        Block processes_block = can_read_processes
            ? getSelectResult("query_id, user, query", "system.processes")
            : getKillableProcesses(getContext(), query.where_expression);
        if (processes_block.empty())
            return res_io;

        ProcessList & process_list = getContext()->getProcessList();
        QueryDescriptors queries_to_stop = extractQueriesExceptMeAndCheckAccess(processes_block, getContext());

        auto header = processes_block.cloneEmpty();
        header.insert(0, {ColumnString::create(), std::make_shared<DataTypeString>(), "kill_status"});

        if (!query.sync || query.test)
        {
            MutableColumns res_columns = header.cloneEmptyColumns();
            for (const auto & query_desc : queries_to_stop)
            {
                if (!query.test)
                    LOG_DEBUG(getLogger("KillQuery"), "Will kill query {} (asynchronously)", query_desc.query_id);
                auto code = (query.test) ? CancellationCode::Unknown : process_list.sendCancelToQuery(query_desc.query_id, query_desc.user);
                insertResultRow(query_desc.source_num, code, processes_block, header, res_columns);
            }

            res_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(header.cloneWithColumns(std::move(res_columns)))));
        }
        else
        {
            res_io.pipeline = QueryPipeline(std::make_shared<SyncKillQuerySource>(
                process_list, std::move(queries_to_stop), std::move(processes_block), std::make_shared<const Block>(header)));
        }

        break;
    }
    case ASTKillQueryQuery::Type::Mutation:
    {
        Block mutations_block = getSelectResult("database, table, mutation_id, command", "system.mutations");
        if (mutations_block.empty())
            return res_io;

        const ColumnString & database_col = typeid_cast<const ColumnString &>(*mutations_block.getByName("database").column);
        const ColumnString & table_col = typeid_cast<const ColumnString &>(*mutations_block.getByName("table").column);
        const ColumnString & mutation_id_col = typeid_cast<const ColumnString &>(*mutations_block.getByName("mutation_id").column);
        const ColumnString & command_col = typeid_cast<const ColumnString &>(*mutations_block.getByName("command").column);

        auto header = mutations_block.cloneEmpty();
        header.insert(0, {ColumnString::create(), std::make_shared<DataTypeString>(), "kill_status"});

        MutableColumns res_columns = header.cloneEmptyColumns();
        auto table_id = StorageID::createEmpty();
        AccessRightsElements required_access_rights;
        auto access = getContext()->getAccess();
        bool access_denied = false;

        for (size_t i = 0; i < mutations_block.rows(); ++i)
        {
            table_id = StorageID{std::string{database_col.getDataAt(i)}, std::string{table_col.getDataAt(i)}};
            std::string mutation_id{mutation_id_col.getDataAt(i)};

            CancellationCode code = CancellationCode::Unknown;
            if (!query.test)
            {
                auto storage = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
                if (!storage)
                    code = CancellationCode::NotFound;
                else
                {
                    const std::string alter_command{command_col.getDataAt(i)};
                    const auto with_round_bracket = alter_command.front() == '(';
                    ParserAlterCommand parser{with_round_bracket};
                    auto command_ast = parseQuery(
                        parser,
                        alter_command,
                        0,
                        getContext()->getSettingsRef()[Setting::max_parser_depth],
                        getContext()->getSettingsRef()[Setting::max_parser_backtracks]);
                    required_access_rights = InterpreterAlterQuery::getRequiredAccessForCommand(
                        command_ast->as<const ASTAlterCommand &>(), table_id.database_name, table_id.table_name,
                        InterpreterAlterQuery::isRowExistsLightweightDeleteMarker(storage, getContext()));
                    if (!access->isGranted(required_access_rights))
                    {
                        access_denied = true;
                        continue;
                    }
                    code = storage->killMutation(mutation_id);
                }
            }

            insertResultRow(i, code, mutations_block, header, res_columns);
        }

        if (res_columns[0]->empty() && access_denied)
            throw Exception(ErrorCodes::ACCESS_DENIED, "Not allowed to kill mutation. "
                "To execute this query, it's necessary to have the grant {}", required_access_rights.toString());

        res_io.pipeline = QueryPipeline(Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(header.cloneWithColumns(std::move(res_columns))))));

        break;
    }
    case ASTKillQueryQuery::Type::PartMoveToShard:
    {
        if (query.sync)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SYNC modifier is not supported for this statement.");

        Block moves_block = getSelectResult(
            "database, table, task_name, task_uuid, part_name, to_shard, state",
            "system.part_moves_between_shards");

        if (moves_block.empty())
            return res_io;

        const ColumnString & database_col = typeid_cast<const ColumnString &>(*moves_block.getByName("database").column);
        const ColumnString & table_col = typeid_cast<const ColumnString &>(*moves_block.getByName("table").column);
        const ColumnUUID & task_uuid_col = typeid_cast<const ColumnUUID &>(*moves_block.getByName("task_uuid").column);

        auto header = moves_block.cloneEmpty();
        header.insert(0, {ColumnString::create(), std::make_shared<DataTypeString>(), "kill_status"});

        MutableColumns res_columns = header.cloneEmptyColumns();
        auto table_id = StorageID::createEmpty();
        AccessRightsElements required_access_rights;
        auto access = getContext()->getAccess();
        bool access_denied = false;

        for (size_t i = 0; i < moves_block.rows(); ++i)
        {
            table_id = StorageID{std::string{database_col.getDataAt(i)}, std::string{table_col.getDataAt(i)}};
            auto task_uuid = task_uuid_col[i].safeGet<UUID>();

            CancellationCode code = CancellationCode::Unknown;

            if (!query.test)
            {
                auto storage = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
                if (!storage)
                    code = CancellationCode::NotFound;
                else
                {
                    ASTAlterCommand alter_command{};
                    alter_command.type = ASTAlterCommand::MOVE_PARTITION;
                    alter_command.move_destination_type = DataDestinationType::SHARD;
                    required_access_rights = InterpreterAlterQuery::getRequiredAccessForCommand(
                        alter_command, table_id.database_name, table_id.table_name,
                        InterpreterAlterQuery::isRowExistsLightweightDeleteMarker(storage, getContext()));
                    if (!access->isGranted(required_access_rights))
                    {
                        access_denied = true;
                        continue;
                    }
                    code = storage->killPartMoveToShard(task_uuid);
                }
            }

            insertResultRow(i, code, moves_block, header, res_columns);
        }

        if (res_columns[0]->empty() && access_denied)
            throw Exception(ErrorCodes::ACCESS_DENIED, "Not allowed to kill move partition. "
                "To execute this query, it's necessary to have the grant {}", required_access_rights.toString());

        res_io.pipeline = QueryPipeline(Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(header.cloneWithColumns(std::move(res_columns))))));

        break;
    }
    case ASTKillQueryQuery::Type::Transaction:
    {
        getContext()->checkAccess(AccessType::KILL_TRANSACTION);

        Block transactions_block = getSelectResult("tid, tid_hash, elapsed, is_readonly, state", "system.transactions");

        if (transactions_block.empty())
            return res_io;

        const ColumnUInt64 & tid_hash_col = typeid_cast<const ColumnUInt64 &>(*transactions_block.getByName("tid_hash").column);

        auto header = transactions_block.cloneEmpty();
        header.insert(0, {ColumnString::create(), std::make_shared<DataTypeString>(), "kill_status"});
        MutableColumns res_columns = header.cloneEmptyColumns();

        for (size_t i = 0; i < transactions_block.rows(); ++i)
        {
            UInt64 tid_hash = tid_hash_col.getUInt(i);

            CancellationCode code = CancellationCode::Unknown;
            if (!query.test)
            {
                auto txn = TransactionLog::instance().tryGetRunningTransaction(tid_hash);
                if (txn)
                {
                    txn->onException();
                    if (txn->getState() == MergeTreeTransaction::ROLLED_BACK)
                        code = CancellationCode::CancelSent;
                    else
                        code = CancellationCode::CancelCannotBeSent;
                }
                else
                {
                    code = CancellationCode::NotFound;
                }
            }

            insertResultRow(i, code, transactions_block, header, res_columns);
        }

        res_io.pipeline = QueryPipeline(Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(header.cloneWithColumns(std::move(res_columns))))));
        break;
    }
    }

    return res_io;
}

Block InterpreterKillQueryQuery::getSelectResult(const String & columns, const String & table)
{
    String select_query = "SELECT " + columns + " FROM " + table;
    auto & where_expression = query_ptr->as<ASTKillQueryQuery>()->where_expression;
    if (where_expression)
        select_query += " WHERE " + where_expression->formatWithSecretsOneLine();

    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    return runInternalSelect(select_query, std::move(query_context));
}


AccessRightsElements InterpreterKillQueryQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<ASTKillQueryQuery &>();
    AccessRightsElements required_access;
    if (query.type == ASTKillQueryQuery::Type::Query)
        required_access.emplace_back(AccessType::KILL_QUERY);
    else if (query.type == ASTKillQueryQuery::Type::Mutation)
        required_access.emplace_back(
                AccessType::ALTER_UPDATE
                | AccessType::ALTER_DELETE
                | AccessType::ALTER_MATERIALIZE_INDEX
                | AccessType::ALTER_MATERIALIZE_COLUMN
                | AccessType::ALTER_MATERIALIZE_TTL
                | AccessType::ALTER_REWRITE_PARTS
            );
    return required_access;
}

void registerInterpreterKillQueryQuery(InterpreterFactory & factory);
void registerInterpreterKillQueryQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterKillQueryQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterKillQueryQuery", create_fn);
}

}
