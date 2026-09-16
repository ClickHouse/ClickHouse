#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterKillQueryQuery.h>
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
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/ISource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
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


struct SelfKillTarget
{
    String query_id;
    String user;
};


/// `<column> = '<literal>'`, in either operand order. `ASTIdentifier::name()` is the qualified name, so a
/// written `processes.query_id` does not match this.
static bool matchColumnEqualsStringLiteral(const IAST & ast, std::string_view column, String & value)
{
    const auto * function = ast.as<ASTFunction>();
    if (!function || function->name != "equals" || !function->arguments || function->arguments->children.size() != 2)
        return false;

    auto match_sides = [&](const ASTPtr & maybe_identifier, const ASTPtr & maybe_literal)
    {
        if (!maybe_identifier || !maybe_literal)
            return false;

        const auto * identifier = maybe_identifier->as<ASTIdentifier>();
        const auto * literal = maybe_literal->as<ASTLiteral>();
        if (!identifier || !literal || identifier->name() != column || literal->value.getType() != Field::Types::String)
            return false;

        value = literal->value.safeGet<String>();
        return true;
    };

    const auto & left = function->arguments->children[0];
    const auto & right = function->arguments->children[1];
    return match_sides(left, right) || match_sides(right, left);
}


/// The string `ProcessList` keys this caller's queries by. Empty when the statement is not running as a
/// registered query, and there is then no identity to scope a kill to.
static std::optional<String> tryGetCallerUserName(const ContextPtr & context)
{
    if (auto element = context->getProcessListElement())
        return element->getClientInfo().current_user;
    return {};
}


/// `query_id = '<id>'`, optionally `AND user = '<the caller>'`; anything else is nullopt and so fails closed.
static std::optional<SelfKillTarget> trySelfKillTarget(const ASTKillQueryQuery & query, const ContextPtr & context)
{
    if (query.type != ASTKillQueryQuery::Type::Query || !query.where_expression)
        return {};

    auto caller = tryGetCallerUserName(context);
    if (!caller)
        return {};

    String query_id;
    if (matchColumnEqualsStringLiteral(*query.where_expression, "query_id", query_id))
        return SelfKillTarget{std::move(query_id), std::move(*caller)};

    const auto * conjunction = query.where_expression->as<ASTFunction>();
    if (!conjunction || conjunction->name != "and" || !conjunction->arguments || conjunction->arguments->children.size() != 2)
        return {};

    const auto & first = *conjunction->arguments->children[0];
    const auto & second = *conjunction->arguments->children[1];

    String user;
    const bool matched
        = (matchColumnEqualsStringLiteral(first, "query_id", query_id) && matchColumnEqualsStringLiteral(second, "user", user))
        || (matchColumnEqualsStringLiteral(second, "query_id", query_id) && matchColumnEqualsStringLiteral(first, "user", user));

    if (!matched || user != *caller)
        return {};

    return SelfKillTarget{std::move(query_id), std::move(*caller)};
}


/// An absent id and an id owned by another user both give an empty block, which is what a read of
/// `system.processes` returns for a predicate matching no row.
static Block ownRunningQueryBlock(ProcessList & process_list, const SelfKillTarget & target)
{
    auto query_id_column = ColumnString::create();
    auto user_column = ColumnString::create();
    auto query_column = ColumnString::create();

    if (auto query_text = process_list.tryGetQueryTextOfUserQuery(target.query_id, target.user))
    {
        query_id_column->insert(target.query_id);
        user_column->insert(target.user);
        query_column->insert(*query_text);
    }

    return Block{
        {std::move(query_id_column), std::make_shared<DataTypeString>(), "query_id"},
        {std::move(user_column), std::make_shared<DataTypeString>(), "user"},
        {std::move(query_column), std::make_shared<DataTypeString>(), "query"}};
}


static ASTPtr canonicalSelfKillPredicate(const SelfKillTarget & target)
{
    return makeASTFunction(
        "and",
        makeASTFunction("equals", make_intrusive<ASTIdentifier>("query_id"), make_intrusive<ASTLiteral>(target.query_id)),
        makeASTFunction("equals", make_intrusive<ASTIdentifier>("user"), make_intrusive<ASTLiteral>(target.user)));
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


BlockIO InterpreterKillQueryQuery::execute()
{
    const auto & query = query_ptr->as<ASTKillQueryQuery &>();

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();

        ASTPtr query_to_queue = query_ptr;

        /// Queue the caller's identity written by the server, not the id alone: access is checked on the
        /// initiator only, and a worker runs DDL with no bound user (so, with full access) unless
        /// `distributed_ddl_use_initial_user_and_roles` is on.
        if (auto self_kill = trySelfKillTarget(query, getContext()))
        {
            query_to_queue = query_ptr->clone();
            auto & kill_to_queue = query_to_queue->as<ASTKillQueryQuery &>();
            kill_to_queue.setOrReplace(kill_to_queue.where_expression, canonicalSelfKillPredicate(*self_kill));
        }

        return executeDDLQueryOnCluster(query_to_queue, getContext(), params);
    }

    BlockIO res_io;
    switch (query.type)
    {
    case ASTKillQueryQuery::Type::Query:
    {
        static const Strings kill_query_columns{"query_id", "user", "query"};
        const bool can_read_processes
            = getContext()->getAccess()->isGranted(AccessType::SELECT, "system", "processes", kill_query_columns);

        std::optional<SelfKillTarget> self_kill;
        if (!can_read_processes)
            self_kill = trySelfKillTarget(query, getContext());

        Block processes_block = self_kill
            ? ownRunningQueryBlock(getContext()->getProcessList(), *self_kill)
            : getSelectResult("query_id, user, query", "system.processes");
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

    auto io = executeQuery(select_query, std::move(query_context), QueryFlags{ .internal = true }).second;

    /// The pipeline can legitimately produce multiple blocks (e.g. when the
    /// `WHERE` clause contains a per-row subquery that the planner splits into
    /// chunks, when `max_block_size` is small, or when parallel reads are used).
    /// Previously this code asserted "Expected one block from input stream",
    /// which fired as a LOGICAL_ERROR for valid queries surfaced by the AST
    /// fuzzer (issue #104857). Collect every produced block and concatenate
    /// them — the result set is bounded by the size of `system.processes`,
    /// `system.mutations`, `system.part_moves_between_shards` or
    /// `system.transactions`, so this remains cheap.
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


AccessRightsElements InterpreterKillQueryQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<ASTKillQueryQuery &>();
    AccessRightsElements required_access;
    if (query.type == ASTKillQueryQuery::Type::Query)
    {
        /// `AccessType::CLUSTER`, checked unconditionally by `executeDDLQueryOnCluster`, still applies.
        if (!trySelfKillTarget(query, getContext()))
            required_access.emplace_back(AccessType::KILL_QUERY);
    }
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
