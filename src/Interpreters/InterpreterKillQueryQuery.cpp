#include <Interpreters/InterpreterKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/IStorage.h>
#include <Common/quoteString.h>
#include <thread>
#include <iostream>
#include <cstddef>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ACCESS_DENIED;
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
        if ((my_client.current_query_id == query_id_col.getDataAt(i).toString())
            && (my_client.current_user == user_col.getDataAt(i).toString()))
            continue;

        auto query_id = query_id_col.getDataAt(i).toString();
        query_user = user_col.getDataAt(i).toString();

        if ((my_client.current_user != query_user) && !is_kill_query_granted())
            continue;

        res.emplace_back(std::move(query_id), query_user, i, false);
    }

    if (res.empty() && access_denied)
        throw Exception("User " + my_client.current_user + " attempts to kill query created by " + query_user, ErrorCodes::ACCESS_DENIED);

    return res;
}


class SyncKillQueryInputStream : public IBlockInputStream
{
public:
    SyncKillQueryInputStream(ProcessList & process_list_, QueryDescriptors && processes_to_stop_, Block && processes_block_,
                             const Block & res_sample_block_)
        : process_list(process_list_),
        processes_to_stop(std::move(processes_to_stop_)),
        processes_block(std::move(processes_block_)),
        res_sample_block(res_sample_block_)
    {
        addTotalRowsApprox(processes_to_stop.size());
    }

    String getName() const override
    {
        return "SynchronousQueryKiller";
    }

    Block getHeader() const override { return res_sample_block; }

    Block readImpl() override
    {
        size_t num_result_queries = processes_to_stop.size();

        if (num_processed_queries >= num_result_queries)
            return Block();

        MutableColumns columns = res_sample_block.cloneEmptyColumns();

        do
        {
            for (auto & curr_process : processes_to_stop)
            {
                if (curr_process.processed)
                    continue;

                auto code = process_list.sendCancelToQuery(curr_process.query_id, curr_process.user, true);

                if (code != CancellationCode::QueryIsNotInitializedYet && code != CancellationCode::CancelSent)
                {
                    curr_process.processed = true;
                    insertResultRow(curr_process.source_num, code, processes_block, res_sample_block, columns);
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

        return res_sample_block.cloneWithColumns(std::move(columns));
    }

    ProcessList & process_list;
    QueryDescriptors processes_to_stop;
    Block processes_block;
    Block res_sample_block;
    size_t num_processed_queries = 0;
};


BlockIO InterpreterKillQueryQuery::execute()
{
    const auto & query = query_ptr->as<ASTKillQueryQuery &>();

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccessForDDLOnCluster());

    BlockIO res_io;
    switch (query.type)
    {
    case ASTKillQueryQuery::Type::Query:
    {
        Block processes_block = getSelectResult("query_id, user, query", "system.processes");
        if (!processes_block)
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
                auto code = (query.test) ? CancellationCode::Unknown : process_list.sendCancelToQuery(query_desc.query_id, query_desc.user, true);
                insertResultRow(query_desc.source_num, code, processes_block, header, res_columns);
            }

            res_io.in = std::make_shared<OneBlockInputStream>(header.cloneWithColumns(std::move(res_columns)));
        }
        else
        {
            res_io.in = std::make_shared<SyncKillQueryInputStream>(
                process_list, std::move(queries_to_stop), std::move(processes_block), header);
        }

        break;
    }
    case ASTKillQueryQuery::Type::Mutation:
    {
        Block mutations_block = getSelectResult("database, table, mutation_id, command", "system.mutations");
        if (!mutations_block)
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
            table_id = StorageID{database_col.getDataAt(i).toString(), table_col.getDataAt(i).toString()};
            auto mutation_id = mutation_id_col.getDataAt(i).toString();

            CancellationCode code = CancellationCode::Unknown;
            if (!query.test)
            {
                auto storage = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
                if (!storage)
                    code = CancellationCode::NotFound;
                else
                {
                    ParserAlterCommand parser;
                    auto command_ast
                        = parseQuery(parser, command_col.getDataAt(i).toString(), 0, getContext()->getSettingsRef().max_parser_depth);
                    required_access_rights = InterpreterAlterQuery::getRequiredAccessForCommand(
                        command_ast->as<const ASTAlterCommand &>(), table_id.database_name, table_id.table_name);
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
            throw Exception(
                "Not allowed to kill mutation. To execute this query it's necessary to have the grant " + required_access_rights.toString(),
                ErrorCodes::ACCESS_DENIED);

        res_io.in = std::make_shared<OneBlockInputStream>(header.cloneWithColumns(std::move(res_columns)));

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
        select_query += " WHERE " + queryToString(where_expression);

    auto stream = executeQuery(select_query, getContext(), true).getInputStream();
    Block res = stream->read();

    if (res && stream->read())
        throw Exception("Expected one block from input stream", ErrorCodes::LOGICAL_ERROR);

    return res;
}


AccessRightsElements InterpreterKillQueryQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<ASTKillQueryQuery &>();
    AccessRightsElements required_access;
    if (query.type == ASTKillQueryQuery::Type::Query)
        required_access.emplace_back(AccessType::KILL_QUERY);
    else if (query.type == ASTKillQueryQuery::Type::Mutation)
        required_access.emplace_back(AccessType::ALTER_UPDATE | AccessType::ALTER_DELETE | AccessType::ALTER_MATERIALIZE_INDEX | AccessType::ALTER_MATERIALIZE_TTL);
    return required_access;
}

}
