#include <Interpreters/InterpreterKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <thread>
#include <iostream>
#include <cstddef>


namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int LOGICAL_ERROR;
}


using CancellationCode = ProcessList::CancellationCode;

static const char * cancellationCodeToStatus(CancellationCode code)
{
    switch (code)
    {
        case CancellationCode::NotFound:
            return "finished";
        case CancellationCode::QueryIsNotInitializedYet:
            return "pending";
        case CancellationCode::CancelCannotBeSent:
            return "error";
        case CancellationCode::CancelSent:
            return "waiting";
        default:
            return "unknown_status";
    };
}


struct QueryDescriptor
{
    String query_id;
    String user;
    size_t source_num;
    bool processed = false;

    QueryDescriptor(String && query_id_, String && user_, size_t source_num_, bool processed_ = false)
        : query_id(std::move(query_id_)), user(std::move(user_)), source_num(source_num_), processed(processed_) {}
};

using QueryDescriptors = std::vector<QueryDescriptor>;


static void insertResultRow(size_t n, CancellationCode code, const Block & source_processes, Block & res)
{
    res.getByPosition(0).column->insert(String(cancellationCodeToStatus(code)));

    for (size_t col_num = 1; col_num < res.columns(); ++col_num)
    {
        ColumnWithTypeAndName & dst_col = res.getByPosition(col_num);
        dst_col.column->insertFrom(*source_processes.getByName(dst_col.name).column, n);
    }
}

static QueryDescriptors extractQueriesExceptMeAndCheckAccess(const Block & processes_block, Context & context)
{
    QueryDescriptors res;
    size_t num_processes = processes_block.rows();
    res.reserve(num_processes);

    const ColumnString & query_id_col = typeid_cast<const ColumnString &>(*processes_block.getByName("query_id").column);
    const ColumnString & user_col = typeid_cast<const ColumnString &>(*processes_block.getByName("user").column);
    const ClientInfo & my_client = context.getProcessListElement()->client_info;

    for (size_t i = 0; i < num_processes; ++i)
    {
        auto query_id = query_id_col.getDataAt(i).toString();
        auto user = user_col.getDataAt(i).toString();

        if (my_client.current_query_id == query_id && my_client.current_user == user)
            continue;

        if (context.getSettingsRef().limits.readonly && my_client.current_user != user)
        {
            throw Exception("Readonly user " + my_client.current_user + " attempts to kill query created by " + user,
                    ErrorCodes::READONLY);
        }

        res.emplace_back(std::move(query_id), std::move(user), i, false);
    }

    return res;
}



class SyncKillQueryInputStream : public IProfilingBlockInputStream
{
public:

    SyncKillQueryInputStream(ProcessList & process_list_, QueryDescriptors && processes_to_stop_, Block && processes_block_,
                             const Block & res_sample_block_)
    :     process_list(process_list_),
        processes_to_stop(std::move(processes_to_stop_)),
        processes_block(std::move(processes_block_)),
        res_sample_block(res_sample_block_)
    {
        total_rows_approx = processes_to_stop.size();
    }

    String getName() const override
    {
        return "SynchronousQueryKiller";
    }

    String getID() const override
    {
        return "SynchronousQueryKiller_" + toString(intptr_t(this));
    }

    Block readImpl() override
    {
        size_t num_result_queries = processes_to_stop.size();

        if (num_processed_queries >= num_result_queries)
            return Block();

        Block res = res_sample_block.cloneEmpty();

        do
        {
            for (auto & curr_process : processes_to_stop)
            {
                if (curr_process.processed)
                    continue;

                auto code = process_list.sendCancelToQuery(curr_process.query_id, curr_process.user);

                if (code != CancellationCode::QueryIsNotInitializedYet && code != CancellationCode::CancelSent)
                {
                    curr_process.processed = true;
                    insertResultRow(curr_process.source_num, code, processes_block, res);
                    ++num_processed_queries;
                }
                /// Wait if QueryIsNotInitializedYet or CancelSent
            }

            /// KILL QUERY could be killed also
            /// Probably interpreting KILL QUERIES as complete (not internal) queries is extra functionality
            if (is_cancelled)
                break;

            /// Sleep if there are unprocessed queries
            if (num_processed_queries < num_result_queries)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));

        /// Don't produce empty block
        } while (res.rows() == 0);

        return res;
    }

    ProcessList & process_list;
    QueryDescriptors processes_to_stop;
    Block processes_block;
    Block res_sample_block;
    size_t num_processed_queries = 0;
};


BlockIO InterpreterKillQueryQuery::execute()
{
    ASTKillQueryQuery & query = typeid_cast<ASTKillQueryQuery &>(*query_ptr);

    BlockIO res_io;
    Block processes_block = getSelectFromSystemProcessesResult();
    if (!processes_block)
        return res_io;

    ProcessList & process_list = context.getProcessList();
    QueryDescriptors queries_to_stop = extractQueriesExceptMeAndCheckAccess(processes_block, context);

    res_io.in_sample = processes_block.cloneEmpty();
    res_io.in_sample.insert(0, {std::make_shared<ColumnString>(), std::make_shared<DataTypeString>(), "kill_status"});

    if (!query.sync || query.test)
    {
        Block res = res_io.in_sample.cloneEmpty();

        for (const auto & query_desc : queries_to_stop)
        {
            auto code = (query.test) ? CancellationCode::Unknown : process_list.sendCancelToQuery(query_desc.query_id, query_desc.user);
            insertResultRow(query_desc.source_num, code, processes_block, res);
        }

        res_io.in = std::make_shared<OneBlockInputStream>(res);
    }
    else
    {
        res_io.in = std::make_shared<SyncKillQueryInputStream>(
            process_list, std::move(queries_to_stop), std::move(processes_block), res_io.in_sample);
    }

    return res_io;
}

Block InterpreterKillQueryQuery::getSelectFromSystemProcessesResult()
{
    String system_processes_query = "SELECT query_id, user, query FROM system.processes WHERE "
        + queryToString(static_cast<ASTKillQueryQuery &>(*query_ptr).where_expression);

    BlockIO system_processes_io = executeQuery(system_processes_query, context, true);
    Block res = system_processes_io.in->read();

    if (res && system_processes_io.in->read())
        throw Exception("Expected one block from input stream", ErrorCodes::LOGICAL_ERROR);

    return res;
}


}
