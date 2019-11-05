#include "PerformanceTest.h"

#include <Core/Types.h>
#include <Common/CpuId.h>
#include <common/getMemoryAmount.h>
#include <DataStreams/copyData.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>

#include <filesystem>

#include "executeQuery.h"


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{
void waitQuery(Connection & connection)
{
    bool finished = false;

    while (true)
    {
        if (!connection.poll(1000000))
            continue;

        Connection::Packet packet = connection.receivePacket();
        switch (packet.type)
        {
            case Protocol::Server::EndOfStream:
                finished = true;
                break;
            case Protocol::Server::Exception:
                throw *packet.exception;
        }

        if (finished)
            break;
    }
}
}

namespace fs = std::filesystem;

PerformanceTest::PerformanceTest(
    const XMLConfigurationPtr & config_,
    Connection & connection_,
    const ConnectionTimeouts & timeouts_,
    InterruptListener & interrupt_listener_,
    const PerformanceTestInfo & test_info_,
    Context & context_,
    const std::vector<size_t> & queries_to_run_)
    : config(config_)
    , connection(connection_)
    , timeouts(timeouts_)
    , interrupt_listener(interrupt_listener_)
    , test_info(test_info_)
    , context(context_)
    , queries_to_run(queries_to_run_)
    , log(&Poco::Logger::get("PerformanceTest"))
{
}

bool PerformanceTest::checkPreconditions() const
{
    if (!config->has("preconditions"))
        return true;

    Strings preconditions;
    config->keys("preconditions", preconditions);
    size_t table_precondition_index = 0;
    size_t cpu_precondition_index = 0;

    for (const std::string & precondition : preconditions)
    {
        if (precondition == "flush_disk_cache")
        {
            if (system(
                    "(>&2 echo 'Flushing disk cache...') && (sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches') && (>&2 echo 'Flushed.')"))
            {
                LOG_WARNING(log, "Failed to flush disk cache");
                return false;
            }
        }

        if (precondition == "ram_size")
        {
            size_t ram_size_needed = config->getUInt64("preconditions.ram_size");
            size_t actual_ram = getMemoryAmount();
            if (!actual_ram)
                throw Exception("ram_size precondition not available on this platform", ErrorCodes::NOT_IMPLEMENTED);

            if (ram_size_needed > actual_ram)
            {
                LOG_WARNING(log, "Not enough RAM: need = " << ram_size_needed << ", present = " << actual_ram);
                return false;
            }
        }

        if (precondition == "table_exists")
        {
            std::string precondition_key = "preconditions.table_exists[" + std::to_string(table_precondition_index++) + "]";
            std::string table_to_check = config->getString(precondition_key);
            std::string query = "EXISTS TABLE " + table_to_check + ";";

            size_t exist = 0;

            connection.sendQuery(timeouts, query, "", QueryProcessingStage::Complete, &test_info.settings, nullptr, false);

            while (true)
            {
                Connection::Packet packet = connection.receivePacket();

                if (packet.type == Protocol::Server::Data)
                {
                    for (const ColumnWithTypeAndName & column : packet.block)
                    {
                        if (column.name == "result" && column.column->size() > 0)
                        {
                            exist = column.column->get64(0);
                            if (exist)
                                break;
                        }
                    }
                }

                if (packet.type == Protocol::Server::Exception
                    || packet.type == Protocol::Server::EndOfStream)
                    break;
            }

            if (!exist)
            {
                LOG_WARNING(log, "Table " << table_to_check << " doesn't exist");
                return false;
            }
        }

        if (precondition == "cpu")
        {
            std::string precondition_key = "preconditions.cpu[" + std::to_string(cpu_precondition_index++) + "]";
            std::string flag_to_check = config->getString(precondition_key);

            #define CHECK_CPU_PRECONDITION(OP) \
            if (flag_to_check == #OP) \
            { \
                if (!Cpu::CpuFlagsCache::have_##OP) \
                { \
                    LOG_WARNING(log, "CPU doesn't support " << #OP); \
                    return false; \
                } \
            } else

            CPU_ID_ENUMERATE(CHECK_CPU_PRECONDITION)
            {
                LOG_WARNING(log, "CPU doesn't support " << flag_to_check);
                return false;
            }

            #undef CHECK_CPU_PRECONDITION
        }
    }

    return true;
}


UInt64 PerformanceTest::calculateMaxExecTime() const
{

    UInt64 result = 0;
    for (const auto & stop_conditions : test_info.stop_conditions_by_run)
    {
        UInt64 condition_max_time = stop_conditions.getMaxExecTime();
        if (condition_max_time == 0)
            return 0;
        result += condition_max_time;
    }
    return result;
}


void PerformanceTest::prepare() const
{
    for (const auto & query : test_info.create_and_fill_queries)
    {
        LOG_INFO(log, "Executing create or fill query \"" << query << '\"');
        connection.sendQuery(timeouts, query, "", QueryProcessingStage::Complete, &test_info.settings, nullptr, false);
        waitQuery(connection);
        LOG_INFO(log, "Query finished");
    }

}

void PerformanceTest::finish() const
{
    for (const auto & query : test_info.drop_queries)
    {
        LOG_INFO(log, "Executing drop query \"" << query << '\"');
        connection.sendQuery(timeouts, query, "", QueryProcessingStage::Complete, &test_info.settings, nullptr, false);
        waitQuery(connection);
        LOG_INFO(log, "Query finished");
    }
}

std::vector<TestStats> PerformanceTest::execute()
{
    std::vector<TestStats> statistics_by_run;
    size_t query_count;
    if (queries_to_run.empty())
        query_count = test_info.queries.size();
    else
        query_count = queries_to_run.size();
    size_t total_runs = test_info.times_to_run * test_info.queries.size();
    statistics_by_run.resize(total_runs);
    LOG_INFO(log, "Totally will run cases " << test_info.times_to_run * query_count << " times");
    UInt64 max_exec_time = calculateMaxExecTime();
    if (max_exec_time != 0)
        LOG_INFO(log, "Test will be executed for a maximum of " << max_exec_time / 1000. << " seconds");
    else
        LOG_INFO(log, "Test execution time cannot be determined");

    for (size_t number_of_launch = 0; number_of_launch < test_info.times_to_run; ++number_of_launch)
    {
        QueriesWithIndexes queries_with_indexes;

        for (size_t query_index = 0; query_index < test_info.queries.size(); ++query_index)
        {
            if (queries_to_run.empty() || std::find(queries_to_run.begin(), queries_to_run.end(), query_index) != queries_to_run.end())
            {
                size_t statistic_index = number_of_launch * test_info.queries.size() + query_index;
                queries_with_indexes.push_back({test_info.queries[query_index], statistic_index});
            }
            else
                LOG_INFO(log, "Will skip query " << test_info.queries[query_index] << " by index");
        }

        if (got_SIGINT)
            break;

        runQueries(queries_with_indexes, statistics_by_run);
    }

    if (got_SIGINT)
    {
        return statistics_by_run;
    }

    // Pull memory usage data from query log. The log is normally filled in
    // background, so we have to flush it synchronously here to see all the
    // previous queries.
    {
        NullBlockOutputStream null_output(Block{});
        RemoteBlockInputStream flush_log(connection, "system flush logs",
            {} /* header */, context);
        copyData(flush_log, null_output);
    }

    for (auto & statistics : statistics_by_run)
    {
        if (statistics.query_id.empty())
        {
            // We have statistics structs for skipped queries as well, so we
            // have to filter them out.
            continue;
        }

        // We run some test queries several times, specifying the same query id,
        // so this query to the log may return several records. Choose the
        // last one, because this is when the query performance has stabilized.
        RemoteBlockInputStream log_reader(connection,
            "select memory_usage, query_start_time from system.query_log "
            "where type = 2 and query_id = '" + statistics.query_id + "' "
            "order by query_start_time desc",
            {} /* header */, context);

        log_reader.readPrefix();
        Block block = log_reader.read();
        if (block.columns() == 0)
        {
            LOG_WARNING(log, "Query '" << statistics.query_id << "' is not found in query log.");
            continue;
        }

        auto column = block.getByName("memory_usage").column;
        statistics.memory_usage = column->get64(0);

        log_reader.readSuffix();
    }

    return statistics_by_run;
}

void PerformanceTest::runQueries(
    const QueriesWithIndexes & queries_with_indexes,
    std::vector<TestStats> & statistics_by_run)
{
    for (const auto & [query, run_index] : queries_with_indexes)
    {
        LOG_INFO(log, "[" << run_index<< "] Run query '" << query << "'");
        TestStopConditions & stop_conditions = test_info.stop_conditions_by_run[run_index];
        TestStats & statistics = statistics_by_run[run_index];
        statistics.startWatches();
        try
        {
            executeQuery(connection, query, statistics, stop_conditions, interrupt_listener, context, test_info.settings);

            if (test_info.exec_type == ExecutionType::Loop)
            {
                LOG_INFO(log, "Will run query in loop");
                for (size_t iteration = 1; !statistics.got_SIGINT; ++iteration)
                {
                    stop_conditions.reportIterations(iteration);
                    if (stop_conditions.areFulfilled())
                    {
                        LOG_INFO(log, "Stop conditions fullfilled");
                        break;
                    }

                    executeQuery(connection, query, statistics, stop_conditions, interrupt_listener, context, test_info.settings);
                }
            }
        }
        catch (const Exception & e)
        {
            statistics.exception = "Code: " + std::to_string(e.code()) + ", e.displayText() = " + e.displayText();
            LOG_WARNING(log, "Code: " << e.code() << ", e.displayText() = " << e.displayText()
                << ", Stack trace:\n\n" << e.getStackTrace().toString());
        }

        if (!statistics.got_SIGINT)
            statistics.ready = true;
        else
        {
            got_SIGINT = true;
            LOG_INFO(log, "Got SIGINT, will terminate as soon as possible");
            break;
        }
    }
}


}
