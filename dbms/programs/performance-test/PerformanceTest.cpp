#include "PerformanceTest.h"

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <common/getMemoryAmount.h>
#include <Core/Types.h>
#include <boost/filesystem.hpp>
#include "executeQuery.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
}

namespace fs = boost::filesystem;

PerformanceTest::PerformanceTest(
    const XMLConfigurationPtr & config_,
    Connection & connection_,
    InterruptListener & interrupt_listener_,
    const PerformanceTestInfo & test_info_)
    : config(config_)
    , connection(connection_)
    , interrupt_listener(interrupt_listener_)
    , test_info(test_info_)
{
}

bool PerformanceTest::checkPreconditions() const
{
    if (!config->has("preconditions"))
        return true;

    std::vector<std::string> preconditions;
    config->keys("preconditions", preconditions);
    size_t table_precondition_index = 0;

    for (const String & precondition : preconditions)
    {
        if (precondition == "flush_disk_cache")
        {
            if (system(
                    "(>&2 echo 'Flushing disk cache...') && (sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches') && (>&2 echo 'Flushed.')"))
            {
                std::cerr << "Failed to flush disk cache" << std::endl;
                return false;
            }
        }

        if (precondition == "ram_size")
        {
            size_t ram_size_needed = config->getUInt64("preconditions.ram_size");
            size_t actual_ram = getMemoryAmount();
            if (!actual_ram)
                throw DB::Exception("ram_size precondition not available on this platform", DB::ErrorCodes::NOT_IMPLEMENTED);

            if (ram_size_needed > actual_ram)
            {
                std::cerr << "Not enough RAM: need = " << ram_size_needed << ", present = " << actual_ram << std::endl;
                return false;
            }
        }

        if (precondition == "table_exists")
        {
            String precondition_key = "preconditions.table_exists[" + std::to_string(table_precondition_index++) + "]";
            String table_to_check = config->getString(precondition_key);
            String query = "EXISTS TABLE " + table_to_check + ";";

            size_t exist = 0;

            connection.sendQuery(query, "", QueryProcessingStage::Complete, &test_info.settings, nullptr, false);

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
                std::cerr << "Table " << table_to_check << " doesn't exist" << std::endl;
                return false;
            }
        }
    }

    return true;
}



std::vector<TestStats> PerformanceTest::execute()
{
    std::vector<TestStats> statistics_by_run;
    statistics_by_run.resize(test_info.times_to_run * test_info.queries.size());
    for (size_t number_of_launch = 0; number_of_launch < test_info.times_to_run; ++number_of_launch)
    {
        QueriesWithIndexes queries_with_indexes;

        for (size_t query_index = 0; query_index < test_info.queries.size(); ++query_index)
        {
            size_t statistic_index = number_of_launch * test_info.queries.size() + query_index;
            test_info.stop_conditions_by_run[statistic_index].reset();

            queries_with_indexes.push_back({test_info.queries[query_index], statistic_index});
        }

        if (interrupt_listener.check())
            break;

        runQueries(queries_with_indexes, statistics_by_run);
    }
    return statistics_by_run;
}


void PerformanceTest::runQueries(
    const QueriesWithIndexes & queries_with_indexes,
    std::vector<TestStats> & statistics_by_run)
{
    for (const auto & [query, run_index] : queries_with_indexes)
    {
        TestStopConditions & stop_conditions = test_info.stop_conditions_by_run[run_index];
        TestStats & statistics = statistics_by_run[run_index];

        statistics.clear();
        try
        {
            executeQuery(connection, query, statistics, stop_conditions, interrupt_listener);

            if (test_info.exec_type == ExecutionType::Loop)
            {
                for (size_t iteration = 1; !statistics.got_SIGINT; ++iteration)
                {
                    stop_conditions.reportIterations(iteration);
                    if (stop_conditions.areFulfilled())
                        break;

                    executeQuery(connection, query, statistics, stop_conditions, interrupt_listener);
                }
            }
        }
        catch (const DB::Exception & e)
        {
            statistics.exception = e.what() + String(", ") + e.displayText();
        }

        if (!statistics.got_SIGINT)
            statistics.ready = true;
    }
}


}
