#include "executeQuery.h"
#include <IO/Progress.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Core/Block.h>
#include <Poco/UUIDGenerator.h>

namespace DB
{

namespace
{

void checkFulfilledConditionsAndUpdate(
    const Progress & progress, RemoteBlockInputStream & stream,
    TestStats & statistics, TestStopConditions & stop_conditions,
    InterruptListener & interrupt_listener, T_test & t_test)
{
    statistics.add(progress.read_rows, progress.read_bytes);

    stop_conditions.reportRowsRead(statistics.total_rows_read);
    stop_conditions.reportBytesReadUncompressed(statistics.total_bytes_read);
    stop_conditions.reportTotalTime(statistics.watch.elapsed() / (1000 * 1000));
    stop_conditions.reportMinTimeNotChangingFor(statistics.min_time_watch.elapsed() / (1000 * 1000));
    stop_conditions.reportMaxSpeedNotChangingFor(statistics.max_rows_speed_watch.elapsed() / (1000 * 1000));
    stop_conditions.reportAverageSpeedNotChangingFor(statistics.avg_rows_speed_watch.elapsed() / (1000 * 1000));
    stop_conditions.reportTtest(t_test);

    if (stop_conditions.areFulfilled())
    {
        statistics.last_query_was_cancelled = true;
        stream.cancel(false);
    }

    if (interrupt_listener.check())
    {
        statistics.got_SIGINT = true;
        statistics.last_query_was_cancelled = true;
        stream.cancel(false);
    }
}

} // anonymous namespace

void executeQuery(
    Connections & connections,
    const std::string & query,
    TestStatsPtrs & statistics,
    T_test & t_test,
    TestStopConditions & stop_conditions,
    InterruptListener & interrupt_listener,
    Context & context,
    const Settings & settings,
    size_t connection_index)
{
    static const std::string query_id_prefix = Poco::UUIDGenerator::defaultGenerator().create().toString() + "-";
    static int next_query_id = 1;

    statistics[connection_index]->watch_per_query.restart();
    statistics[connection_index]->last_query_was_cancelled = false;
    statistics[connection_index]->last_query_rows_read = 0;
    statistics[connection_index]->last_query_bytes_read = 0;
    statistics[connection_index]->query_id = query_id_prefix + std::to_string(next_query_id++);

    RemoteBlockInputStream stream(*connections[connection_index], query, {}, context, &settings);
    stream.setQueryId(statistics[connection_index]->query_id);

    stream.setProgressCallback(
        [&](const Progress & value)
        {
            checkFulfilledConditionsAndUpdate(
                value, stream, *statistics[connection_index],
                stop_conditions, interrupt_listener, t_test);
        });
    stream.readPrefix();
    while (Block block = stream.read());
    stream.readSuffix();
    double seconds = statistics[connection_index]->watch_per_query.elapsedSeconds();

    if (!statistics[connection_index]->last_query_was_cancelled)
        statistics[connection_index]->updateQueryInfo();

//    statistics.setTotalTime();
    statistics[connection_index]->total_time += seconds;
    t_test.add(connection_index, seconds);

}

}
