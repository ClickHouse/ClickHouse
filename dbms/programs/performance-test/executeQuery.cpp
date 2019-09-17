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
        const Progress & progress,
        RemoteBlockInputStream & stream,
        ConnectionTestStats & statistics,
        ConnectionTestStopConditions & stop_conditions,
        InterruptListener & interrupt_listener)
{
    statistics.add(progress.read_rows, progress.read_bytes);

    stop_conditions.reportRowsRead(statistics.total_rows_read);
    stop_conditions.reportBytesReadUncompressed(statistics.total_bytes_read);
    stop_conditions.reportTotalTime(statistics.watch.elapsedMilliseconds());
    stop_conditions.reportMinTimeNotChangingFor(statistics.min_time_watch.elapsedMilliseconds());
    stop_conditions.reportMaxSpeedNotChangingFor(statistics.max_rows_speed_watch.elapsedMilliseconds());
    stop_conditions.reportAverageSpeedNotChangingFor(statistics.avg_rows_speed_watch.elapsedMilliseconds());

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
        const Connections & connections,
        const std::string & query,
        TestStats & statistics,
        StudentTTest & t_test,
        TestStopConditions & stop_conditions,
        InterruptListener & interrupt_listener,
        Context & context,
        const Settings & settings,
        size_t connection_index)
{
    static const std::string query_id_prefix = Poco::UUIDGenerator::defaultGenerator().create().toString() + "-";
    static int next_query_id = 1;

    ConnectionTestStats & statistic = statistics[connection_index];

    statistic.last_query_was_cancelled = false;
    statistic.last_query_rows_read = 0;
    statistic.last_query_bytes_read = 0;
    statistic.query_id = query_id_prefix + std::to_string(next_query_id++);
    statistic.watch_per_query.restart();

    RemoteBlockInputStream stream(*connections[connection_index], query, {}, context, &settings);
    stream.setQueryId(statistic.query_id);

    stream.setProgressCallback(
        [&](const Progress & value)
        {
            checkFulfilledConditionsAndUpdate(value, stream, statistic, stop_conditions[connection_index], interrupt_listener);
        });
    stream.readPrefix();
    while (Block block = stream.read());
    stream.readSuffix();

    double seconds = statistic.watch_per_query.elapsedSeconds();
    if (!statistic.last_query_was_cancelled)
        statistic.updateQueryInfo();

    statistic.total_time += seconds;
    if (stop_conditions[connection_index].isInitializedTTestWithConfidenceLevel())
        t_test.add(connection_index, seconds);

}

}
