#include "executeQuery.h"
#include <IO/Progress.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Core/Block.h>

namespace DB
{
namespace
{

void checkFulfilledConditionsAndUpdate(
    const Progress & progress, RemoteBlockInputStream & stream,
    TestStats & statistics, TestStopConditions & stop_conditions,
    InterruptListener & interrupt_listener)
{
    statistics.add(progress.rows, progress.bytes);

    stop_conditions.reportRowsRead(statistics.total_rows_read);
    stop_conditions.reportBytesReadUncompressed(statistics.total_bytes_read);
    stop_conditions.reportTotalTime(statistics.watch.elapsed() / (1000 * 1000));
    stop_conditions.reportMinTimeNotChangingFor(statistics.min_time_watch.elapsed() / (1000 * 1000));
    stop_conditions.reportMaxSpeedNotChangingFor(statistics.max_rows_speed_watch.elapsed() / (1000 * 1000));
    stop_conditions.reportAverageSpeedNotChangingFor(statistics.avg_rows_speed_watch.elapsed() / (1000 * 1000));

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

}

void executeQuery(
    Connection & connection,
    const std::string & query,
    TestStats & statistics,
    TestStopConditions & stop_conditions,
    InterruptListener & interrupt_listener,
    Context & context,
    const Settings & settings)
{
    statistics.watch_per_query.restart();
    statistics.last_query_was_cancelled = false;
    statistics.last_query_rows_read = 0;
    statistics.last_query_bytes_read = 0;

    RemoteBlockInputStream stream(connection, query, {}, context, &settings);

    stream.setProgressCallback(
        [&](const Progress & value)
        {
            checkFulfilledConditionsAndUpdate(
                value, stream, statistics,
                stop_conditions, interrupt_listener);
        });
    stream.readPrefix();
    while (Block block = stream.read());
    stream.readSuffix();

    if (!statistics.last_query_was_cancelled)
        statistics.updateQueryInfo();

    statistics.setTotalTime();
}
}
