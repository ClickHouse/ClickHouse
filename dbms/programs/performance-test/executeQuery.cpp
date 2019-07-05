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
    InterruptListener & interrupt_listener)
{
    statistics.add(progress.read_rows, progress.read_bytes);

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

} // anonymous namespace

void executeQuery(
    Connection & connection,
    const std::string & query,
    TestStats & statistics,
    TestStopConditions & stop_conditions,
    InterruptListener & interrupt_listener,
    Context & context,
    const Settings & settings)
{
    static const std::string query_id_prefix
        = Poco::UUIDGenerator::defaultGenerator().create().toString() + "-";
    static int next_query_id = 1;

    statistics.watch_per_query.restart();
    statistics.last_query_was_cancelled = false;
    statistics.last_query_rows_read = 0;
    statistics.last_query_bytes_read = 0;
    statistics.query_id = query_id_prefix + std::to_string(next_query_id++);

    fprintf(stderr, "Query id is '%s'\n", statistics.query_id.c_str());
    RemoteBlockInputStream stream(connection, query, {}, context, &settings);
    stream.setQueryId(statistics.query_id);

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

    /// Get max memory usage from the server query log.
    /// We might have to wait for some time before the query log is updated.
    int n_waits = 0;
    const int one_wait_us = 500 * 1000;
    const int max_waits = (10 * 1000 * 1000) / one_wait_us;
    for (; n_waits < max_waits; n_waits++)
    {
        RemoteBlockInputStream log(connection,
                                   "select memory_usage from system.query_log where type = 2 and query_id = '"
                                   + statistics.query_id + "'",
        {}, context, &settings);

        log.readPrefix();
        Block block = log.read();
        if (block.columns() == 0)
        {
            log.readSuffix();
            ::usleep(one_wait_us);
            continue;
        }
        assert(block.columns() == 1);
        assert(block.getDataTypes()[0]->getName() == "UInt64");
        ColumnPtr column = block.getByPosition(0).column;
        assert(column->size() == 1);
        StringRef ref = column->getDataAt(0);
        assert(ref.size == sizeof(UInt64));
        const UInt64 memory_usage = *reinterpret_cast<const UInt64*>(ref.data);
        statistics.max_memory_usage = std::max(statistics.max_memory_usage,
                                               memory_usage);
        statistics.min_memory_usage = std::min(statistics.min_memory_usage,
                                               memory_usage);
        log.readSuffix();

        fprintf(stderr, "Memory usage is %ld\n", memory_usage);
        break;
    }
    fprintf(stderr, "Waited for query log for %.2fs\n",
            (n_waits * one_wait_us) / 1e6f);
}

}
