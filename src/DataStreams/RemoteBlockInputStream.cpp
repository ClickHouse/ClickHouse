#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/Context.h>

namespace DB
{

RemoteBlockInputStream::RemoteBlockInputStream(
        Connection & connection,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings,
        const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : query_executor(connection, query_, header_, context_, settings, throttler, scalars_, external_tables_, stage_)
{
    init();
}

RemoteBlockInputStream::RemoteBlockInputStream(
        std::vector<IConnectionPool::Entry> && connections,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings,
        const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : query_executor(std::move(connections), query_, header_, context_, settings, throttler, scalars_, external_tables_, stage_)
{
    init();
}

RemoteBlockInputStream::RemoteBlockInputStream(
        const ConnectionPoolWithFailoverPtr & pool,
        const String & query_, const Block & header_, const Context & context_, const Settings * settings,
        const ThrottlerPtr & throttler, const Scalars & scalars_, const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : query_executor(pool, query_, header_, context_, settings, throttler, scalars_, external_tables_, stage_)
{
    init();
}

void RemoteBlockInputStream::init()
{
    query_executor.setProgressCallback([this](const Progress & progress) { progressImpl(progress); });
    query_executor.setProfileInfoCallback([this](const BlockStreamProfileInfo & info_) { info.setFrom(info_, true); });
    query_executor.setLogger(log);
}

void RemoteBlockInputStream::readPrefix()
{
    query_executor.sendQuery();
}

void RemoteBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;

    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    query_executor.cancel();
}

Block RemoteBlockInputStream::readImpl()
{
    auto block = query_executor.read();

    if (isCancelledOrThrowIfKilled())
        return Block();

    return block;
}

void RemoteBlockInputStream::readSuffixImpl()
{
    query_executor.finish();
}

}
