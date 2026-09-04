#include <Storages/Kafka/Kafka2Source.h>

#include <Storages/Kafka/StorageKafka2.h>

#include <Interpreters/ExpressionActions.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

Kafka2Source::Kafka2Source(
    StorageKafka2 & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    LoggerPtr log_,
    size_t max_block_size_,
    size_t consumer_index_,
    bool commit_in_suffix_)
    : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns)))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , log(log_)
    , max_block_size(max_block_size_)
    , consumer_index(consumer_index_)
    , commit_in_suffix(commit_in_suffix_)
{
}

Kafka2Source::~Kafka2Source()
{
    if (consumer)
    {
        auto component_guard = Coordination::setCurrentComponent("Kafka2Source::~Kafka2Source");

        /// If `commit` was not called, the `OffsetGuard` destructor rolls back.
        offset_guard.reset();
        storage.releaseConsumer(std::move(consumer));
    }

    storage.active_direct_readers.fetch_sub(1);
}

Chunk Kafka2Source::generate()
{
    auto chunk = generateImpl();
    if (!chunk && commit_in_suffix)
        commit();

    return chunk;
}

Chunk Kafka2Source::generateImpl()
{
    auto component_guard = Coordination::setCurrentComponent("Kafka2Source::generateImpl");

    if (!consumer)
    {
        consumer = storage.acquireConsumer(consumer_index);

        if (consumer->needsNewKeeper())
            consumer->setKeeper(storage.getZooKeeperAndAssertActive());

        if (const auto cannot_poll_reason = consumer->prepareToPoll(); cannot_poll_reason.has_value())
        {
            LOG_DEBUG(log, "Cannot poll consumer for direct read");
            return {};
        }
    }

    if (is_finished)
        return {};

    is_finished = true;

    auto maybe_blocks_and_guard = storage.pollConsumer(*consumer, total_stopwatch, context, max_block_size);

    if (!maybe_blocks_and_guard.has_value())
        return {};

    offset_guard.emplace(std::move(maybe_blocks_and_guard->guard));

    if (maybe_blocks_and_guard->blocks.empty())
    {
        /// Messages were consumed but produced no rows (e.g. all went to dead-letter queue).
        /// We still keep the offset guard so that commit can advance offsets.
        return {};
    }

    auto result_block = std::move(maybe_blocks_and_guard->blocks.front());

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    converting_actions->execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}

void Kafka2Source::commit()
{
    if (!consumer)
        return;

    auto component_guard = Coordination::setCurrentComponent("Kafka2Source::commit");

    if (offset_guard.has_value())
    {
        offset_guard->commit();
        offset_guard.reset();
    }
}

}
