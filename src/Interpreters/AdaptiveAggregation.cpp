#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

void Aggregator::prepareStagedChunk(StagedChunk & block) const
{
    auto & payload = std::get<StagedChunk::AggregatePayload>(block.payload);

    auto prep = std::make_unique<StagedChunkPreparation>();
    prep->aggregate_columns.resize(params.aggregates_size);
    prepareAggregateInstructions(
        payload.argument_columns, prep->aggregate_columns, prep->materialized_columns, prep->instructions, prep->nested_columns_holder);

    payload.prepared = std::move(prep);
}

void Aggregator::initAdaptiveSession(AggregatedDataVariants & local_result, AdaptiveAggregationSession & shared) const
{
    auto early_drain_variants = std::make_shared<AggregatedDataVariants>();
    early_drain_variants->aggregator = this;
    early_drain_variants->keys_size = params.keys_size;
    early_drain_variants->key_sizes = key_sizes;
    early_drain_variants->init(convertToTwoLevelTypeIfPossible(local_result.type));

    shared.early_drain_variants = std::move(early_drain_variants);
    shared.initialized.store(true, std::memory_order_release);
}

std::unique_ptr<AdaptiveAggregationProducer> Aggregator::createAdaptiveProducer(AdaptiveAggregationSessionPtr session) const
{
    /// The owner (the producing transform) installs the staging sink right after this call:
    /// the destination is transport policy, not the aggregator's.
    return std::make_unique<AdaptiveAggregationProducer>(
        std::move(session), StagedChunkBuilder(aggregates_positions, params.aggregates_size, log));
}

void StagedChunkBacklogSink::consume(MutableStagedChunkPtr chunk)
{
    aggregator.publishStagedChunk(session, std::move(chunk));
}

void Aggregator::publishStagedChunk(
    AdaptiveAggregationSession & shared, MutableStagedChunkPtr block) const
{
    chassert(block->wellFormed());

    /// The transport prepared the chunk on the producing thread (see the pipeline staging
    /// sink), where the preparation parallelizes across producers, so the chunk is immutable
    /// by the time any bucket can see it.
    chassert(
        !std::holds_alternative<StagedChunk::AggregatePayload>(block->payload)
        || std::get<StagedChunk::AggregatePayload>(block->payload).prepared);

    shared.backlog.publish(std::move(block));
}

void Aggregator::flushStaging(AdaptiveAggregationProducer & adaptive) const
{
    adaptive.staging.flush(*adaptive.staging_sink);
}

/// The flushed variants' sizes are meaningless by the time the external path finishes, so a
/// stored entry keeps its sizes: only the verdict is written, and only when the session staged
/// enough records to trust the thaw sampler. Runs without a measurement leave the entry alone.
void Aggregator::recordAdaptiveStagingVerdict(AdaptiveAggregationSession & shared) const
{
    const auto & stats_params = params.stats_collecting_params;
    if (!stats_params.isCollectionAndUseEnabled())
        return;

    const auto measurement = shared.thaw_sampler.measure();
    if (!measurement.measured)
        return;
    const bool wasteful_staging = measurement.wasteful_staging;

    auto & stats = getHashTablesStatistics<AggregationEntry>();
    AggregationEntry entry{.sum_of_sizes = 0, .median_size = 0, .adaptive_staging_wasteful = wasteful_staging};
    if (const auto prev = stats.getSizeHint(stats_params))
    {
        entry.sum_of_sizes = prev->sum_of_sizes;
        entry.median_size = prev->median_size;
    }
    stats.update(entry, stats_params);
}

}
