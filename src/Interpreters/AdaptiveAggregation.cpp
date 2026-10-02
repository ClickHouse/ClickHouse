#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationBucketsRetired;
    extern const Event AdaptiveAggregationSpills;
    extern const Event AdaptiveAggregationSpilledRecords;
    extern const Event AdaptiveAggregationSpilledBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void Aggregator::initAdaptiveSession(AdaptiveAggregationSession & shared) const
{
    shared.layout = AdaptivePartitionLayout::forProducers(params.max_threads, params.max_bytes_before_external_group_by);

    if (tmp_data && params.max_bytes_before_external_group_by)
    {
        /// An eighth of the threshold over the three buffers of each of the streams.
        const size_t buffers_share = params.max_bytes_before_external_group_by / (8 * 3 * ADAPTIVE_AGGREGATION_NUM_BUCKETS);
        const size_t buffer_bytes = std::min(tmp_data->getSettings().buffer_size, std::max(buffers_share, adaptive_spill_min_buffer_bytes));
        shared.spill_scope = tmp_data->childScope(tmp_data->getSettings().metrics, buffer_bytes);
    }
    shared.initialized.store(true, std::memory_order_release);
}

void Aggregator::finishAdaptiveProducer(AdaptiveAggregationProducer & adaptive) const
{
    /// A producer that never froze staged nothing.
    if (!adaptive.partitions)
        return;

    adaptive.partitions->finishAppending();
    auto & shared = *adaptive.session;
    std::lock_guard lock(shared.producer_buffers_mutex);
    shared.producer_buffers.push_back(std::move(adaptive.partitions));
}

void Aggregator::spillAdaptivePartitions(AdaptiveAggregationProducer & adaptive) const
{
    auto & partitions = *adaptive.partitions;
    auto & shared = *adaptive.session;
    if (!shared.spill_scope)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write to temporary file because temporary file is not initialized");

    const size_t partitions_per_bucket = partitions.layout().partitionsPerBucket();
    partitions.finishAppending();

    /// The producers that cross the threshold together start at different buckets, so they do not queue on the same
    /// stream's lock bucket after bucket.
    const size_t first_bucket = (reinterpret_cast<uintptr_t>(&adaptive) >> 6) % ADAPTIVE_AGGREGATION_NUM_BUCKETS;
    size_t spilled_records = 0;
    size_t spilled_bytes = 0;
    for (size_t i = 0; i < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++i)
    {
        const size_t bucket = (first_bucket + i) % ADAPTIVE_AGGREGATION_NUM_BUCKETS;
        const size_t first_partition = bucket * partitions_per_bucket;

        bool has_records = false;
        for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
            has_records |= partitions.hasRecords(first_partition + sub);
        if (!has_records)
            continue;

        auto & spilled = shared.spilled_buckets[bucket];
        {
            std::lock_guard lock(spilled.mutex);
            if (!spilled.stream)
                spilled.stream = std::make_unique<TemporaryDataBuffer>(shared.spill_scope);
            auto & out = *spilled.stream;

            for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
            {
                const size_t partition = first_partition + sub;
                if (!partitions.hasRecords(partition))
                    continue;

                UInt64 bytes = 0;
                partitions.forEachChunk(partition, [&](std::string_view chunk) { bytes += chunk.size(); });
                const UInt64 records = partitions.recordsOf(partition);

                writeBinaryLittleEndian(static_cast<UInt32>(sub), out);
                writeBinaryLittleEndian(records, out);
                writeBinaryLittleEndian(bytes, out);
                partitions.forEachChunk(partition, [&](std::string_view chunk) { out.write(chunk.data(), chunk.size()); });

                spilled_records += records;
                spilled_bytes += bytes;
            }
        }

        for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
            partitions.releasePartition(first_partition + sub);
    }

    partitions.resetHeldBytes();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpills);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledRecords, spilled_records);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationSpilledBytes, spilled_bytes);
    LOG_TRACE(log, "Adaptive aggregation: spilled {} staged records ({} bytes)", spilled_records, spilled_bytes);
}

void Aggregator::retireAdaptiveMergedBucket(AggregatedDataVariants & dest, size_t bucket) const
{
    dest.adaptive_merge_bucket_arenas[bucket].reset();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationBucketsRetired);
}

/// The flushed variants' sizes are meaningless by the time the external path finishes, so a
/// stored entry keeps its sizes: only the verdict is written, and only when the session staged
/// enough records to trust the thaw sampler. Runs without a measurement leave the entry alone.
void Aggregator::recordAdaptiveStagingVerdict(AdaptiveAggregationSession & shared) const
{
    const auto & stats_params = params.stats_collecting_params;
    if (!stats_params.isCollectionAndUseEnabled())
        return;

    bool measured = false;
    bool repeat_dominated = false;
    {
        std::lock_guard lock(shared.thaw_sample_mutex);
        measured = shared.staged_records >= adaptive_thaw_min_staged_records;
        repeat_dominated = shared.thaw_all.load(std::memory_order_relaxed);
    }
    if (!measured)
        return;

    auto & stats = getHashTablesStatistics<AggregationEntry>();
    AggregationEntry entry{.sum_of_sizes = 0, .median_size = 0, .adaptive_staging_repeat_dominated = repeat_dominated};
    if (const auto prev = stats.getSizeHint(stats_params))
    {
        entry.sum_of_sizes = prev->sum_of_sizes;
        entry.median_size = prev->median_size;
    }
    stats.update(entry, stats_params);
}

}
