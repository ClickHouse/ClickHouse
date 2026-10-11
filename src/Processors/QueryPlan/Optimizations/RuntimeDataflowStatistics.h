#pragma once

#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Compression/ICompressionCodec.h>
#include <Storages/ColumnSize.h>
#include <Common/CacheBase.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class Aggregator;
struct AggregatedDataVariants;

struct RuntimeDataflowStatistics
{
    /// The read parallel replicas would coordinate: they split it, so the cost model divides it by the
    /// number of replicas.
    size_t input_bytes = 0;
    /// Every other read of the same subtree. Parallel replicas do not split these - each replica runs the
    /// whole subtree, so each reads all of them. They cost the same wall-clock time either way, which is
    /// why they are kept apart from `input_bytes` rather than added to it, but they cost the cluster
    /// `num_replicas` times as much work, which is what the amplification gate weighs.
    size_t duplicated_bytes = 0;
    size_t output_bytes = 0;
    size_t total_rows_to_read = 0;
};

inline RuntimeDataflowStatistics operator+(const RuntimeDataflowStatistics & lhs, const RuntimeDataflowStatistics & rhs)
{
    return RuntimeDataflowStatistics{
        .input_bytes = lhs.input_bytes + rhs.input_bytes,
        .duplicated_bytes = lhs.duplicated_bytes + rhs.duplicated_bytes,
        .output_bytes = lhs.output_bytes + rhs.output_bytes,
        .total_rows_to_read = lhs.total_rows_to_read + rhs.total_rows_to_read,
    };
}

class RuntimeDataflowStatisticsCache
{
public:
    using Entry = RuntimeDataflowStatistics;
    using Cache = DB::CacheBase<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;

    RuntimeDataflowStatisticsCache()
        : stats_cache(std::make_shared<Cache>(CurrentMetrics::end(), CurrentMetrics::end(), 1024 * 1024 * 1024, 0))
    {
    }

    std::optional<Entry> getStats(size_t key) const;

    void update(size_t key, RuntimeDataflowStatistics stats);

private:
    CachePtr stats_cache;
};

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache();

/// The codecs a column's `CODEC` resolves to, for the two shapes its serialized sample can have.
///
/// The writer resolves a `CODEC` per substream: a type-specific codec (`ALP`, `T64`, `Delta`, ...) is
/// applied only to a substream that carries the column type itself, and structural substreams (`Array`
/// offsets, null map, sparse offsets, ...) keep only the generic codecs. The estimate serializes a whole
/// column into a single buffer, so `type_specific` describes that buffer only when it holds exactly one
/// stream of `type_specific_for`. Neither is a property of the table metadata alone: the serialization is
/// chosen per block from the column at hand, and an unfinished `ALTER MODIFY COLUMN` leaves the part
/// holding the old type while the metadata already reports the new one.
struct ColumnCodecs
{
    /// Null when the column's `CODEC` has no resolution against a type - then only `generic` applies.
    CompressionCodecPtr type_specific = nullptr;
    DataTypePtr type_specific_for = nullptr;
    CompressionCodecPtr generic = nullptr;
};

/// Only columns whose `CODEC` overrides the part's default; resolved once per read task.
using ColumnCodecByName = UnorderedMapWithMemoryTracking<String, ColumnCodecs>;

/// Whether `serialization` writes the column as a single stream carrying `type` itself, the only layout
/// a type-specific codec may be applied to.
bool isSerializedAsSingleStreamOfColumnType(const ISerialization & serialization, const DataTypePtr & type);

/// One execution's dataflow statistics, the single cache entry its updaters fill. Every updater of the
/// execution shares it, and it writes the entry when the last of them is gone.
struct RuntimeDataflowStatisticsBlock
{
    struct Statistics
    {
        std::atomic_size_t counter{0};

        std::mutex mutex;
        size_t bytes TSA_GUARDED_BY(mutex) = 0;
        size_t sample_bytes TSA_GUARDED_BY(mutex) = 0;
        size_t compressed_bytes TSA_GUARDED_BY(mutex) = 0;
        size_t elapsed_microseconds TSA_GUARDED_BY(mutex) = 0;
    };

    enum InputStatisticsType
    {
        WithByteHint = 0,
        WithoutByteHint = 1,
        MaxInputType = 2,
    };

    enum OutputStatisticsType
    {
        AggregationState = 0,
        AggregationKeys = 1,
        OutputChunk = 2,
        MaxOutputType = 3,
    };

    RuntimeDataflowStatisticsBlock(size_t cache_key_, size_t total_rows_to_read_)
        : cache_key(cache_key_)
        , total_rows_to_read(total_rows_to_read_)
    {
        if (cache_key == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cache key for RuntimeDataflowStatisticsBlock cannot be zero");

        if (total_rows_to_read == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Total rows from storage cannot be zero");
    }

    ~RuntimeDataflowStatisticsBlock();

    const size_t cache_key = 0;
    const size_t total_rows_to_read = 0;

    std::atomic_bool unsupported_case{false};

    std::array<Statistics, MaxInputType> input_bytes_statistics;
    std::array<Statistics, MaxInputType> duplicated_bytes_statistics;
    std::array<Statistics, MaxOutputType> output_bytes_statistics;
};

class RuntimeDataflowStatisticsCacheUpdater
{
    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
    using Statistics = RuntimeDataflowStatisticsBlock::Statistics;
    using InputStatisticsType = RuntimeDataflowStatisticsBlock::InputStatisticsType;
    using OutputStatisticsType = RuntimeDataflowStatisticsBlock::OutputStatisticsType;

public:
    /// `duplicated` is set on the updater given to the reads parallel replicas would not split: each replica
    /// performs them in full, so their bytes go to `duplicated_bytes` rather than to `input_bytes`.
    explicit RuntimeDataflowStatisticsCacheUpdater(std::shared_ptr<RuntimeDataflowStatisticsBlock> block_, bool duplicated_ = false)
        : block(std::move(block_))
        , duplicated(duplicated_)
    {
    }

    void recordOutputChunk(const Chunk & chunk, const Block & header);

    void recordAggregationStateSizes(AggregatedDataVariants & variant, ssize_t bucket);

    void recordAggregationKeySizes(const Chunk & chunk, const ColumnNumbers & keys_positions, const DataTypes & key_types);

    /// For a conversion that materialized only some of the groups (the bucket Top-K, the HAVING pre-filter):
    /// `full_key_bytes` is the byte size all keys would occupy materialized, measured on the
    /// hash table, and the chunk provides the compression-ratio sample only. The statistics
    /// must describe the untruncated output because they price the parallel-replicas plan,
    /// whose partial aggregation materializes every group.
    /// `untruncated_sample_columns`, when the conversion kept one, is a bounded copy of the untruncated
    /// keys and is sampled in place of the chunk: the chunk holds the kept groups only, whose keys
    /// compress differently, and holds no row at all when every group was rejected - and without a
    /// compression ratio the accumulated byte count is dropped rather than estimated.
    void recordAggregationKeySizes(
        const Chunk & chunk,
        const ColumnNumbers & keys_positions,
        const DataTypes & key_types,
        size_t full_key_bytes,
        const Columns & untruncated_sample_columns);

    /// Estimates compressed size of aggregate state columns in the output chunk.
    /// Mirrors the logic of Aggregator::estimateSizeOfCompressedState but works on ColumnAggregateFunction columns
    /// rather than a hash table. Used by in-order aggregation where states are already materialized into columns (single-stream case).
    void recordAggregationStateColumnSizes(const Chunk & chunk, const ColumnNumbers & keys_positions, const Block & header);

    /// Updates should_continue_sampling to true if the current read block is chosen for sampling.
    /// It is needed because in general we read each block in multiple steps because of prewhere.
    /// If the first part of the block was chosen for sampling, we want to record statistics for the whole block in later steps,
    /// so should_continue_sampling remains true for subsequent calls for the same logical block.
    void recordInputColumns(
        const ColumnsWithTypeAndName & input_columns,
        const NameSet & partially_read_columns,
        const NamesAndTypesList & part_columns,
        const ColumnSizeByName & column_sizes,
        const ColumnCodecByName & column_codecs,
        const CompressionCodecPtr & default_codec,
        size_t read_bytes,
        std::optional<bool> & should_continue_sampling);

    /// Ignored for duplicated reads: they only refine the duplicated-read gate, so a read that cannot be
    /// measured there must not drop the statistics that decide whether parallel replicas are considered at all.
    void markUnsupportedCase()
    {
        if (!duplicated)
            block->unsupported_case.store(true, std::memory_order_relaxed);
    }

private:
    static bool shouldSampleBlock(Statistics & statistics, size_t block_rows);

    /// `full_bytes` overrides the byte count taken from the columns, for callers whose columns
    /// are only a sample of the dataflow being accounted.
    static void
    recordColumns(Statistics & statistics, size_t num_rows, const ColumnsWithTypeAndName & cols, std::optional<size_t> full_bytes = {});

    const std::shared_ptr<RuntimeDataflowStatisticsBlock> block;
    const bool duplicated;
};

using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

class RuntimeDataflowStatisticsCollector : public ISimpleTransform
{
public:
    RuntimeDataflowStatisticsCollector(SharedHeader header_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "RuntimeDataflowStatisticsCollector"; }

protected:
    void transform(Chunk & chunk) override;

private:
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
};
}
