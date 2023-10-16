#pragma once

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>

#include <any>

namespace DB
{
namespace Streaming
{
class AggregatedChunkInfo : public ChunkInfo
{
public:
    bool is_overflows = false;
    Int32 bucket_num = -1;
    UInt64 chunk_num = 0; // chunk number in order of generation, used during memory bound merging to restore chunks order
};

struct AggregatingTransformParams
{
    Aggregator aggregator;
    Aggregator::Params & params;
    bool final;

    AggregatingTransformParams(const Aggregator::Params & params_, bool final_)
        : aggregator(params_), params(aggregator.getParams()), final(final_)
    {
    }

    static Block getHeader(const Aggregator::Params & params, bool final) { return params.getHeader(final); }

    Block getHeader() const { return getHeader(params, final); }
};

class AggregatingTransform;

struct ManyAggregatedData
{
    /// Reference to all transforms
    std::vector<AggregatingTransform *> aggregating_transforms;

    std::vector<std::unique_ptr<std::timed_mutex>> variants_mutexes;
    ManyAggregatedDataVariants variants;

    /// Watermarks for all variants
    /// Acquire lock when update current watermark and find min watermark from all transform
    std::mutex watermarks_mutex;
    std::vector<Int64> watermarks;

    std::mutex finalizing_mutex;

    /// `finalized_watermark` is capturing the max watermark we have progressed
    std::atomic<Int64> finalized_watermark = INVALID_WATERMARK;
    std::atomic<Int64> finalized_window_end = INVALID_WATERMARK;

    std::atomic<Int64> version = 0;

    std::vector<std::unique_ptr<std::atomic<UInt64>>> rows_since_last_finalizations;

    /// std::atomic<AggregatingTransform *> last_checkpointing_transform = nullptr;

    /// Stuff additional data context to it if needed
    struct AnyField
    {
        std::any field;
    } any_field;

    explicit ManyAggregatedData(size_t num_threads) : variants(num_threads), watermarks(num_threads, INVALID_WATERMARK)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();

        for (size_t i = 0; i < num_threads; ++i)
        {
            rows_since_last_finalizations.emplace_back(std::make_unique<std::atomic<UInt64>>(0));
            variants_mutexes.emplace_back(std::make_unique<std::timed_mutex>());
        }

        aggregating_transforms.resize(variants.size());
    }

    bool hasField() const { return any_field.field.has_value(); }

    void setField(AnyField && field_) { any_field = std::move(field_); }

    template <typename T>
    T & getField()
    {
        return std::any_cast<T &>(any_field.field);
    }

    template <typename T>
    const T & getField() const
    {
        return std::any_cast<const T &>(any_field.field);
    }

    bool hasNewData() const
    {
        return std::any_of(
            rows_since_last_finalizations.begin(), rows_since_last_finalizations.end(), [](const auto & rows) { return *rows > 0; });
    }

    void resetRowCounts()
    {
        for (auto & rows : rows_since_last_finalizations)
            *rows = 0;
    }

    void addRowCount(size_t rows, size_t current_variant) { *rows_since_last_finalizations[current_variant] += rows; }
};

using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

/** It is for streaming query only. Streaming query never ends.
  * It aggregate streams of blocks in memory and finalize (project) intermediate
  * results periodically or on demand
  */
class AggregatingTransform : public IProcessor
{
public:
    AggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const String & log_name);

    /// For Parallel aggregating.
    AggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant_,
        size_t max_threads,
        const String & log_name);

    ~AggregatingTransform() override = default;

    Status prepare() override;
    void work() override;

    friend struct ManyAggregatedData;

private:
    virtual void consume(Chunk chunk);

    virtual void finalize(const ChunkContextPtr &) { }

    inline IProcessor::Status preparePushToOutput();

    void finalizeAlignment(const ChunkContextPtr &);

    /// returns @p min_watermark
    Int64 updateAndAlignWatermark(Int64 new_watermark);

    /// Try propagate and garbage collect time bucketed memory by finalized watermark
    bool propagateWatermarkAndClear();

    /// Try propagate an empty rows chunk to downstream, act as a heart beat
    bool propagateHeartbeatChunk();

protected:
    void emitVersion(Block & block);
    /// return {should_abort, need_finalization} pair
    virtual std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, size_t num_rows);
    void setCurrentChunk(Chunk chunk, const ChunkContextPtr & chunk_ctx);

    /// Quickly check if need finalization
    virtual bool needFinalization(Int64 /*min_watermark*/) const { return true; }

    /// Prepare and check whether can finalization many_data (called after acquired finalizing lock)
    virtual bool prepareFinalization(Int64 /*min_watermark*/) { return true; }

    virtual void removeBuckets(Int64 /*finalized_watermark*/) { }

protected:
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsPtr params;
    Poco::Logger * log;

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;

    ManyAggregatedDataPtr many_data;
    std::timed_mutex & variants_mutex;
    AggregatedDataVariants & variants;
    Int64 & watermark;

    /// It is used to save the AggregatingTransform has been propagated watermark and garbage collect time bucketed memory for itself:
    /// time buckets which are below this watermark can be safely GCed.
    Int64 propagated_watermark = INVALID_WATERMARK;

    size_t current_variant;

    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    bool is_consume_finished = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    /// Aggregated result which is pushed to downstream output
    Chunk current_chunk_aggregated;
    bool has_input = false;

    static constexpr auto finalizing_check_interval_ms = std::chrono::milliseconds(100);

    /// If the current thread fails to acquire the finalizing lock, then we keep the watermark and
    /// continue to try in the next processing (it's efficient, avoiding lock waiting)
    std::optional<Int64> try_finalizing_watermark;
};

Chunk convertToChunk(const Block & block);

}
}
