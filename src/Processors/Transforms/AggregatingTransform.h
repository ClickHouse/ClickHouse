#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Processors/ISource.h>
#include <Processors/IProcessor.h>
#include <Interpreters/Aggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Common/Stopwatch.h>

namespace DB
{

class AggregatedArenasChunkInfo : public ChunkInfo
{
public:
    Arenas arenas;
    AggregatedArenasChunkInfo(Arenas arenas_)
        : arenas(std::move(arenas_))
    {}
};

class AggregatedChunkInfo : public ChunkInfo
{
public:
    bool is_overflows = false;
    Int32 bucket_num = -1;
};

using AggregatorList = std::list<Aggregator>;
using AggregatorListPtr = std::shared_ptr<AggregatorList>;

using AggregatorList = std::list<Aggregator>;
using AggregatorListPtr = std::shared_ptr<AggregatorList>;

struct AggregatingTransformParams
{
    Aggregator::Params params;

    /// Each params holds a list of aggregators which are used in query. It's needed because we need
    /// to use a pointer of aggregator to proper destroy complex aggregation states on exception
    /// (See comments in AggregatedDataVariants). However, this pointer might not be valid because
    /// we can have two different aggregators at the same time due to mixed pipeline of aggregate
    /// projections, and one of them might gets destroyed before used.
    AggregatorListPtr aggregator_list_ptr;
    Aggregator & aggregator;
    bool final;
    bool only_merge = false;

    AggregatingTransformParams(const Aggregator::Params & params_, bool final_)
        : params(params_)
        , aggregator_list_ptr(std::make_shared<AggregatorList>())
        , aggregator(*aggregator_list_ptr->emplace(aggregator_list_ptr->end(), params))
        , final(final_)
    {
    }

    AggregatingTransformParams(const Aggregator::Params & params_, const AggregatorListPtr & aggregator_list_ptr_, bool final_)
        : params(params_)
        , aggregator_list_ptr(aggregator_list_ptr_)
        , aggregator(*aggregator_list_ptr->emplace(aggregator_list_ptr->end(), params))
        , final(final_)
    {
    }

    Block getHeader() const { return aggregator.getHeader(final); }

    Block getCustomHeader(bool final_) const { return aggregator.getHeader(final_); }
};

struct ManyAggregatedData
{
    ManyAggregatedDataVariants variants;
    std::vector<std::unique_ptr<std::mutex>> mutexes;
    std::atomic<UInt32> num_finished = 0;

    explicit ManyAggregatedData(size_t num_threads = 0) : variants(num_threads), mutexes(num_threads)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();

        for (auto & mut : mutexes)
            mut = std::make_unique<std::mutex>();
    }
};

using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;
using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;

/// Worker which merges buckets for two-level aggregation.
/// Atomically increments bucket counter and returns merged result.
class ConvertingAggregatedToChunksSource : public ISource
{
public:
    static constexpr UInt32 NUM_BUCKETS = 256;

    struct SharedData
    {
        std::atomic<UInt32> next_bucket_to_merge = 0;
        std::array<std::atomic<bool>, NUM_BUCKETS> is_bucket_processed{};
        std::atomic<bool> is_cancelled = false;

        SharedData()
        {
            for (auto & flag : is_bucket_processed)
                flag = false;
        }
    };

    using SharedDataPtr = std::shared_ptr<SharedData>;

    ConvertingAggregatedToChunksSource(
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataVariantsPtr data_,
        SharedDataPtr shared_data_,
        Arena * arena_);

    String getName() const override { return "ConvertingAggregatedToChunksSource"; }

protected:
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    Arena * arena;
};

/// Generates chunks with aggregated data.
/// In single level case, aggregates data itself.
/// In two-level case, creates `ConvertingAggregatedToChunksSource` workers:
///
/// ConvertingAggregatedToChunksSource ->
/// ConvertingAggregatedToChunksSource -> ConvertingAggregatedToChunksTransform -> AggregatingTransform
/// ConvertingAggregatedToChunksSource ->
///
/// Result chunks guaranteed to be sorted by bucket number.
class ConvertingAggregatedToChunksTransform : public IProcessor
{
public:
    ConvertingAggregatedToChunksTransform(AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, size_t num_threads_);

    String getName() const override { return "ConvertingAggregatedToChunksTransform"; }

    void work() override;

    Processors expandPipeline() override;

    IProcessor::Status prepare() override;

private:
    IProcessor::Status preparePushToOutput();

    /// Read all sources and try to push current bucket.
    IProcessor::Status prepareTwoLevel();

    void setCurrentChunk(Chunk chunk);

    void initialize();

    void mergeSingleLevel();

    void createSources();

    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    ConvertingAggregatedToChunksSource::SharedDataPtr shared_data;

    size_t num_threads;

    bool is_initialized = false;
    bool has_input = false;
    bool finished = false;

    Chunk current_chunk;

    UInt32 current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
    std::array<Chunk, NUM_BUCKETS> chunks;

    Processors processors;
};

/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
  * Columns with aggregate functions adds to the end of the block.
  * If final = false, the aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  *
  * For every separate stream of data separate AggregatingTransform is created.
  * Every AggregatingTransform reads data from the first port till is is not run out, or max_rows_to_group_by reached.
  * When the last AggregatingTransform finish reading, the result of aggregation is needed to be merged together.
  * This task is performed by ConvertingAggregatedToChunksTransform.
  * Last AggregatingTransform expands pipeline and adds second input port, which reads from ConvertingAggregated.
  *
  * Aggregation data is passed by ManyAggregatedData structure, which is shared between all aggregating transforms.
  * At aggregation step, every transform uses it's own AggregatedDataVariants structure.
  * At merging step, all structures pass to ConvertingAggregatedToChunksTransform.
  */
class AggregatingTransform : public IProcessor
{
public:
    AggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    AggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);
    ~AggregatingTransform() override;

    String getName() const override { return "AggregatingTransform"; }
    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

protected:
    void consume(Chunk chunk);

private:
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsPtr params;
    Poco::Logger * log = &Poco::Logger::get("AggregatingTransform");

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;
    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_generate_initialized = false;
    bool is_consume_finished = false;
    bool is_pipeline_created = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    bool is_consume_started = false;

    void initGenerate();
};

Chunk convertToChunk(const Block & block);

}
