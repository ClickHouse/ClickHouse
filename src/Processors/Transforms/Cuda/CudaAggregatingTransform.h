#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Cuda/CudaAggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Common/Stopwatch.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

using CudaAggregatorList = std::list<CudaAggregator>;
using CudaAggregatorListPtr = std::shared_ptr<CudaAggregatorList>;

struct CudaAggregatingTransformParams
{
    Aggregator::Params params;

    /// Each params holds a list of aggregators which are used in query. It's needed because we need
    /// to use a pointer of aggregator to proper destroy complex aggregation states on exception
    /// (See comments in AggregatedDataVariants). However, this pointer might not be valid because
    /// we can have two different aggregators at the same time due to mixed pipeline of aggregate
    /// projections, and one of them might gets destroyed before used.

    /// evillique: Probably get rid of this
    CudaAggregatorListPtr aggregator_list_ptr;
    CudaAggregator & aggregator;
    bool final;
    bool only_merge = false;

    CudaAggregatingTransformParams(const Block & header, const Aggregator::Params & params_, bool final_, ContextPtr context_)
        : params(params_)
        , aggregator_list_ptr(std::make_shared<CudaAggregatorList>())
        , aggregator(*aggregator_list_ptr->emplace(aggregator_list_ptr->end(), context_, header, params))
        , final(final_)
    {
    }

    Block getHeader() const { return aggregator.getHeader(final); }

    Block getCustomHeader(bool final_) const { return aggregator.getHeader(final_); }
};

using CudaAggregatingTransformParamsPtr = std::shared_ptr<CudaAggregatingTransformParams>;

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
class CudaAggregatingTransform : public IProcessor
{
public:
    CudaAggregatingTransform(Block header, CudaAggregatingTransformParamsPtr params_, ContextPtr context_);

    /// For Parallel aggregating.
    CudaAggregatingTransform(
        Block header,
        CudaAggregatingTransformParamsPtr params_,
        CudaAggregatedDataVariantsPtr variants_,
        size_t current_variant,
        size_t max_threads,
        ContextPtr context_);
    ~CudaAggregatingTransform() override;

    String getName() const override { return "CudaAggregatingTransform"; }
    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

protected:
    void consume(Chunk chunk);

private:
    ContextPtr context;
    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    CudaAggregatingTransformParamsPtr params;
    Poco::Logger * log = &Poco::Logger::get("CudaAggregatingTransform");

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;

    CudaAggregatedDataVariantsPtr variants;
    size_t max_threads = 1;

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
