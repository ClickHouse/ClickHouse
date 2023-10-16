#include <Processors/Transforms/Streaming/AggregatingTransform.h>

#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{

/// Convert block to chunk.
/// Adds additional info about aggregation.
Chunk convertToChunk(const Block & block)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.setChunkInfo(std::move(info));

    return chunk;
}

AggregatingTransform::AggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const String & log_name)
    : AggregatingTransform(std::move(header), std::move(params_), std::make_shared<ManyAggregatedData>(1), 0, 1, log_name)
{
}

AggregatingTransform::AggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    const String & log_name)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , params(std::move(params_))
    , log(&Poco::Logger::get(log_name))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants_mutex(*many_data->variants_mutexes[current_variant_])
    , variants(*many_data->variants[current_variant_])
    , watermark(many_data->watermarks[current_variant_])
    , current_variant(current_variant_)
    , max_threads(std::min(many_data->variants.size(), max_threads_))
{
    /// Register itself in the many aggregated data
    many_data->aggregating_transforms[current_variant] = this;
}

IProcessor::Status AggregatingTransform::prepare()
{
    /// There are one or two input ports.
    /// The first one is used at aggregation step, the second one - while reading merged data from ConvertingAggregated

    auto & output = outputs.front();
    /// Last output is current. All other outputs should already be closed.
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished() || is_consume_finished)
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_input)
        return preparePushToOutput();

    /// Only possible while consuming.
    if (read_current_chunk)
        return Status::Ready;

    bool need_process = false;
    /// cond-1: Only possible after finalize failed (because other threads were finalizing)
    /// cond-2: Only possible after finalization, need to propagate new watermark
    if (try_finalizing_watermark.has_value() || many_data->finalized_watermark.load(std::memory_order_relaxed) > propagated_watermark)
        need_process = true;

    /// Get chunk from input.
    if (input.isFinished() && !need_process)
    {
        is_consume_finished = true;
        output.finish();
        return Status::Finished;
    }

    if (!input.hasData())
    {
        input.setNeeded();

        if (need_process)
            return Status::Ready;

        return Status::NeedData;
    }

    current_chunk = input.pull(/*set_not_needed = */ true);
    read_current_chunk = true;

    return Status::Ready;
}

void AggregatingTransform::work()
{
    if (likely(!is_consume_finished))
    {
        auto num_rows = current_chunk.getNumRows();
        if (num_rows > 0)
            many_data->addRowCount(num_rows, current_variant);

        consume(std::move(current_chunk));

        read_current_chunk = false;
    }
}

void AggregatingTransform::consume(Chunk chunk)
{
    bool should_abort = false, need_finalization = false, need_propagate_heartbeat = false;

    auto num_rows = chunk.getNumRows();
    if (num_rows > 0)
    {
        /// There indeed has cases where num_rows of a chunk is greater than 0, but
        /// the columns are empty : select count() from stream where a != 0.
        /// So `executeOrMergeColumns` accepts num_rows as parameter
        if (std::tie(should_abort, need_finalization) = executeOrMergeColumns(chunk, num_rows); should_abort)
            is_consume_finished = true;
    }
    else
        need_propagate_heartbeat = true;

    /// Watermark and need_finalization shall not be true at the same time
    /// since when UDA has user defined emit strategy, watermark is disabled
    assert(!(chunk.hasWatermark() && need_finalization));

    if (chunk.hasWatermark())
        finalizeAlignment(chunk.getChunkContext());
    else if (need_finalization)
        finalize(chunk.getChunkContext());

    /// If the last attempt to finalize failed (because other threads were finalizing), then we will continue to try in this processing.
    /// But there is already output, we will try in the next `work()`
    if (try_finalizing_watermark.has_value() && !has_input)
        finalizeAlignment(std::make_shared<ChunkContext>());

    /// Try propagate and garbage collect time bucketed memory by finalized watermark
    propagateWatermarkAndClear();

    /// Try propagate an empty rows chunk to downstream
    if (need_propagate_heartbeat)
        propagateHeartbeatChunk();
}

std::pair<bool, bool> AggregatingTransform::executeOrMergeColumns(Chunk & chunk, [[maybe_unused]] size_t num_rows)
{
    auto columns = chunk.detachColumns();

    std::lock_guard lock(variants_mutex);
    return params->aggregator.executeOnBlock(std::move(columns), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys);
}

void AggregatingTransform::setCurrentChunk(Chunk chunk, const ChunkContextPtr & chunk_ctx)
{
    if (has_input)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Current chunk was already set.");

    has_input = true;
    current_chunk_aggregated = std::move(chunk);

    if (chunk_ctx)
    {
        current_chunk_aggregated.setChunkContext(std::move(chunk_ctx));
    }
}

IProcessor::Status AggregatingTransform::preparePushToOutput()
{
    auto & output = outputs.front();
    output.push(std::move(current_chunk_aggregated));
    has_input = false;

    return Status::PortFull;
}

bool AggregatingTransform::propagateHeartbeatChunk()
{
    if (has_input)
        return false;

    setCurrentChunk(Chunk{getOutputs().front().getHeader().getColumns(), 0}, nullptr);
    return true;
}

Int64 AggregatingTransform::updateAndAlignWatermark(Int64 new_watermark)
{
    std::lock_guard lock(many_data->watermarks_mutex);
    watermark = new_watermark;
    return std::ranges::min(many_data->watermarks);
}

void AggregatingTransform::finalizeAlignment(const ChunkContextPtr & chunk_ctx)
{
    if (chunk_ctx->hasWatermark())
    {
        /// Re-launch current watermark based on the finalized watermark
        if (unlikely(watermark == TIMEOUT_WATERMARK))
        {
            /// Firstly, acquired finalizing lock, blocking update `many_data->finalized_watermark` in other threads
            /// Secondly, acquired watermark lock, blocking watermark alignment in other threads
            /// NOTICE: Keeping the order of locking, since the watermark lock shall be acquired in `GlobalAggregatingTransform::prepareFinalization()`
            std::lock_guard lock1(many_data->finalizing_mutex);
            std::lock_guard lock2(many_data->watermarks_mutex);
            watermark = many_data->finalized_watermark.load(std::memory_order_relaxed);
        }

        auto new_watermark = chunk_ctx->getWatermark();
        if (likely(new_watermark > watermark))
            /// Found min watermark to finalize
            try_finalizing_watermark = updateAndAlignWatermark(new_watermark);
        else if (new_watermark < watermark)
            LOG_ERROR(log, "Found outdate watermark. current watermark={}, but got watermark={}", watermark, new_watermark);
    }

    if (!try_finalizing_watermark.has_value())
        return;

    /// Fastly check can finalize (without lock)
    if (!needFinalization(*try_finalizing_watermark))
    {
        try_finalizing_watermark.reset();
        return;
    }

    std::unique_lock<std::mutex> lock(many_data->finalizing_mutex, std::try_to_lock);
    if (!lock.owns_lock())
        return; /// Anothor thread is finalizing, so we try in next `work()`

    /// After acquired the lock, we need to prepare and check whether `try_finalizing_watermark` has been finalized in by another AggregatingTransform
    if (!prepareFinalization(*try_finalizing_watermark))
    {
        try_finalizing_watermark.reset();
        return;
    }

    auto start = MonotonicMilliseconds::now();

    /// Blocking all variants's processing of AggregatingTransform
    std::vector<std::unique_lock<std::timed_mutex>> lock_holders;
    lock_holders.reserve(many_data->variants_mutexes.size());
    for (auto & mutex : many_data->variants_mutexes)
        lock_holders.emplace_back(*mutex, std::try_to_lock);

    /// Lock for each varitants mutex
    bool all_locks_acquired = true;
    do
    {
        all_locks_acquired = true;
        for (auto & lock_holder : lock_holders)
        {
            if (isCancelled())
                return;

            if (!lock_holder.owns_lock())
                if (!lock_holder.try_lock_for(finalizing_check_interval_ms))
                    all_locks_acquired = false;
        }
    } while (!all_locks_acquired);

    chunk_ctx->setWatermark(*try_finalizing_watermark);
    finalize(chunk_ctx);
    try_finalizing_watermark.reset();

    auto end = MonotonicMilliseconds::now();
    LOG_INFO(
        log,
        "Took {} milliseconds to finalize {} shard aggregation, finalized watermark={}",
        end - start,
        many_data->variants.size(),
        many_data->finalized_watermark.load(std::memory_order_relaxed));
}

bool AggregatingTransform::propagateWatermarkAndClear()
{
    auto finalized_watermark = many_data->finalized_watermark.load(std::memory_order_relaxed);
    if (finalized_watermark > propagated_watermark)
    {
        if (!has_input)
        {
            auto chunk_ctx = std::make_shared<ChunkContext>();
            chunk_ctx->setWatermark(finalized_watermark);
            setCurrentChunk(Chunk{getOutputs().front().getHeader().getColumns(), 0}, chunk_ctx);
        }
        else
            assert(finalized_watermark == current_chunk_aggregated.getWatermark());

        removeBuckets(finalized_watermark);
        propagated_watermark = finalized_watermark;
        return true;
    }
    return false;
}

}
}
