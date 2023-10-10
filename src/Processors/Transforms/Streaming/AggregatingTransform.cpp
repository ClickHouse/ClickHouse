#include <Processors/Transforms/Streaming/AggregatingTransform.h>

// #include <Checkpoint/CheckpointCoordinator.h>
// #include <Common/ProtonCommon.h>
#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int RECOVER_CHECKPOINT_FAILED;
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
    : AggregatingTransform(std::move(header), std::move(params_), std::make_shared<ManyAggregatedData>(1), 0, 1, 1, log_name)
{
}

AggregatingTransform::AggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
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
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
    (void)temporary_data_merge_threads;

    /// Register itself in the many aggregated data
    many_data->aggregating_transforms[current_variant] = this;
}

AggregatingTransform::~AggregatingTransform() = default;

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

    /// Only possible after local checkpointed and other AggregatingTransforms don't finish checkpoint yet.
    /// Check it before input, since we can not fetch new data until all checkpoint request completed
    // if (ckpt_request)
    //     return Status::Ready;

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
    // Int64 start_ns = MonotonicNanoseconds::now();
    // metrics.processed_bytes += current_chunk.bytes();

    if (likely(!is_consume_finished))
    {
        auto num_rows = current_chunk.getNumRows();
        if (num_rows > 0)
        {
            many_data->addRowCount(num_rows, current_variant);
            src_rows += num_rows;
            src_bytes += current_chunk.bytes();
        }

        consume(std::move(current_chunk));

        read_current_chunk = false;
    }

    // metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
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

    /// Since checkpoint barrier is always standalone, it can't coexist with watermark,
    /// we handle watermark and checkpoint barrier separately
    // assert(!chunk.hasWatermark());

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

    /// Try propagate checkpoint to downstream
    // propagateCheckpointAndReset();

    /// Try propagate an empty rows chunk to downstream
    if (need_propagate_heartbeat)
        propagateHeartbeatChunk();
}

std::pair<bool, bool> AggregatingTransform::executeOrMergeColumns(Chunk & chunk, size_t num_rows)
{
    auto columns = chunk.detachColumns();
    if (params->params.only_merge)
    {
        auto block = getInputs().front().getHeader().cloneWithColumns(columns);
        materializeBlockInplace(block);
        /// FIXME
        /// Blocking finalization during execution on current variant
        std::lock_guard lock(variants_mutex);
        auto success = params->aggregator.mergeOnBlock(block, variants, no_more_keys);
        return {!success, false};
    }
    else
    {
        /// Blocking finalization during execution on current variant
        std::lock_guard lock(variants_mutex);
        return params->aggregator.executeOnBlock(std::move(columns), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys);
    }
}

void AggregatingTransform::emitVersion([[maybe_unused]] Block & block)
{
    // size_t rows = block.rows();
    // if (params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
    // {
    //     /// For UDA with own emit strategy, possibly a block can trigger multiple emits, each emit cause version+1
    //     /// each emit only has one result, therefore we can count emit times by row number
    //     auto col = params->version_type->createColumn();
    //     col->reserve(rows);
    //     for (size_t i = 0; i < rows; i++)
    //         col->insert(many_data->version++);
    //     block.insert({std::move(col), params->version_type, ProtonConsts::RESERVED_EMIT_VERSION});
    // }
    // else
    // {
    //     Int64 version = many_data->version++;
    //     block.insert(
    //         {params->version_type->createColumnConst(rows, version)->convertToFullColumnIfConst(),
    //          params->version_type,
    //          ProtonConsts::RESERVED_EMIT_VERSION});
    // }
}

void AggregatingTransform::setCurrentChunk(Chunk chunk, const ChunkContextPtr & chunk_ctx)
{
    if (has_input)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Current chunk was already set.");

    has_input = true;
    current_chunk_aggregated = std::move(chunk);

    if (chunk_ctx)
    {
        /// NOTE: For StremaingShrinkResize of downstream, it's need all inputs propagate watermark then do watermark alignment,
        /// So the watermark cannot be cleared. On the other hand, if the downstream needs to establish its own watermark,
        /// the watermark will be cleared and reassigned in another `WatermarkStamper` of downstream.
        // if (params->final && params->params.group_by != Aggregator::Params::GroupBy::OTHER)
        //     chunk_ctx->clearWatermark();

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

// void AggregatingTransform::checkpointAlignment(const CheckpointContextPtr & ckpt_ctx)
// {
//     assert(!ckpt_request);
//     ckpt_request = std::move(ckpt_ctx);

//     bool is_last_checkpoint_transform = false;
//     if (many_data->ckpt_requested.fetch_add(1) + 1 == many_data->variants.size())
//     {
//         is_last_checkpoint_transform = true;
//         many_data->last_checkpointing_transform.store(this);
//     }

//     /// Do checkpoint for itself
//     checkpoint(ckpt_request);

//     /// Last checkpoint request done, reset the progress of checkpointing
//     if (is_last_checkpoint_transform)
//     {
//         many_data->last_checkpointing_transform.store(nullptr);
//         many_data->ckpt_requested.store(0);
//     }
// }

// bool AggregatingTransform::propagateCheckpointAndReset()
// {
//     /// Since checkpoint barrier is always standalone, it can't coexist with other contexts.
//     if (!ckpt_request || has_input)
//         return false;

//     /// Only all checkpoints request done, then can reset and propagate current ckpt request
//     if (many_data->ckpt_requested.load() == 0)
//     {
//         auto chunk_ctx = std::make_shared<ChunkContext>();
//         chunk_ctx->setCheckpointContext(std::move(ckpt_request));
//         setCurrentChunk(Chunk{getOutputs().front().getHeader().getColumns(), 0}, std::move(chunk_ctx));
//         assert(!ckpt_request);
//         return true;
//     }
//     return false;
// }

// void AggregatingTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
// {
//     ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
//         bool is_last_checkpointing_transform = (this == many_data->last_checkpointing_transform.load());
//         DB::writeBoolText(is_last_checkpointing_transform, wb);

//         /// Serializing shared data (only do it on last checkpointing transform)
//         if (is_last_checkpointing_transform)
//         {
//             UInt16 num_variants = many_data->variants.size();
//             DB::writeIntBinary(num_variants, wb);

//             DB::writeIntBinary(many_data->finalized_watermark.load(std::memory_order_relaxed), wb);
//             DB::writeIntBinary(many_data->finalized_window_end.load(std::memory_order_relaxed), wb);
//             DB::writeIntBinary(many_data->version.load(std::memory_order_relaxed), wb);

//             assert(num_variants == many_data->rows_since_last_finalizations.size());
//             for (const auto & last_row : many_data->rows_since_last_finalizations)
//                 writeIntBinary<UInt64>(last_row->load(std::memory_order_relaxed), wb);

//             bool has_field = many_data->hasField();
//             DB::writeBoolText(has_field, wb);
//             if (has_field)
//                 many_data->any_field.serializer(many_data->any_field.field, wb);
//         }

//         /// Serializing no shared data
//         params->aggregator.checkpoint(variants, wb);

//         DB::writeIntBinary(watermark, wb);

//         /// After the local checkpoint is processed, the `propagated_watermark` may still be updated,
//         /// because other transforms may have new finalizing processing.
//         /// But it doesn't matter, we will update according to the recovered `finalized_watermark` later
//         DB::writeIntBinary(propagated_watermark, wb);
//     });
// }

// void AggregatingTransform::recover(CheckpointContextPtr ckpt_ctx)
// {
//     ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) {
//         bool is_last_checkpointing_transform;
//         DB::readBoolText(is_last_checkpointing_transform, rb);

//         /// Serializing shared data
//         if (is_last_checkpointing_transform)
//         {
//             UInt16 num_variants = 0;
//             DB::readIntBinary(num_variants, rb);
//             if (num_variants != many_data->variants.size())
//                 throw Exception(
//                     ErrorCodes::RECOVER_CHECKPOINT_FAILED,
//                     "Failed to recover aggregation checkpoint. Number of data variants are not the same, checkpointed={}, current={}",
//                     num_variants,
//                     variants.size());

//             Int64 last_finalized_watermark;
//             DB::readIntBinary(last_finalized_watermark, rb);
//             many_data->finalized_watermark = last_finalized_watermark;

//             Int64 last_finalized_window_end;
//             DB::readIntBinary(last_finalized_window_end, rb);
//             many_data->finalized_window_end = last_finalized_window_end;

//             Int64 last_version = 0;
//             DB::readIntBinary(last_version, rb);
//             many_data->version = last_version;

//             assert(num_variants == many_data->rows_since_last_finalizations.size());
//             for (auto & rows_since_last_finalization : many_data->rows_since_last_finalizations)
//             {
//                 UInt64 last_rows = 0;
//                 DB::readIntBinary<UInt64>(last_rows, rb);
//                 *rows_since_last_finalization = last_rows;
//             }

//             bool has_field;
//             DB::readBoolText(has_field, rb);
//             if (has_field)
//                 many_data->any_field.deserializer(many_data->any_field.field, rb);
//         }

//         /// Serializing local or stable data during checkpointing
//         params->aggregator.recover(variants, rb);

//         DB::readIntBinary(watermark, rb);

//         DB::readIntBinary(propagated_watermark, rb);
//     });
// }

}
}
