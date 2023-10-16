#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>


namespace DB
{
namespace Streaming
{
GlobalAggregatingTransform::GlobalAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : GlobalAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1)
{
}

GlobalAggregatingTransform::GlobalAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant_,
        max_threads_,
        "GlobalAggregatingTransform")
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::OTHER);
}

bool GlobalAggregatingTransform::needFinalization(Int64 min_watermark) const
{
    if (min_watermark == INVALID_WATERMARK)
        return false;

    return true;
}

bool GlobalAggregatingTransform::prepareFinalization(Int64 min_watermark)
{
    if (min_watermark == INVALID_WATERMARK)
        return false;

    std::lock_guard lock(many_data->watermarks_mutex);
    if (std::ranges::all_of(many_data->watermarks, [](const auto & wm) { return wm != INVALID_WATERMARK; }))
    {
        /// Reset all watermarks to INVALID,
        /// Next finalization will just be triggered when all transform watermarks are updated
        std::ranges::for_each(many_data->watermarks, [](auto & wm) { wm = INVALID_WATERMARK; });
        return many_data->hasNewData(); /// If there is no new data, don't emit aggr result
    }
    return false;
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    SCOPE_EXIT({
        many_data->resetRowCounts();
        many_data->finalized_watermark.store(chunk_ctx->getWatermark(), std::memory_order_relaxed);
    });

    auto prepared_data_ptr = params->aggregator.prepareVariantsToMerge(many_data->variants);
    if (prepared_data_ptr->empty())
        return;

    if (initialize(prepared_data_ptr, chunk_ctx))
        /// Processed
        return;

    if (prepared_data_ptr->at(0)->isTwoLevel())
        convertTwoLevel(prepared_data_ptr, chunk_ctx);
    else
        convertSingleLevel(prepared_data_ptr, chunk_ctx);
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::initialize
bool GlobalAggregatingTransform::initialize(ManyAggregatedDataVariantsPtr & data, const ChunkContextPtr & chunk_ctx)
{
    AggregatedDataVariantsPtr & first = data->at(0);

    if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
    {
        params->aggregator.mergeWithoutKeyDataImpl(*data);
        auto block = params->aggregator.prepareBlockAndFillWithoutKey(
            *first, params->final, first->type != AggregatedDataVariants::Type::without_key, ConvertAction::STREAMING_EMIT);

        setCurrentChunk(convertToChunk(block), chunk_ctx);
        return true;
    }

    return false;
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::mergeSingleLevel
void GlobalAggregatingTransform::convertSingleLevel(ManyAggregatedDataVariantsPtr & data, const ChunkContextPtr & chunk_ctx)
{
    AggregatedDataVariantsPtr & first = data->at(0);

    /// Without key aggregation is already handled in `::initialize(...)`
    assert(first->type != AggregatedDataVariants::Type::without_key);

#define M(NAME) \
    else if (first->type == AggregatedDataVariants::Type::NAME) \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data, ConvertAction::STREAMING_EMIT);
    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
    else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");

    auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final, ConvertAction::STREAMING_EMIT);

    setCurrentChunk(convertToChunk(block), chunk_ctx);
}

void GlobalAggregatingTransform::convertTwoLevel(ManyAggregatedDataVariantsPtr & data, const ChunkContextPtr & chunk_ctx)
{
    /// FIXME
    (void)chunk_ctx;
    if (data)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Two level merge is not implemented in global aggregation");
}

}
}
