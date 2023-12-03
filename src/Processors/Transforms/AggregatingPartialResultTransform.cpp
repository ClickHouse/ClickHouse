#include <Processors/Transforms/AggregatingPartialResultTransform.h>

namespace DB
{

AggregatingPartialResultTransform::AggregatingPartialResultTransform(
    const Block & input_header, const Block & output_header, AggregatingTransformPtr aggregating_transform_,
    UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_)
    : PartialResultTransform(input_header, output_header, partial_result_limit_, partial_result_duration_ms_)
    , aggregating_transform(std::move(aggregating_transform_))
    , transform_aggregator(input_header, aggregating_transform->params->params)
    {}

void AggregatingPartialResultTransform::transformPartialResult(Chunk & chunk)
{
    auto & params = aggregating_transform->params->params;

    bool no_more_keys = false;
    AggregatedDataVariants variants;
    ColumnRawPtrs key_columns(params.keys_size);
    Aggregator::AggregateColumns aggregate_columns(params.aggregates_size);

    const UInt64 num_rows = chunk.getNumRows();
    transform_aggregator.executeOnBlock(chunk.detachColumns(), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys);

    auto transformed_block = transform_aggregator.convertToBlocks(variants, /*final*/ true, /*max_threads*/ 1).front();

    chunk = convertToChunk(transformed_block);
}

PartialResultTransform::ShaphotResult AggregatingPartialResultTransform::getRealProcessorSnapshot()
{
    std::lock_guard lock(aggregating_transform->snapshot_mutex);
    if (aggregating_transform->is_generate_initialized)
        return {{}, SnaphotStatus::Stopped};

    if (aggregating_transform->variants.empty())
        return {{}, SnaphotStatus::NotReady};

    auto & snapshot_aggregator = aggregating_transform->params->aggregator;
    auto & snapshot_variants = aggregating_transform->many_data->variants;
    auto block = snapshot_aggregator.prepareBlockAndFillWithoutKeySnapshot(*snapshot_variants.at(0));

    return {convertToChunk(block), SnaphotStatus::Ready};
}

}
