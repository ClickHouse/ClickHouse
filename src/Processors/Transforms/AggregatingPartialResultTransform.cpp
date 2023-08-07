#include <Processors/Transforms/AggregatingPartialResultTransform.h>

namespace DB
{

AggregatingPartialResultTransform::AggregatingPartialResultTransform(
    const Block & input_header, const Block & output_header, AggregatingTransformPtr aggregating_transform_,
    UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_)
    : PartialResultTransform(input_header, output_header, partial_result_limit_, partial_result_duration_ms_)
    , aggregating_transform(std::move(aggregating_transform_))
    {}

PartialResultTransform::ShaphotResult AggregatingPartialResultTransform::getRealProcessorSnapshot()
{
    std::lock_guard lock(aggregating_transform->snapshot_mutex);

    auto & params = aggregating_transform->params;
    /// Currently not supported cases
    /// TODO: check that insert results from prepareBlockAndFillWithoutKey return values without changing of the aggregator state
    if (params->params.keys_size != 0 /// has at least one key for aggregation
        || params->aggregator.hasTemporaryData() /// use external storage for aggregation
        || aggregating_transform->many_data->variants.size() > 1) /// use more then one stream for aggregation
        return {{}, SnaphotStatus::Stopped};

    if (aggregating_transform->is_generate_initialized)
        return {{}, SnaphotStatus::Stopped};

    if (aggregating_transform->variants.empty())
        return {{}, SnaphotStatus::NotReady};

    auto & aggregator = params->aggregator;

    auto prepared_data = aggregator.prepareVariantsToMerge(aggregating_transform->many_data->variants);
    AggregatedDataVariantsPtr & first = prepared_data.at(0);

    aggregator.mergeWithoutKeyDataImpl(prepared_data);
    auto block = aggregator.prepareBlockAndFillWithoutKeySnapshot(*first);

    return {convertToChunk(block), SnaphotStatus::Ready};
}

}
