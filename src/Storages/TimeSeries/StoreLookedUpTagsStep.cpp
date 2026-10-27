#include <Storages/TimeSeries/StoreLookedUpTagsStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/TimeSeries/StoreLookedUpTagsTransform.h>


namespace DB
{

namespace
{
    SharedHeader makeOutputHeader(const Block & input_header, const size_t tags_start_index)
    {
        Block header;
        for (size_t i = 0; i != tags_start_index; ++i)
            header.insert(input_header.getByPosition(i));
        return std::make_shared<const Block>(std::move(header));
    }

    ITransformingStep::Traits getTraits()
    {
        /// The tag columns are dropped, and the sorting description may refer to them (`metric_name`).
        return ITransformingStep::Traits{
            {
                .returns_single_stream = false,
                .preserves_number_of_streams = true,
                .preserves_sorting = false,
            },
            {
                .preserves_number_of_rows = true,
            }};
    }
}


StoreLookedUpTagsStep::StoreLookedUpTagsStep(
    const SharedHeader & input_header_,
    const size_t tags_start_index_,
    const std::shared_ptr<ContextTimeSeriesTagsCollector> & tags_collector_)
    : ITransformingStep(input_header_, makeOutputHeader(*input_header_, tags_start_index_), getTraits())
    , tags_start_index(tags_start_index_)
    , tags_collector(tags_collector_)
{
}

void StoreLookedUpTagsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader & stream_header)
    {
        return std::make_shared<StoreLookedUpTagsTransform>(stream_header, output_header, tags_start_index, tags_collector);
    });
}

QueryPlanStepPtr StoreLookedUpTagsStep::clone() const
{
    return std::make_unique<StoreLookedUpTagsStep>(*this);
}

void StoreLookedUpTagsStep::updateOutputHeader()
{
    output_header = makeOutputHeader(*input_headers.front(), tags_start_index);
}

}
