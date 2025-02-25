#include <Processors/QueryPlan/scatterDataByKeys.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <Processors/ResizeProcessor.h>

namespace DB
{

void scatterDataByKeysIfNeeded(
    QueryPipelineBuilder & pipeline,
    const ColumnNumbers & key_columns,
    size_t threads,
    size_t streams
)
{
    if (!key_columns.empty() && threads > 1)
    {
        Block stream_header = pipeline.getHeader();

        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            Processors processors;
            for (auto * port : ports)
            {
                auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, threads, key_columns);
                connect(*port, scatter->getInputs().front());
                processors.push_back(scatter);
            }
            return processors;
        });

        if (streams > 1)
        {
            pipeline.transform([&](OutputPortRawPtrs ports)
            {
                Processors processors;
                for (size_t i = 0; i < threads; ++i)
                {
                    size_t output_it = i;
                    auto resize = std::make_shared<ResizeProcessor>(stream_header, streams, 1);
                    auto & inputs = resize->getInputs();

                    for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += threads, ++input_it)
                        connect(*ports[output_it], *input_it);
                    processors.push_back(resize);
                }
                return processors;
            });
        }
    }
}

}
