#include "InnerShuffleStep.h"
#include <memory>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/InnerShuffleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <base/types.h>
#include <Processors/Port.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
static ITransformingStep::Traits getTraits(const DataStream& /*input_stream_*/)
{
    return ITransformingStep::Traits
    {
        .data_stream_traits =
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        .transform_traits =
        {
            .preserves_number_of_rows = false,
        }
    };
}

InnerShuffleStep::InnerShuffleStep(const DataStream & input_stream_, const std::vector<String> & hash_columns_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(input_stream_))
    , hash_columns(hash_columns_)
{
}

void InnerShuffleStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    const auto & header = pipeline.getHeader();
    std::vector<size_t> keys;
    keys.reserve(hash_columns.size());
    for (const auto & key_name : hash_columns)
    {
        keys.push_back(header.getPositionByName(key_name));
    }
    OutputPortRawPtrs current_outports;

    // Split one block into n blocks by hash function, n is equal to num_streams.
    // One input port will have n output ports.
    // The result blocks are mark by hash id (0 <= id < num_streams), and are delivered into
    // different output ports.
    size_t num_streams = pipeline.getNumStreams();
    assert(num_streams > 1);
    auto add_scatter_transform = [&](OutputPortRawPtrs outports)
    {
        Processors scatters;
        if (outports.size() != num_streams)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "The output ports size is expected to be {}, but got {}", num_streams, outports.size());
        }
        for (auto & outport : outports)
        {
            auto scatter = std::make_shared<InnerShuffleScatterTransform>(num_streams, header, keys);
            connect(*outport, scatter->getInputs().front());
            scatters.push_back(scatter);
        }
        return scatters;
    };
    pipeline.transform(add_scatter_transform);

    // Gather the blocks from the upstream output porst marked with the same id.
    auto add_gather_transform = [&](OutputPortRawPtrs outports)
    {
        Processors gathers;
        assert(outports.size() == num_streams * num_streams);
        for (size_t i = 0; i < num_streams; ++i)
        {
            OutputPortRawPtrs gather_upstream_outports;
            auto gather = std::make_shared<InnerShuffleGatherTransform>(header, num_streams);
            gathers.push_back(gather);
            auto & gather_inputs = gather->getInputs();
            for (size_t j = 0; j < num_streams; ++j)
            {
                gather_upstream_outports.push_back(outports[j * num_streams + i]);
            }
            auto oiter = gather_upstream_outports.begin();
            auto iiter = gather_inputs.begin();
            for (; oiter != gather_upstream_outports.end(); oiter++, iiter++)
            {
                connect(**oiter, *iiter);
            }
        }
        return gathers;
    };
    pipeline.transform(add_gather_transform);
}

void InnerShuffleStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        input_streams.front().header,
        getDataStreamTraits());
}
}
