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
/**
 * 1. InnerShuffleScatterTransform scatter each input block into num_streams blocks. The join keys are
 *   used as the hash keys. num_streams should be a number of power of 2.
 * 2. To avoid createting too many edges between scatter and gather processors. we make a small set of
 *   InnerShuffleDispatchTransform. It collect split chunks from InnerShuffleScatterTransform and
 *   dispatch them into different output ports.
 * 3. InnerShuffleGatherTransform gather data from InnerShuffleDispatchTransforms and merge them into
 *   one outport.
 */
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

    size_t num_streams = pipeline.getNumStreams();
    if (num_streams != alignStreamsNum(static_cast<UInt32>(num_streams)))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The num_streams {} is not a power of 2", num_streams);
    }

    // should not be a high overhead operation, small number is OK.
    size_t max_dispatcher_num = 8;
    size_t dispatchers_num = num_streams/2;
    if (dispatchers_num > max_dispatcher_num)
    {
        dispatchers_num = max_dispatcher_num;
    }
    if (!dispatchers_num)
    {
        dispatchers_num = 1;
    }

    auto add_scatter_transform = [&](OutputPortRawPtrs outports)
    {
        Processors scatters;
        Processors dispatchers;
        std::vector<InputPort *> dispatcher_input_ports;

        if (outports.size() != num_streams)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "The output ports size is expected to be {}, but got {}", num_streams, outports.size());
        }
        for (size_t i = 0; i < dispatchers_num; ++i)
        {
            dispatchers.push_back(std::make_shared<InnerShuffleDispatchTransform>(num_streams/dispatchers_num, num_streams, header));
            for (auto & port : dispatchers.back()->getInputs())
            {
                dispatcher_input_ports.push_back(&port);
            }
        }

        for (auto & outport : outports)
        {
            auto scatter = std::make_shared<InnerShuffleScatterTransform>(num_streams, header, keys);
            connect(*outport, scatter->getInputs().front());
            scatters.push_back(scatter);
        }
        for (size_t i = 0; i < num_streams; ++i)
        {
            connect(scatters[i]->getOutputs().front(), *dispatcher_input_ports[i]);
        }
        for (auto & dispatcher : dispatchers)
        {
            scatters.push_back(dispatcher);
        }
        return scatters;
    };
    pipeline.transform(add_scatter_transform);

    auto add_gather_transform = [&](OutputPortRawPtrs outports)
    {
        Processors gathers;
        for (size_t i = 0; i < num_streams; ++i)
        {
            OutputPortRawPtrs gather_upstream_outports;
            auto gather = std::make_shared<InnerShuffleGatherTransform>(header, dispatchers_num);
            gathers.push_back(gather);
            auto & gather_inputs = gather->getInputs();
            for (size_t j = 0; j < dispatchers_num; ++j)
            {
                gather_upstream_outports.push_back(outports[j * num_streams + i]);
            }
            auto oiter = gather_upstream_outports.begin();
            auto iiter = gather_inputs.begin();
            for (; oiter != gather_upstream_outports.end();)
            {
                connect(**oiter, *iiter);
                oiter++;
                iiter++;
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
UInt32 InnerShuffleStep::alignStreamsNum(UInt32 n)
{
    if (n <= 1)
        return 1;
    return static_cast<UInt32>(1) << (32 - std::countl_zero(n - 1));
}
}
