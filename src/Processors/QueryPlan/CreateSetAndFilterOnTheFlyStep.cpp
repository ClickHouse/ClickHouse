#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/Transforms/CreateSetAndFilterOnTheFlyTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/logger_useful.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/IProcessor.h>
#include <Processors/PingPongProcessor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void connectAllInputs(OutputPortRawPtrs ports, InputPorts & inputs, size_t num_ports)
{
    auto input_it = inputs.begin();
    for (size_t i = 0; i < num_ports; ++i)
    {
        connect(*ports[i], *input_it);
        input_it++;
    }
}

static ColumnsWithTypeAndName getColumnSubset(const Block & block, const Names & column_names)
{
    ColumnsWithTypeAndName result;
    for (const auto & name : column_names)
        result.emplace_back(block.getByName(name));
    return result;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

class CreateSetAndFilterOnTheFlyStep::CrosswiseConnection : public boost::noncopyable
{
public:
    using PortPair = std::pair<InputPort *, OutputPort *>;

    /// Remember ports passed on the first call and connect with ones from second call.
    /// Thread-safe.
    void connectPorts(PortPair rhs_ports, IProcessor * proc)
    {
        assert(!rhs_ports.first->isConnected() && !rhs_ports.second->isConnected());

        std::lock_guard lock(mux);
        if (input_port || output_port)
        {
            assert(input_port && output_port);
            assert(!input_port->isConnected());
            connect(*rhs_ports.second, *input_port);
            connect(*output_port, *rhs_ports.first, /* reconnect= */ true);
        }
        else
        {
            std::tie(input_port, output_port) = rhs_ports;
            assert(input_port && output_port);
            assert(!input_port->isConnected() && !output_port->isConnected());

            dummy_input_port = std::make_unique<InputPort>(output_port->getHeader(), proc);
            connect(*output_port, *dummy_input_port);
        }
    }

private:
    std::mutex mux;
    InputPort * input_port = nullptr;
    OutputPort * output_port = nullptr;

    /// Output ports should always be connected, and we can't add a step to the pipeline without them.
    /// So, connect the port from the first processor to this dummy port and then reconnect to the second processor.
    std::unique_ptr<InputPort> dummy_input_port;
};

CreateSetAndFilterOnTheFlyStep::CrosswiseConnectionPtr CreateSetAndFilterOnTheFlyStep::createCrossConnection()
{
    return std::make_shared<CreateSetAndFilterOnTheFlyStep::CrosswiseConnection>();
}

CreateSetAndFilterOnTheFlyStep::CreateSetAndFilterOnTheFlyStep(
    const DataStream & input_stream_,
    const Names & column_names_,
    size_t max_rows_in_set_,
    CrosswiseConnectionPtr crosswise_connection_,
    JoinTableSide position_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , column_names(column_names_)
    , max_rows_in_set(max_rows_in_set_)
    , own_set(std::make_shared<SetWithState>(SizeLimits(max_rows_in_set, 0, OverflowMode::BREAK), 0, true))
    , filtering_set(nullptr)
    , crosswise_connection(crosswise_connection_)
    , position(position_)
{
    if (crosswise_connection == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Crosswise connection is not initialized");

    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Step requires exactly one input stream, got {}", input_streams.size());

    own_set->setHeader(getColumnSubset(input_streams[0].header, column_names));
}

void CreateSetAndFilterOnTheFlyStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    size_t num_streams = pipeline.getNumStreams();
    pipeline.addSimpleTransform([this, num_streams](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;
        auto res = std::make_shared<CreatingSetsOnTheFlyTransform>(header, column_names, num_streams, own_set);
        res->setDescription(this->getStepDescription());
        return res;
    });

    Block input_header = pipeline.getHeader();
    auto pipeline_transform = [&input_header, this](OutputPortRawPtrs ports)
    {
        Processors result_transforms;

        size_t num_ports = ports.size();

        /// Add balancing transform
        auto idx = position == JoinTableSide::Left ? PingPongProcessor::First : PingPongProcessor::Second;
        auto stream_balancer = std::make_shared<ReadHeadBalancedProcessor>(input_header, num_ports, max_rows_in_set, idx);
        stream_balancer->setDescription("Reads rows from two streams evenly");

        /// Regular inputs just bypass data for respective ports
        connectAllInputs(ports, stream_balancer->getInputs(), num_ports);

        /// Connect auxiliary ports
        crosswise_connection->connectPorts(stream_balancer->getAuxPorts(), stream_balancer.get());

        if (!filtering_set)
        {
            LOG_DEBUG(log, "Skip filtering {} stream", position);
            result_transforms.emplace_back(std::move(stream_balancer));
            return result_transforms;
        }

        /// Add filtering transform, ports just connected respectively
        auto & outputs = stream_balancer->getOutputs();
        auto output_it = outputs.begin();
        for (size_t i = 0; i < outputs.size() - 1; ++i)
        {
            auto & port = *output_it++;
            auto transform = std::make_shared<FilterBySetOnTheFlyTransform>(port.getHeader(), column_names, filtering_set);
            transform->setDescription("Filter rows using other join table side's set");
            connect(port, transform->getInputPort());
            result_transforms.emplace_back(std::move(transform));
        }
        assert(output_it == std::prev(outputs.end()));
        result_transforms.emplace_back(std::move(stream_balancer));

        return result_transforms;
    };

    /// Auxiliary port stream_balancer can be connected later (by crosswise_connection).
    /// So, use unsafe `transform` with `check_ports = false` to avoid assertions
    pipeline.transform(std::move(pipeline_transform), /* check_ports= */ false);
}

void CreateSetAndFilterOnTheFlyStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add(getName(), true);
}

void CreateSetAndFilterOnTheFlyStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << getName();

    settings.out << '\n';
}

void CreateSetAndFilterOnTheFlyStep::updateOutputStream()
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    own_set->setHeader(getColumnSubset(input_streams[0].header, column_names));

    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
}

bool CreateSetAndFilterOnTheFlyStep::isColumnPartOfSetKey(const String & column_name) const
{
    return std::find(column_names.begin(), column_names.end(), column_name) != column_names.end();
}

}
