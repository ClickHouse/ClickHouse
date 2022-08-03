#include <Processors/QueryPlan/CreatingSetOnTheFlyStep.h>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/IProcessor.h>
#include <Processors/DelayedPortsProcessor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{

InputPorts::iterator connectAllInputs(OutputPortRawPtrs ports, InputPorts & inputs, size_t num_ports)
{
    auto input_it = inputs.begin();
    for (size_t i = 0; i < num_ports; ++i)
    {
        connect(*ports[i], *input_it);
        input_it++;
    }
    return input_it;
}

}

static ITransformingStep::Traits getTraits(bool is_filter)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false, /// resize doesn't perserve sorting (TODO fixit)
        },
        {
            .preserves_number_of_rows = !is_filter,
        }
    };
}


class CreatingSetOnTheFlyStep::CrosswiseConnection : public boost::noncopyable
{
public:
    using PortPair = std::pair<InputPort *, OutputPort *>;

    /// Remember ports passed on the first call and connect with ones from second call.
    bool tryConnectPorts(PortPair rhs_ports, IProcessor * proc)
    {
        std::lock_guard<std::mutex> lock(mux);
        if (input_port || output_port)
        {
            assert(input_port && output_port);
            assert(!input_port->isConnected());
            connect(*rhs_ports.second, *input_port);
            connect(*output_port, *rhs_ports.first, /* reconnect= */ true);
            return true;
        }
        std::tie(input_port, output_port) = rhs_ports;
        assert(input_port && output_port);
        assert(!input_port->isConnected() && !output_port->isConnected());

        dummy_input_port = std::make_unique<InputPort>(output_port->getHeader(), proc);
        connect(*output_port, *dummy_input_port);
        return false;
    }

private:
    std::mutex mux;
    InputPort * input_port = nullptr;
    OutputPort * output_port = nullptr;

    std::unique_ptr<InputPort> dummy_input_port;
};

CreatingSetOnTheFlyStep::CrosswiseConnectionPtr CreatingSetOnTheFlyStep::createCrossConnection()
{
    return std::make_shared<CreatingSetOnTheFlyStep::CrosswiseConnection>();
}

CreatingSetOnTheFlyStep::CreatingSetOnTheFlyStep(
    const DataStream & input_stream_,
    const DataStream & rhs_input_stream_,
    const Names & column_names_,
    size_t max_rows_,
    CrosswiseConnectionPtr crosswise_connection_,
    JoinTableSide position_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(false))
    , column_names(column_names_)
    , max_rows(max_rows_)
    , rhs_input_stream_header(rhs_input_stream_.header)
    , own_set(std::make_shared<SetWithState>(SizeLimits(max_rows, 0, OverflowMode::BREAK), false, true))
    , filtering_set(nullptr)
    , crosswise_connection(crosswise_connection_)
    , position(position_)
{
    if (crosswise_connection == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Crosswise connection is not initialized");

    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    ColumnsWithTypeAndName header;
    for (const auto & name : column_names)
        header.emplace_back(input_streams[0].header.getByName(name));
    own_set->setHeader(header);
}

void CreatingSetOnTheFlyStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
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

    if (!filtering_set)
    {
        LOG_DEBUG(log, "filtering_set is null");
        return;
    }

    Block input_header = pipeline.getHeader();
    pipeline.transform([&input_header, this](OutputPortRawPtrs ports)
    {
        Processors transforms;

        size_t num_ports = ports.size();

        auto idx = position == JoinTableSide::Left ? PingPongProcessor::First : PingPongProcessor::Second;
        auto notifier = std::make_shared<ReadHeadBalancedProceesor>(input_header, rhs_input_stream_header, num_ports, max_rows, idx);
        notifier->setDescription(getStepDescription());

        auto input_it = connectAllInputs(ports, notifier->getInputs(), num_ports);
        assert(&*input_it == notifier->getAuxPorts().first);
        input_it++;
        assert(input_it == notifier->getInputs().end());

        crosswise_connection->tryConnectPorts(notifier->getAuxPorts(), notifier.get());

        auto & outputs = notifier->getOutputs();
        auto output_it = outputs.begin();
        for (size_t i = 0; i < outputs.size() - 1; ++i)
        {
            auto & port = *output_it++;
            auto transform = std::make_shared<FilterBySetOnTheFlyTransform>(port.getHeader(), column_names, filtering_set);
            transform->setDescription(this->getStepDescription());
            connect(port, transform->getInputPort());
            transforms.emplace_back(std::move(transform));
        }
        output_it++;
        assert(output_it == outputs.end());
        transforms.emplace_back(std::move(notifier));

        return transforms;
    }, /* check_ports= */ false);
}

void CreatingSetOnTheFlyStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add(getName(), true);
}

void CreatingSetOnTheFlyStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << getName();

    settings.out << '\n';
}

void CreatingSetOnTheFlyStep::updateOutputStream()
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    output_stream = input_streams[0];
}


}
