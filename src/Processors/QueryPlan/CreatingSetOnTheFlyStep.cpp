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

CreatingSetOnTheFlyStep::CreatingSetOnTheFlyStep(const DataStream & input_stream_, const Names & column_names_, const SizeLimits & size_limits)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(false))
    , column_names(column_names_)
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    set = std::make_shared<SetWithState>(size_limits, false, true);

    {
        ColumnsWithTypeAndName header;
        for (const auto & name : column_names)
        {
            ColumnWithTypeAndName column = input_streams[0].header.getByName(name);
            header.emplace_back(column);
        }
        set->setHeader(header);
    }
}

void CreatingSetOnTheFlyStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    UNUSED(settings);
    size_t num_streams = pipeline.getNumStreams();
    // pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;
        auto res = std::make_shared<CreatingSetsOnTheFlyTransform>(header, column_names, num_streams, set);
        res->setDescription(this->getStepDescription());
        return res;
    });
    // pipeline.resize(num_streams);
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


FilterBySetOnTheFlyStep::FilterBySetOnTheFlyStep(const DataStream & input_stream_, const Block & rhs_input_stream_header_,
                                                 const Names & column_names_, size_t buffer_size_,
                                                 SetWithStatePtr set_, PortsStatePtr ports_state_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(true))
    , column_names(column_names_)
    , buffer_size(buffer_size_)
    , rhs_input_stream_header(rhs_input_stream_header_.cloneEmpty())
    , set(set_)
    , ports_state(ports_state_)
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());
}


static InputPorts::iterator connectAllInputs(OutputPortRawPtrs ports, InputPorts & inputs, size_t num_ports)
{
    auto input_it = inputs.begin();
    for (size_t i = 0; i < num_ports; ++i)
    {
        connect(*ports[i], *input_it);
        input_it++;
    }
    return input_it;
}

void FilterBySetOnTheFlyStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    UNUSED(settings);
    UNUSED(connectAllInputs);

    UNUSED(buffer_size);

    Block input_header = pipeline.getHeader();
    pipeline.transform([&input_header, this](OutputPortRawPtrs ports)
    {
        Processors transforms;

        size_t num_ports = ports.size();

        auto notifier = std::make_shared<NotifyProcessor2>(input_header, rhs_input_stream_header, num_ports, buffer_size, ports_state->sync_state);
        notifier->setDescription(getStepDescription());

        auto input_it = connectAllInputs(ports, notifier->getInputs(), num_ports);
        assert(&*input_it == notifier->getAuxPorts().first);
        input_it++;
        assert(input_it == notifier->getInputs().end());

        ports_state->tryConnectPorts(notifier->getAuxPorts(), notifier.get());
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} {} / {}", __FILE__, __LINE__,
            notifier->getAuxPorts().first->isConnected(), notifier->getAuxPorts().second->isConnected());

        auto & outputs = notifier->getOutputs();
        auto output_it = outputs.begin();
        for (size_t i = 0; i < outputs.size() - 1; ++i)
        {
            auto & port = *output_it++;
            auto transform = std::make_shared<FilterBySetOnTheFlyTransform>(port.getHeader(), column_names, set);
            transform->setDescription(this->getStepDescription());
            connect(port, transform->getInputPort());
            transforms.emplace_back(std::move(transform));
        }
        output_it++;
        assert(output_it == outputs.end());
        transforms.emplace_back(std::move(notifier));

        return transforms;
    }, /* check_ports= */ false);

    /*
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;
        auto res = std::make_shared<FilterBySetOnTheFlyTransform>(header, column_names, set);
        res->setDescription(this->getStepDescription());
        return res;
    });
    */
}

void FilterBySetOnTheFlyStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add(getName(), true);
}

void FilterBySetOnTheFlyStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << getName();

    settings.out << '\n';
}

void FilterBySetOnTheFlyStep::updateOutputStream()
{
    if (input_streams.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} requires exactly one input stream, got {}", getName(), input_streams.size());

    output_stream = input_streams[0];
}


}
