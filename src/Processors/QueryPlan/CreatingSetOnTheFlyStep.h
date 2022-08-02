#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>
#include <Processors/DelayedPortsProcessor.h>


namespace DB
{

class CreatingSetOnTheFlyStep : public ITransformingStep
{
public:
    explicit CreatingSetOnTheFlyStep(
        const DataStream & input_stream_,
        const Names & column_names_,
        const SizeLimits & size_limits = {});

    String getName() const override { return "CreatingSetsOnTheFly"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    SetWithStatePtr getSet() const { return set; }

private:
    void updateOutputStream() override;

    Names column_names;
    SetWithStatePtr set;
};


class FilterBySetOnTheFlyStep : public ITransformingStep
{
public:

    class PortsState : public boost::noncopyable
    {
    public:
        std::shared_ptr<NotifyProcessor::State> sync_state;

        explicit PortsState()
            : sync_state(std::make_shared<NotifyProcessor::State>())
        {
        }

        using PortPair = std::pair<InputPort *, OutputPort *>;

        /// Remember ports passed on the first call and connect with ones from second call.
        bool tryConnectPorts(PortPair rhs_ports, IProcessor * proc)
        {
            LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} {} {} / {} {}", __FILE__, __LINE__,
                bool(input_port), input_port ? input_port->isConnected() : false,
                bool(output_port), output_port ? output_port->isConnected() : false);
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

    using PortsStatePtr = std::shared_ptr<PortsState>;

    FilterBySetOnTheFlyStep(
        const DataStream & input_stream_,
        const Block & rhs_input_stream_header_,
        const Names & column_names_,
        size_t buffer_size_,
        SetWithStatePtr set_,
        PortsStatePtr ports_state_);

    String getName() const override { return "FilterBySetOnTheFly"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    Names column_names;

    size_t buffer_size;

    Block rhs_input_stream_header;

    SetWithStatePtr set;
    PortsStatePtr ports_state;
};

}
