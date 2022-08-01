#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>


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
        using PortPair = std::pair<InputPort *, OutputPort *>;

        /// Remember ports passed on the first call and connect with ones from second call.
        bool tryConnectPorts(PortPair rhs_ports)
        {
            std::lock_guard<std::mutex> lock(mux);
            if (input_port || output_port)
            {
                assert(input_port && output_port);
                connect(*rhs_ports.second, *input_port);
                connect(*output_port, *rhs_ports.first);
                return true;
            }
            std::tie(input_port, output_port) = rhs_ports;
            return false;
        }

    private:
        std::mutex mux;
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
    };

    using PortsStatePtr = std::shared_ptr<PortsState>;

    FilterBySetOnTheFlyStep(
        const DataStream & input_stream_,
        const Names & column_names_,
        SetWithStatePtr set_,
        PortsStatePtr ports_state_);

    String getName() const override { return "FilterBySetOnTheFly"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    Names column_names;

    SetWithStatePtr set;
    PortsStatePtr ports_state;
};

}
