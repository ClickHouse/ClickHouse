#pragma once
#include <Processors/IProcessor.h>
#include <base/unit.h>
#include <Processors/Chunk.h>

namespace DB
{

/// Processor with N inputs and N outputs. Only moves data from i-th input to i-th output as is.
/// Some ports are delayed. Delayed ports are processed after other outputs are all finished.
/// Data between ports is not mixed. It is important because this processor can be used before MergingSortedTransform.
/// Delayed ports are appeared after joins, when some non-matched data need to be processed at the end.
class DelayedPortsProcessor : public IProcessor
{
public:
    DelayedPortsProcessor(const Block & header, size_t num_ports, const PortNumbers & delayed_ports, bool assert_main_ports_empty = false);

    String getName() const override { return "DelayedPorts"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:

    struct PortsPair
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_delayed = false;
        bool is_finished = false;
    };

    std::vector<PortsPair> port_pairs;
    const size_t num_delayed_ports;
    size_t num_finished_pairs = 0;
    size_t num_finished_outputs = 0;

    std::vector<size_t> output_to_pair;
    bool are_inputs_initialized = false;

    bool processPair(PortsPair & pair);
    void finishPair(PortsPair & pair);
};


class NotifyProcessor : public IProcessor
{
public:
    NotifyProcessor(const Block & header, size_t num_ports);

    String getName() const override { return "NotifyProcessor"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

    std::pair<InputPort *, OutputPort *> getAuxPorts();

    virtual bool isReady() const { return true; }
    virtual bool isWaiting() const { return false; }

    virtual void dataCallback(const Chunk & chunk) { UNUSED(chunk); }

private:

    enum class AuxPortState
    {
        NotInitialized,
        Triggered,
        Finished,
    };

    struct PortsPair
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;
    };

    bool processPair(PortsPair & pair);
    void finishPair(PortsPair & pair);
    Status processRegularPorts(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs);

    std::vector<PortsPair> port_pairs;
    size_t num_finished_pairs = 0;

    InputPort aux_in_port;
    OutputPort aux_out_port;

    AuxPortState ready = AuxPortState::NotInitialized;
    AuxPortState waiting = AuxPortState::NotInitialized;
};


class NotifyProcessor2 : public NotifyProcessor
{
public:
    using NotifyProcessor::NotifyProcessor;

    bool isReady() const override
    {
        return data_consumed > 10_MiB;
    }

    bool isWaiting() const override
    {
        return data_consumed < 10_MiB;
    }

    void dataCallback(const Chunk & chunk) override
    {
        data_consumed += chunk.allocatedBytes();
    }

private:
    size_t data_consumed = 0;
};

}
