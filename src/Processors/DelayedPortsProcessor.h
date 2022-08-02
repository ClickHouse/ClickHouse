#pragma once
#include <Processors/IProcessor.h>
#include <base/unit.h>
#include <Processors/Chunk.h>
#include <Common/logger_useful.h>


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
    struct State
    {
        std::atomic_bool waiting = false;
        std::atomic_bool can_push = false;
        std::atomic_size_t idx = 0;
    };
    using StatePtr = std::shared_ptr<State>;

    NotifyProcessor(const Block & header, const Block & aux_header, size_t num_ports, StatePtr sync_state_);

    String getName() const override { return "NotifyProcessor"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;
    void work() override;

    std::pair<InputPort *, OutputPort *> getAuxPorts();


    virtual bool canSend() const = 0;

    virtual void dataCallback(const Chunk & chunk) { UNUSED(chunk); }

protected:
    struct PortsPair
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;
    };

    bool sendPing();
    bool recievePing();
    virtual String log() = 0;

    bool isPairsFinished() const;
    bool processPair(PortsPair & pair);
    void finishPair(PortsPair & pair);
    Status processRegularPorts(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs);

    std::vector<PortsPair> port_pairs;
    size_t num_finished_pairs = 0;

    InputPort & aux_in_port;
    OutputPort & aux_out_port;

    bool is_send = false;
    bool is_recieved = false;

    bool set_needed_once = false;
    StatePtr sync_state;

    size_t idx;
};


class NotifyProcessor2 : public NotifyProcessor
{
public:
    NotifyProcessor2(const Block & header, const Block & aux_header, size_t num_ports, size_t size_, NotifyProcessor::StatePtr sync_state_)
        : NotifyProcessor(header, aux_header, num_ports, sync_state_)
        , size(size_)
    {
    }

    bool canSend() const override
    {
        return isPairsFinished() || data_consumed > size;
    }


    void dataCallback(const Chunk & chunk) override
    {
        data_consumed += chunk.getNumRows();
        // LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} {}: data_consumed {}", __FILE__, __LINE__, getDescription(), data_consumed);
    }

    String log() override
    {
        return fmt::format("data {} / {} = {:.2f}", data_consumed, size, data_consumed / float(size));
    }

private:
    size_t data_consumed = 0;

    size_t size;
};

}
