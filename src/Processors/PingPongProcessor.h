#pragma once

#include <Processors/IProcessor.h>
#include <base/unit.h>
#include <Processors/Chunk.h>
#include <Common/logger_useful.h>

namespace DB
{

/*
 * Processor with N inputs and N outputs. Moves data from i-th input to i-th output as is.
 * It has a pair of auxiliary ports to notify another instance by sending empty chunk after some condition holds.
 * You should use this processor in pair of instances and connect auxiliary ports crosswise.
 *
 *     ╭─┴───┴───┴───┴───┴─╮       ╭─┴───┴───┴───┴───┴─╮
 *     │                   ├─ aux ⟶│                   │
 *     │ PingPongProcessor │       │ PingPongProcessor │
 *     │                   │⟵ aux ─┤                   │
 *     ╰─┬───┬───┬───┬───┬─╯       ╰─┬───┬───┬───┬───┬─╯
 *
 * One of the processors starts processing data, and another waits for notification.
 * When `isReady` returns true, the first stops processing, sends a ping to another and waits for notification.
 * After that, the second one also processes data until `isReady`, then send a notification back to the first one.
 * After this roundtrip, processors bypass data from regular inputs to outputs.
 */
class PingPongProcessor : public IProcessor
{
public:
    enum class Order : uint8_t
    {
        /// Processor that starts processing data.
        First,
        /// Processor that waits for notification.
        Second,
    };

    using enum Order;

    /// The `aux_header` is a header from another instance of procssor.
    /// It's required because all outputs should have the same structure.
    /// We don't care about structure of another processor, because we send just empty chunk, but need to follow the contract.
    PingPongProcessor(const Block & header, const Block & aux_header, size_t num_ports, Order order_);

    String getName() const override { return "PingPongProcessor"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

    std::pair<InputPort *, OutputPort *> getAuxPorts();

    virtual bool isReady(const Chunk & chunk) = 0;

protected:
    struct PortsPair
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;
    };

    bool sendPing();
    bool recievePing();
    bool canSend() const;

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

    bool ready_to_send = false;

    bool set_needed_once = false;

    Order order;
};

/// Reads first N rows from two streams evenly.
class ReadHeadBalancedProceesor : public PingPongProcessor
{
public:
    ReadHeadBalancedProceesor(const Block & header, const Block & aux_header, size_t num_ports, size_t size_, Order order_)
        : PingPongProcessor(header, aux_header, num_ports, order_)
        , size(size_)
    {
    }

    bool isReady(const Chunk & chunk) override
    {
        data_consumed += chunk.getNumRows();
        return data_consumed > size;
    }

private:
    size_t data_consumed = 0;

    size_t size;
};

}
