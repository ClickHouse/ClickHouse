#pragma once

#include <memory>
#include <vector>
#include <Core/Block.h>
#include <Common/Exception.h>


namespace DB
{

class InputPort;
class OutputPort;
class IProcessor;


class Port
{
    friend void connect(OutputPort &, InputPort &);
    friend class IProcessor;

protected:
    /// Shared state of two connected ports.
    struct State
    {
        Block data;
        bool needed = false;
        bool finished = false;
    };

    Block header;
    std::shared_ptr<State> state;

    IProcessor * processor = nullptr;

public:
    Port(const Block & header)
        : header(header) {}

    const Block & getHeader() const { return header; }
    bool isConnected() const { return state != nullptr; }

    void assumeConnected() const
    {
        if (!isConnected())
            throw Exception("Port is not connected");
    }

    bool hasData() const
    {
        assumeConnected();
        return state->data;
    }

    bool isNeeded() const
    {
        assumeConnected();
        return state->needed;
    }

    IProcessor & getProcessor()
    {
        if (!processor)
            throw Exception("Port does not belong to Processor");
        return *processor;
    }

    const IProcessor & getProcessor() const
    {
        if (!processor)
            throw Exception("Port does not belong to Processor");
        return *processor;
    }
};


class InputPort : public Port
{
    friend void connect(OutputPort &, InputPort &);

private:
    OutputPort * output_port = nullptr;

public:
    using Port::Port;

    Block pull()
    {
        if (!hasData())
            throw Exception("Port has no data");

        return std::move(state->data);
    }

    bool isFinished() const
    {
        assumeConnected();
        return state->finished;
    }

    void setNeeded()
    {
        assumeConnected();
        state->needed = true;
    }

    void setNotNeeded()
    {
        assumeConnected();
        state->needed = false;
    }

    OutputPort & getOutputPort()
    {
        assumeConnected();
        return *output_port;
    }

    const OutputPort & getOutputPort() const
    {
        assumeConnected();
        return *output_port;
    }
};


class OutputPort : public Port
{
    friend void connect(OutputPort &, InputPort &);

private:
    InputPort * input_port = nullptr;

public:
    using Port::Port;

    void push(Block block)
    {
        if (hasData())
            throw Exception("Port already has data");

        state->data = std::move(block);
    }

    void setFinished()
    {
        assumeConnected();
        state->finished = true;
    }

    InputPort & getInputPort()
    {
        assumeConnected();
        return *input_port;
    }

    const InputPort & getInputPort() const
    {
        assumeConnected();
        return *input_port;
    }
};


using InputPorts = std::vector<InputPort>;
using OutputPorts = std::vector<OutputPort>;


void connect(OutputPort & output, InputPort & input);

}
