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
    class State
    {
    public:
        State() = default;

        void push(Block block)
        {
            if (finished)
                throw Exception("Cannot push block to finished port.", ErrorCodes::LOGICAL_ERROR);

            if (!needed)
                throw Exception("Cannot push block to port which is not needed.", ErrorCodes::LOGICAL_ERROR);

            if (has_data)
                throw Exception("Cannot push block to port which already has data.", ErrorCodes::LOGICAL_ERROR);

            data = std::move(block);
            has_data = true;
        }

        Block pull()
        {
            if (!needed)
                throw Exception("Cannot pull block from port which is not needed.", ErrorCodes::LOGICAL_ERROR);

            if (!has_data)
                throw Exception("Cannot pull block from port which has no data.", ErrorCodes::LOGICAL_ERROR);

            has_data = false;
            return std::move(data);
        }

        bool hasData() const
        {
            if (finished)
                throw Exception("Finished port can't has data.", ErrorCodes::LOGICAL_ERROR);

            if (!needed)
                throw Exception("Cannot check if not needed port has data.", ErrorCodes::LOGICAL_ERROR);

            return has_data;
        }

        /// Only for output port.
        /// If port still has data, it will be finished after pulling.
        void finish()
        {
            finished = true;
        }

        /// Only for input port. Removes data if has.
        void close()
        {
            finished = true;
            has_data = false;
            data.clear();
        }

        /// Only empty ports are finished.
        bool isFinished() const { return finished && !has_data; }
        bool isSetFinished() const { return finished; }

        void setNeeded()
        {
            if (finished)
                throw Exception("Can't set port needed if it is finished.", ErrorCodes::LOGICAL_ERROR);

//            if (has_data)
//                throw Exception("Can't set port needed if it has data.", ErrorCodes::LOGICAL_ERROR);

            needed = true;
        }

        void setNotNeeded()
        {
//            if (finished)
//                throw Exception("Can't set port not needed if it is finished.", ErrorCodes::LOGICAL_ERROR);

            needed = false;
        }

        /// Only for output port.
        bool isNeeded() const { return needed && !finished; }

    private:
        Block data;
        /// Use special flag to check if block has data. This allows to send empty blocks between processors.
        bool has_data = false;
        /// Block is not needed right now, but may be will be needed later.
        /// This allows to pause calculations if we are not sure that we need more data.
        bool needed = false;
        /// Port was set finished or closed.
        bool finished = false;
    };

    Block header;
    std::shared_ptr<State> state;

    IProcessor * processor = nullptr;

public:
    Port(Block header) : header(std::move(header)) {}

    const Block & getHeader() const { return header; }
    bool isConnected() const { return state != nullptr; }

    void assumeConnected() const
    {
        if (!isConnected())
            throw Exception("Port is not connected", ErrorCodes::LOGICAL_ERROR);
    }

    bool hasData() const
    {
        assumeConnected();
        return state->hasData();
    }

    IProcessor & getProcessor()
    {
        if (!processor)
            throw Exception("Port does not belong to Processor", ErrorCodes::LOGICAL_ERROR);
        return *processor;
    }

    const IProcessor & getProcessor() const
    {
        if (!processor)
            throw Exception("Port does not belong to Processor", ErrorCodes::LOGICAL_ERROR);
        return *processor;
    }
};

/// Invariants:
///   * If you close port, it isFinished().
///   * If port isFinished(), you can do nothing with it.
///   * If port is not needed, you can only setNeeded() or close() it.
///   * You can pull only if port hasData().
class InputPort : public Port
{
    friend void connect(OutputPort &, InputPort &);

private:
    OutputPort * output_port = nullptr;

public:
    using Port::Port;

    Block pull()
    {
        assumeConnected();
        return state->pull();
    }

    bool isFinished() const
    {
        assumeConnected();
        return state->isFinished();
    }

    void setNeeded()
    {
        assumeConnected();
        state->setNeeded();
    }

    void setNotNeeded()
    {
        assumeConnected();
        state->setNotNeeded();
    }

    void close()
    {
        assumeConnected();
        state->close();
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


/// Invariants:
///   * If you finish port, it isFinished().
///   * If port isFinished(), you can do nothing with it.
///   * If port not isNeeded(), you can only finish() it.
///   * You can hush only if port doesn't hasData().
class OutputPort : public Port
{
    friend void connect(OutputPort &, InputPort &);

private:
    InputPort * input_port = nullptr;

public:
    using Port::Port;

    void push(Block block)
    {
        assumeConnected();
        state->push(std::move(block));
    }

    void finish()
    {
        assumeConnected();
        state->finish();
    }

    bool isNeeded() const
    {
        assumeConnected();
        return state->isNeeded();
    }

    bool isFinished() const
    {
        assumeConnected();
        return state->isSetFinished();
    }

    bool canPush() const { return isNeeded() && !hasData(); }

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
