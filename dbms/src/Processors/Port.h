#pragma once

#include <memory>
#include <vector>
#include <variant>

#include <Core/Block.h>
#include <Processors/Chunk.h>
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
        using Data = std::pair<Chunk, std::exception_ptr>;

        State() = default;

        void ALWAYS_INLINE pushData(Data data_)
        {
            if (finished)
                throw Exception("Cannot push block to finished port.", ErrorCodes::LOGICAL_ERROR);

            if (!needed)
                throw Exception("Cannot push block to port which is not needed.", ErrorCodes::LOGICAL_ERROR);

            if (has_data)
                throw Exception("Cannot push block to port which already has data.", ErrorCodes::LOGICAL_ERROR);

            data = std::move(data_);
            has_data = true;
        }

        void ALWAYS_INLINE push(Chunk chunk)
        {
            pushData({std::move(chunk), {}});
        }

        void ALWAYS_INLINE push(std::exception_ptr exception)
        {
            pushData({Chunk(), std::move(exception)});
        }

        auto ALWAYS_INLINE pullData()
        {
            if (!needed)
                throw Exception("Cannot pull block from port which is not needed.", ErrorCodes::LOGICAL_ERROR);

            if (!has_data)
                throw Exception("Cannot pull block from port which has no data.", ErrorCodes::LOGICAL_ERROR);

            has_data = false;

            return std::move(data);
        }

        Chunk ALWAYS_INLINE pull()
        {
            auto cur_data = pullData();

            if (cur_data.second)
                std::rethrow_exception(std::move(cur_data.second));

            return std::move(cur_data.first);
        }

        bool ALWAYS_INLINE hasData() const
        {
// TODO: check for output port only.
//            if (finished)
//                throw Exception("Finished port can't has data.", ErrorCodes::LOGICAL_ERROR);

            if (!needed)
                throw Exception("Cannot check if not needed port has data.", ErrorCodes::LOGICAL_ERROR);

            return has_data;
        }

        /// Only for output port.
        /// If port still has data, it will be finished after pulling.
        void ALWAYS_INLINE finish()
        {
            finished = true;
        }

        /// Only for input port. Removes data if has.
        void ALWAYS_INLINE close()
        {
            finished = true;
            has_data = false;

            data.first.clear();
        }

        /// Only empty ports are finished.
        bool ALWAYS_INLINE isFinished() const { return finished && !has_data; }
        bool ALWAYS_INLINE isSetFinished() const { return finished; }

        void ALWAYS_INLINE setNeeded()
        {
            if (isFinished())
                throw Exception("Can't set port needed if it is finished.", ErrorCodes::LOGICAL_ERROR);

//            if (has_data)
//                throw Exception("Can't set port needed if it has data.", ErrorCodes::LOGICAL_ERROR);

            needed = true;
        }

        void ALWAYS_INLINE setNotNeeded()
        {
//            if (finished)
//                throw Exception("Can't set port not needed if it is finished.", ErrorCodes::LOGICAL_ERROR);

            needed = false;
        }

        /// Only for output port.
        bool ALWAYS_INLINE isNeeded() const { return needed && !finished; }

    private:
        Data data;
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
    using Data = State::Data;

    Port(Block header) : header(std::move(header)) {}
    Port(Block header, IProcessor * processor) : header(std::move(header)), processor(processor) {}

    const Block & getHeader() const { return header; }
    bool ALWAYS_INLINE isConnected() const { return state != nullptr; }

    void ALWAYS_INLINE assumeConnected() const
    {
        if (!isConnected())
            throw Exception("Port is not connected", ErrorCodes::LOGICAL_ERROR);
    }

    bool ALWAYS_INLINE hasData() const
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

    /// If version was set, it will be increased on each pull.
    UInt64 * version = nullptr;

public:
    using Port::Port;

    void setVersion(UInt64 * value) { version = value; }

    Chunk ALWAYS_INLINE pull()
    {
        if (version)
            ++(*version);

        assumeConnected();
        return state->pull();
    }

    Data ALWAYS_INLINE pullData()
    {
        if (version)
            ++(*version);

        assumeConnected();
        return state->pullData();
    }

    bool ALWAYS_INLINE isFinished() const
    {
        assumeConnected();
        return state->isFinished();
    }

    void ALWAYS_INLINE setNeeded()
    {
        assumeConnected();

        if (!state->isNeeded() && version)
            ++(*version);

        state->setNeeded();
    }

    void ALWAYS_INLINE setNotNeeded()
    {
        assumeConnected();
        state->setNotNeeded();
    }

    void ALWAYS_INLINE close()
    {
        if (version && !isFinished())
            ++(*version);

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

    /// If version was set, it will be increased on each push.
    UInt64 * version = nullptr;

public:
    using Port::Port;

    void setVersion(UInt64 * value) { version = value; }

    void ALWAYS_INLINE push(Chunk chunk)
    {
        if (version)
            ++(*version);

        assumeConnected();

        if (chunk.getNumColumns() != header.columns())
        {
            String msg = "Invalid number of columns in chunk pushed to OutputPort. Expected "
                    + std::to_string(header.columns()) + ", found " + std::to_string(chunk.getNumColumns()) + '\n';

            msg += "Header: " + header.dumpStructure() + '\n';
            msg += "Chunk: " + chunk.dumpStructure() + '\n';

            throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
        }

        state->push(std::move(chunk));
    }

    void ALWAYS_INLINE push(std::exception_ptr exception)
    {
        if (version)
            ++(*version);

        assumeConnected();
        state->push(std::move(exception));
    }

    void ALWAYS_INLINE pushData(Data data)
    {
        if (data.second)
            push(std::move(data.second));
        else
            push(std::move(data.first));
    }

    void ALWAYS_INLINE finish()
    {
        if (version && !isFinished())
            ++(*version);

        assumeConnected();
        state->finish();
    }

    bool ALWAYS_INLINE isNeeded() const
    {
        assumeConnected();
        return state->isNeeded();
    }

    bool ALWAYS_INLINE isFinished() const
    {
        assumeConnected();
        return state->isSetFinished();
    }

    bool ALWAYS_INLINE canPush() const { return isNeeded() && !hasData(); }

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


using InputPorts = std::list<InputPort>;
using OutputPorts = std::list<OutputPort>;


void connect(OutputPort & output, InputPort & input);

}
