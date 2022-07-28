#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <variant>
#include <cstdint>

#include <Core/Block.h>
#include <Core/Defines.h>
#include <Processors/Chunk.h>
#include <Common/Exception.h>

namespace DB
{

class InputPort;
class OutputPort;
class IProcessor;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Port
{
    friend void connect(OutputPort &, InputPort &);
    friend class IProcessor;

public:
    struct UpdateInfo
    {
        using UpdateList = std::vector<void *>;

        UpdateList * update_list = nullptr;
        void * id = nullptr;
        UInt64 version = 0;
        UInt64 prev_version = 0;

        void inline ALWAYS_INLINE update()
        {
            if (version == prev_version && update_list)
                update_list->push_back(id);

            ++version;
        }

        void inline ALWAYS_INLINE trigger() { prev_version = version; }
    };

protected:
    /// Shared state of two connected ports.
    class State
    {
    public:

        struct Data
        {
            /// Note: std::variant can be used. But move constructor for it can't be inlined.
            Chunk chunk;
            std::exception_ptr exception;
        };

    private:
        static std::uintptr_t getUInt(Data * data) { return reinterpret_cast<std::uintptr_t>(data); }
        static Data * getPtr(std::uintptr_t data) { return reinterpret_cast<Data *>(data); }

    public:

        /// Flags for Port state.
        /// Will store them in least pointer bits.

        /// Port was set finished or closed.
        static constexpr std::uintptr_t IS_FINISHED = 1;
        /// Block is not needed right now, but may be will be needed later.
        /// This allows to pause calculations if we are not sure that we need more data.
        static constexpr std::uintptr_t IS_NEEDED = 2;
        /// Check if port has data.
        static constexpr std::uintptr_t HAS_DATA = 4;

        static constexpr std::uintptr_t FLAGS_MASK = IS_FINISHED | IS_NEEDED | HAS_DATA;
        static constexpr std::uintptr_t PTR_MASK = ~FLAGS_MASK;

        /// Tiny smart ptr class for Data. Takes into account that ptr can have flags in least bits.
        class DataPtr
        {
        public:
            DataPtr() : data(new Data())
            {
                if (unlikely((getUInt(data) & FLAGS_MASK) != 0))
                    throw Exception("Not alignment memory for Port.", ErrorCodes::LOGICAL_ERROR);
            }
            /// Pointer can store flags in case of exception in swap.
            ~DataPtr() { delete getPtr(getUInt(data) & PTR_MASK); }

            DataPtr(DataPtr const &) : data(new Data()) {}
            DataPtr& operator=(DataPtr const &) = delete;

            Data * operator->() const { return data; }
            Data & operator*()  const { return *data; }

            Data * get() const { return data; }
            explicit operator bool() const { return data; }

            Data * release()
            {
                Data * result = nullptr;
                std::swap(result, data);
                return result;
            }

            uintptr_t ALWAYS_INLINE swap(std::atomic<Data *> & value, std::uintptr_t flags, std::uintptr_t mask)
            {
                Data * expected = nullptr;
                Data * desired = getPtr(flags | getUInt(data));

                while (!value.compare_exchange_weak(expected, desired))
                    desired = getPtr((getUInt(expected) & FLAGS_MASK & (~mask)) | flags | getUInt(data));

                /// It's not very safe. In case of exception after exchange and before assignment we will get leak.
                /// Don't know how to make it better.
                data = getPtr(getUInt(expected) & PTR_MASK);

                return getUInt(expected) & FLAGS_MASK;
            }

        private:
            Data * data = nullptr;
        };

        /// Not finished, not needed, has not data.
        State() : data(new Data())
        {
            if (unlikely((getUInt(data) & FLAGS_MASK) != 0))
                throw Exception("Not alignment memory for Port.", ErrorCodes::LOGICAL_ERROR);
        }

        ~State()
        {
            Data * desired = nullptr;
            Data * expected = nullptr;

            while (!data.compare_exchange_weak(expected, desired));

            expected = getPtr(getUInt(expected) & PTR_MASK);
            delete expected;
        }

        void ALWAYS_INLINE push(DataPtr & data_, std::uintptr_t & flags)
        {
            flags = data_.swap(data, HAS_DATA, HAS_DATA);

            /// It's possible to push data into finished port. Will just ignore it.
            /// if (flags & IS_FINISHED)
            ///    throw Exception("Cannot push block to finished port.", ErrorCodes::LOGICAL_ERROR);

            /// It's possible to push data into port which is not needed now.
            /// if ((flags & IS_NEEDED) == 0)
            ///    throw Exception("Cannot push block to port which is not needed.", ErrorCodes::LOGICAL_ERROR);

            if (unlikely(flags & HAS_DATA))
                throw Exception("Cannot push block to port which already has data.", ErrorCodes::LOGICAL_ERROR);
        }

        void ALWAYS_INLINE pull(DataPtr & data_, std::uintptr_t & flags, bool set_not_needed = false)
        {
            uintptr_t mask = HAS_DATA;

            if (set_not_needed)
                mask |= IS_NEEDED;

            flags = data_.swap(data, 0, mask);

            /// It's ok to check because this flag can be changed only by pulling thread.
            if (unlikely((flags & IS_NEEDED) == 0) && !set_not_needed)
                throw Exception("Cannot pull block from port which is not needed.", ErrorCodes::LOGICAL_ERROR);

            if (unlikely((flags & HAS_DATA) == 0))
                throw Exception("Cannot pull block from port which has no data.", ErrorCodes::LOGICAL_ERROR);
        }

        std::uintptr_t ALWAYS_INLINE setFlags(std::uintptr_t flags, std::uintptr_t mask)
        {
            Data * expected = nullptr;
            Data * desired = getPtr(flags);

            while (!data.compare_exchange_weak(expected, desired))
                desired = getPtr((getUInt(expected) & FLAGS_MASK & (~mask)) | flags | (getUInt(expected) & PTR_MASK));

            return getUInt(expected) & FLAGS_MASK;
        }

        std::uintptr_t ALWAYS_INLINE getFlags() const
        {
            return getUInt(data.load()) & FLAGS_MASK;
        }

    private:
        std::atomic<Data *> data;
    };

    Block header;
    std::shared_ptr<State> state;

    /// This object is only used for data exchange between port and shared state.
    State::DataPtr data;

    IProcessor * processor = nullptr;

    /// If update_info was set, will call update() for it in case port's state have changed.
    UpdateInfo * update_info = nullptr;

public:
    using Data = State::Data;

    Port(Block header_) : header(std::move(header_)) {}
    Port(Block header_, IProcessor * processor_) : header(std::move(header_)), processor(processor_) {}

    void setUpdateInfo(UpdateInfo * info) { update_info = info; }

    const Block & getHeader() const { return header; }
    bool ALWAYS_INLINE isConnected() const { return state != nullptr; }

    void ALWAYS_INLINE assumeConnected() const
    {
        if (unlikely(!isConnected()))
            throw Exception("Port is not connected", ErrorCodes::LOGICAL_ERROR);
    }

    bool ALWAYS_INLINE hasData() const
    {
        assumeConnected();
        return state->getFlags() & State::HAS_DATA;
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

protected:
    void inline ALWAYS_INLINE updateVersion()
    {
        if (likely(update_info))
            update_info->update();
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

    mutable bool is_finished = false;

public:
    using Port::Port;

    Data ALWAYS_INLINE pullData(bool set_not_needed = false)
    {
        if (!set_not_needed)
            updateVersion();

        assumeConnected();

        std::uintptr_t flags = 0;
        state->pull(data, flags, set_not_needed);

        is_finished = flags & State::IS_FINISHED;

        if (unlikely(!data->exception && data->chunk.getNumColumns() != header.columns()))
        {
            auto & chunk = data->chunk;

            String msg = "Invalid number of columns in chunk pulled from OutputPort. Expected "
                         + std::to_string(header.columns()) + ", found " + std::to_string(chunk.getNumColumns()) + '\n';

            msg += "Header: " + header.dumpStructure() + '\n';
            msg += "Chunk: " + chunk.dumpStructure() + '\n';

            throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
        }

        return std::move(*data);
    }

    Chunk ALWAYS_INLINE pull(bool set_not_needed = false)
    {
        auto data_ = pullData(set_not_needed);

        if (data_.exception)
            std::rethrow_exception(data_.exception);

        return std::move(data_.chunk);
    }

    bool ALWAYS_INLINE isFinished() const
    {
        assumeConnected();

        if (is_finished)
            return true;

        auto flags = state->getFlags();

        is_finished = (flags & State::IS_FINISHED) && ((flags & State::HAS_DATA) == 0);

        return is_finished;
    }

    void ALWAYS_INLINE setNeeded()
    {
        assumeConnected();

        if ((state->setFlags(State::IS_NEEDED, State::IS_NEEDED) & State::IS_NEEDED) == 0)
            updateVersion();
    }

    void ALWAYS_INLINE setNotNeeded()
    {
        assumeConnected();
        state->setFlags(0, State::IS_NEEDED);
    }

    void ALWAYS_INLINE close()
    {
        assumeConnected();

        if ((state->setFlags(State::IS_FINISHED, State::IS_FINISHED) & State::IS_FINISHED) == 0)
            updateVersion();

        is_finished = true;
    }

    void ALWAYS_INLINE reopen()
    {
        assumeConnected();

        if (!isFinished())
            return;

        state->setFlags(0, State::IS_FINISHED);
        is_finished = false;
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
///   * You can push only if port doesn't hasData().
class OutputPort : public Port
{
    friend void connect(OutputPort &, InputPort &);

private:
    InputPort * input_port = nullptr;

public:
    using Port::Port;

    void ALWAYS_INLINE push(Chunk chunk)
    {
        pushData({.chunk = std::move(chunk), .exception = {}});
    }

    void ALWAYS_INLINE push(std::exception_ptr exception)
    {
        pushData({.chunk = {}, .exception = std::move(exception)});
    }

    void ALWAYS_INLINE pushData(Data data_)
    {
        if (unlikely(!data_.exception && data_.chunk.getNumColumns() != header.columns()))
        {
            String msg = "Invalid number of columns in chunk pushed to OutputPort. Expected "
                         + std::to_string(header.columns())
                         + ", found " + std::to_string(data_.chunk.getNumColumns()) + '\n';

            msg += "Header: " + header.dumpStructure() + '\n';
            msg += "Chunk: " + data_.chunk.dumpStructure() + '\n';

            throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
        }

        updateVersion();

        assumeConnected();

        std::uintptr_t flags = 0;
        *data = std::move(data_);
        state->push(data, flags);
    }

    void ALWAYS_INLINE finish()
    {
        assumeConnected();

        auto flags = state->setFlags(State::IS_FINISHED, State::IS_FINISHED);

        if ((flags & State::IS_FINISHED) == 0)
            updateVersion();
    }

    bool ALWAYS_INLINE isNeeded() const
    {
        assumeConnected();
        return state->getFlags() & State::IS_NEEDED;
    }

    bool ALWAYS_INLINE isFinished() const
    {
        assumeConnected();
        return state->getFlags() & State::IS_FINISHED;
    }

    bool ALWAYS_INLINE canPush() const
    {
        assumeConnected();
        auto flags = state->getFlags();
        return (flags & State::IS_NEEDED) && ((flags & State::HAS_DATA) == 0);
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


using InputPorts = std::list<InputPort>;
using OutputPorts = std::list<OutputPort>;


void connect(OutputPort & output, InputPort & input);

}
