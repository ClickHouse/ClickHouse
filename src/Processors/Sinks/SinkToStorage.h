#pragma once
#include <Processors/IProcessor.h>
#include <Storages/TableLockHolder.h>

namespace DB
{

class ThreadStatus;

/// Has one input and one output.
/// Works similarly to ISimpleTransform, but with much care about exceptions.
///
/// If input contain exception, this exception is pushed directly to output port.
/// If input contain data chunk, transform() is called for it.
/// When transform throws exception itself, data chunk is replaced by caught exception.
/// Transformed chunk or newly caught exception is pushed to output.
///
/// There may be any number of exceptions read from input, transform keeps the order.
/// It is expected that output port won't be closed from the other side before all data is processed.
///
/// Method onStart() is called before reading any data.
/// Method onFinish() is called after all data from input is processed, if no exception happened.
/// In case of exception, it is additionally pushed into pipeline.
class ExceptionKeepingTransform : public IProcessor
{
private:
    InputPort & input;
    OutputPort & output;
    Port::Data data;

    bool ready_input = false;
    bool ready_output = false;
    bool has_exception = false;
    bool was_on_start_called = false;
    bool was_on_finish_called = false;

protected:
    virtual void transform(Chunk & chunk) = 0;
    virtual void onStart() {}
    virtual void onFinish() {}

public:
    ExceptionKeepingTransform(const Block & in_header, const Block & out_header);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    struct RuntimeData
    {
        std::unique_ptr<ThreadStatus> thread_status = nullptr;
        UInt64 elapsed_ms = 0;
        std::string additional_exception_message;
    };

    using RuntimeDataPtr = std::shared_ptr<RuntimeData>;

    void setRuntimeData(RuntimeDataPtr runtime_data_) { runtime_data = std::move(runtime_data_); }

private:
    RuntimeDataPtr runtime_data;
};


/// Sink which is returned from Storage::read.
class SinkToStorage : public ExceptionKeepingTransform
{
public:
    explicit SinkToStorage(const Block & header);

    const Block & getHeader() const { return inputs.front().getHeader(); }
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

protected:
    virtual void consume(Chunk chunk) = 0;
    virtual bool lastBlockIsDuplicate() const { return false; }

private:
    std::vector<TableLockHolder> table_locks;

    void transform(Chunk & chunk) override;
};

using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;


class NullSinkToStorage : public SinkToStorage
{
public:
    using SinkToStorage::SinkToStorage;
    std::string getName() const override { return "NullSinkToStorage"; }
    void consume(Chunk) override {}
};

}
