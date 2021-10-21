#pragma once
#include <Processors/IProcessor.h>

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
protected:
    InputPort & input;
    OutputPort & output;
    Port::Data data;

    bool ready_input = false;
    bool ready_output = false;
    bool has_exception = false;

    const bool ignore_on_start_and_finish = true;
    bool was_on_start_called = false;
    bool was_on_finish_called = false;

//protected:
    virtual void transform(Chunk & chunk) = 0;
    virtual void onStart() {}
    virtual void onFinish() {}

public:
    ExceptionKeepingTransform(const Block & in_header, const Block & out_header, bool ignore_on_start_and_finish_ = true);

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    void setRuntimeData(ThreadStatus * thread_status_, std::atomic_uint64_t * elapsed_counter_ms_);

private:
    ThreadStatus * thread_status = nullptr;
    std::atomic_uint64_t * elapsed_counter_ms = nullptr;
};

}
