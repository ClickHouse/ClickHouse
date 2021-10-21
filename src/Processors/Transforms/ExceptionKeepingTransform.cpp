#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Common/ThreadStatus.h>
#include <Common/Stopwatch.h>
#include <base/scope_guard.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ExceptionKeepingTransform::ExceptionKeepingTransform(const Block & in_header, const Block & out_header, bool ignore_on_start_and_finish_)
    : IProcessor({in_header}, {out_header})
    , input(inputs.front()), output(outputs.front())
    , ignore_on_start_and_finish(ignore_on_start_and_finish_)
{
}

IProcessor::Status ExceptionKeepingTransform::prepare()
{
    if (!ignore_on_start_and_finish && !was_on_start_called)
        return Status::Ready;

    /// Check can output.

    if (output.isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output port is finished for {}", getName());

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (ready_output)
    {
        output.pushData(std::move(data));
        ready_output = false;
        return Status::PortFull;
    }

    if (!ready_input)
    {
        if (input.isFinished())
        {
            if (!ignore_on_start_and_finish && !was_on_finish_called && !has_exception)
                return Status::Ready;

            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        data = input.pullData(true);

        if (data.exception)
        {
            has_exception = true;
            output.pushData(std::move(data));
            return Status::PortFull;
        }

        if (has_exception)
            /// In case of exception, just drop all other data.
            /// If transform is stateful, it's state may be broken after exception from transform()
            data.chunk.clear();
        else
            ready_input = true;
    }

    return Status::Ready;
}

static std::exception_ptr runStep(std::function<void()> step, ThreadStatus * thread_status, std::atomic_uint64_t * elapsed_ms)
{
    std::exception_ptr res;
    std::optional<Stopwatch> watch;

    auto * original_thread = current_thread;
    SCOPE_EXIT({ current_thread = original_thread; });

    if (thread_status)
    {
        /// Change thread context to store individual metrics. Once the work in done, go back to the original thread
        thread_status->resetPerformanceCountersLastUsage();
        current_thread = thread_status;
    }

    if (elapsed_ms)
        watch.emplace();

    try
    {
        step();
    }
    catch (...)
    {
        res = std::current_exception();
    }

    if (thread_status)
        thread_status->updatePerformanceCounters();

    if (elapsed_ms && watch)
        *elapsed_ms += watch->elapsedMilliseconds();

    return res;
}

void ExceptionKeepingTransform::work()
{
    if (!ignore_on_start_and_finish && !was_on_start_called)
    {
        was_on_start_called = true;

        if (auto exception = runStep([this] { onStart(); }, thread_status, elapsed_counter_ms))
        {
            has_exception = true;
            ready_output = true;
            data.exception = std::move(exception);
        }
    }
    else if (ready_input)
    {
        ready_input = false;

        if (auto exception = runStep([this] { transform(data.chunk); }, thread_status, elapsed_counter_ms))
        {
            has_exception = true;
            data.chunk.clear();
            data.exception = std::move(exception);
        }

        if (data.chunk || data.exception)
            ready_output = true;
    }
    else if (!ignore_on_start_and_finish && !was_on_finish_called)
    {
        was_on_finish_called = true;

        if (auto exception = runStep([this] { onFinish(); }, thread_status, elapsed_counter_ms))
        {
            has_exception = true;
            ready_output = true;
            data.exception = std::move(exception);
        }
    }
}

void ExceptionKeepingTransform::setRuntimeData(ThreadStatus * thread_status_, std::atomic_uint64_t * elapsed_counter_ms_)
{
    thread_status = thread_status_;
    elapsed_counter_ms = elapsed_counter_ms_;
}

}
