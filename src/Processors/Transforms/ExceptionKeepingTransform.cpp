#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Common/ThreadStatus.h>
#include <Common/Stopwatch.h>
#include <common/scope_guard.h>
#include <iostream>

namespace DB
{

ExceptionKeepingTransformRuntimeData::ExceptionKeepingTransformRuntimeData(
    ThreadStatus * thread_status_,
    std::string additional_exception_message_)
    : thread_status(thread_status_)
    , additional_exception_message(std::move(additional_exception_message_))
{
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

static std::exception_ptr runStep(std::function<void()> step, ExceptionKeepingTransformRuntimeData * runtime_data)
{
    auto * original_thread = current_thread;
    SCOPE_EXIT({ current_thread = original_thread; });

    if (runtime_data && runtime_data->thread_status)
    {
        /// Change thread context to store individual metrics. Once the work in done, go back to the original thread
        runtime_data->thread_status->resetPerformanceCountersLastUsage();
        current_thread = runtime_data->thread_status;
    }

    std::exception_ptr res;
    Stopwatch watch;

    try
    {
        step();
    }
    catch (Exception & exception)
    {
        // std::cerr << "===== got exception " << getExceptionMessage(exception, false);
        if (runtime_data && !runtime_data->additional_exception_message.empty())
            exception.addMessage(runtime_data->additional_exception_message);

        res = std::current_exception();
    }
    catch (...)
    {
        // std::cerr << "===== got exception " << getExceptionMessage(std::current_exception(), false);
        res = std::current_exception();
    }

    if (runtime_data)
    {
        if (runtime_data->thread_status)
            runtime_data->thread_status->updatePerformanceCounters();

        runtime_data->elapsed_ms += watch.elapsedMilliseconds();
    }

    return res;
}

void ExceptionKeepingTransform::work()
{
    // std::cerr << "============ Executing " << getName() << std::endl;
    if (!ignore_on_start_and_finish && !was_on_start_called)
    {
        was_on_start_called = true;

        if (auto exception = runStep([this] { onStart(); }, runtime_data.get()))
        {
            has_exception = true;
            ready_output = true;
            data.exception = std::move(exception);
        }
    }
    else if (ready_input)
    {
        ready_input = false;

        if (auto exception = runStep([this] { transform(data.chunk); }, runtime_data.get()))
        {
            // std::cerr << "===== got exception in " << getName() << std::endl;
            // std::cerr << getExceptionMessage(exception, true) << std::endl;
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

        if (auto exception = runStep([this] { onFinish(); }, runtime_data.get()))
        {
            has_exception = true;
            ready_output = true;
            data.exception = std::move(exception);
        }
    }
}

}
