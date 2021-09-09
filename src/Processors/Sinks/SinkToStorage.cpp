#include <Processors/Sinks/SinkToStorage.h>
#include <Common/ThreadStatus.h>
#include <Common/Stopwatch.h>
#include <common/scope_guard.h>
#include <DataTypes/NestedUtils.h>
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

ExceptionKeepingTransform::ExceptionKeepingTransform(const Block & in_header, const Block & out_header)
    : IProcessor({in_header}, {out_header})
    , input(inputs.front()), output(outputs.front())
{
}

IProcessor::Status ExceptionKeepingTransform::prepare()
{
    if (!was_on_start_called)
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
            if (!was_on_finish_called && !has_exception)
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
    if (!was_on_start_called)
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
    else if (!was_on_finish_called)
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

SinkToStorage::SinkToStorage(const Block & header) : ExceptionKeepingTransform(header, header) {}

void SinkToStorage::transform(Chunk & chunk)
{
    /** Throw an exception if the sizes of arrays - elements of nested data structures doesn't match.
      * We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
      * NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
      * but currently we don't have methods for serialization of nested structures "as a whole".
      */
    Nested::validateArraySizes(getHeader().cloneWithColumns(chunk.getColumns()));

    consume(chunk.clone());
    if (lastBlockIsDuplicate())
        chunk.clear();
}

}
