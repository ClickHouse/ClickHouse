#pragma once

#include <list>
#include <queue>
#include <atomic>
#include <thread>
#include <mutex>

#include <common/logger_useful.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>


/** Allows to process multiple block input streams (sources) in parallel, using specified number of threads.
  * Reads (pulls) blocks from any available source and passes it to specified handler.
  *
  * Implemented in following way:
  * - there are multiple input sources to read blocks from;
  * - there are multiple threads, that could simultaneously read blocks from different sources;
  * - "available" sources (that are not read in any thread right now) are put in queue of sources;
  * - when thread take a source to read from, it removes source from queue of sources,
  *    then read block from source and then put source back to queue of available sources.
  */

namespace CurrentMetrics
{
    extern const Metric QueryThread;
}

namespace DB
{

/** Union mode.
  */
enum class StreamUnionMode
{
    Basic = 0, /// take out blocks
    ExtraInfo  /// take out blocks + additional information
};

/// Example of the handler.
struct ParallelInputsHandler
{
    /// Processing the data block.
    void onBlock(Block & block, size_t thread_num) {}

    /// Processing the data block + additional information.
    void onBlock(Block & block, BlockExtraInfo & extra_info, size_t thread_num) {}

    /// Called for each thread, when the thread has nothing else to do.
    /// Due to the fact that part of the sources has run out, and now there are fewer sources left than streams.
    /// Called if the `onException` method does not throw an exception; is called before the `onFinish` method.
    void onFinishThread(size_t thread_num) {}

    /// Blocks are over. Due to the fact that all sources ran out or because of the cancellation of work.
    /// This method is always called exactly once, at the end of the work, if the `onException` method does not throw an exception.
    void onFinish() {}

    /// Exception handling. It is reasonable to call the ParallelInputsProcessor::cancel method in this method, and also pass the exception to the main thread.
    void onException(std::exception_ptr & exception, size_t thread_num) {}
};


template <typename Handler, StreamUnionMode mode = StreamUnionMode::Basic>
class ParallelInputsProcessor
{
public:
    /** additional_input_at_end - if not nullptr,
      *  then the blocks from this source will start to be processed only after all other sources are processed.
      * This is done in the main thread.
      *
      * Intended for implementation of FULL and RIGHT JOIN
      * - where you must first make JOIN in parallel, while noting which keys are not found,
      *   and only after the completion of this work, create blocks of keys that are not found.
      */
    ParallelInputsProcessor(BlockInputStreams inputs_, BlockInputStreamPtr additional_input_at_end_, size_t max_threads_, Handler & handler_)
        : inputs(inputs_), additional_input_at_end(additional_input_at_end_), max_threads(std::min(inputs_.size(), max_threads_)), handler(handler_)
    {
        for (size_t i = 0; i < inputs_.size(); ++i)
            available_inputs.emplace(inputs_[i], i);
    }

    ~ParallelInputsProcessor()
    {
        try
        {
            wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// Start background threads, start work.
    void process()
    {
        active_threads = max_threads;
        threads.reserve(max_threads);
        for (size_t i = 0; i < max_threads; ++i)
            threads.emplace_back(std::bind(&ParallelInputsProcessor::thread, this, current_memory_tracker, i));
    }

    /// Ask all sources to stop earlier than they run out.
    void cancel()
    {
        finish = true;

        for (auto & input : inputs)
        {
            if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*input))
            {
                try
                {
                    child->cancel();
                }
                catch (...)
                {
                    /** If you can not ask one or more sources to stop.
                      * (for example, the connection is broken for distributed query processing)
                      * - then do not care.
                      */
                    LOG_ERROR(log, "Exception while cancelling " << child->getName());
                }
            }
        }
    }

    /// Wait until all threads are finished, before the destructor.
    void wait()
    {
        if (joined_threads)
            return;

        for (auto & thread : threads)
            thread.join();

        threads.clear();
        joined_threads = true;
    }

    size_t getNumActiveThreads() const
    {
        return active_threads;
    }

private:
    /// Single source data
    struct InputData
    {
        BlockInputStreamPtr in;
        size_t i;        /// The source number (for debugging).

        InputData() {}
        InputData(BlockInputStreamPtr & in_, size_t i_) : in(in_), i(i_) {}
    };

    template <StreamUnionMode mode2 = mode>
    void publishPayload(BlockInputStreamPtr & stream, Block & block, size_t thread_num,
        typename std::enable_if<mode2 == StreamUnionMode::Basic>::type * = nullptr)
    {
        handler.onBlock(block, thread_num);
    }

    template <StreamUnionMode mode2 = mode>
    void publishPayload(BlockInputStreamPtr & stream, Block & block, size_t thread_num,
        typename std::enable_if<mode2 == StreamUnionMode::ExtraInfo>::type * = nullptr)
    {
        BlockExtraInfo extra_info = stream->getBlockExtraInfo();
        handler.onBlock(block, extra_info, thread_num);
    }

    void thread(MemoryTracker * memory_tracker, size_t thread_num)
    {
        current_memory_tracker = memory_tracker;
        std::exception_ptr exception;

        setThreadName("ParalInputsProc");
        CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

        try
        {
            loop(thread_num);
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        if (exception)
        {
            handler.onException(exception, thread_num);
        }

        handler.onFinishThread(thread_num);

        /// The last thread on the output indicates that there is no more data.
        if (0 == --active_threads)
        {
            /// And then it processes an additional source, if there is one.
            if (additional_input_at_end)
            {
                try
                {
                    while (Block block = additional_input_at_end->read())
                        publishPayload(additional_input_at_end, block, thread_num);
                }
                catch (...)
                {
                    exception = std::current_exception();
                }

                if (exception)
                {
                    handler.onException(exception, thread_num);
                }
            }

            handler.onFinish ();       /// TODO If in `onFinish` or `onFinishThread` there is an exception, then std::terminate is called.
        }
    }

    void loop(size_t thread_num)
    {
        while (!finish)    /// You may need to stop work earlier than all sources run out.
        {
            InputData input;

            /// Select the next source.
            {
                std::lock_guard<std::mutex> lock(available_inputs_mutex);

                /// If there are no free sources, then this thread is no longer needed. (But other threads can work with their sources.)
                if (available_inputs.empty())
                    break;

                input = available_inputs.front();

                /// We remove the source from the queue of available sources.
                available_inputs.pop();
            }

            /// The main work.
            Block block = input.in->read();

            {
                if (finish)
                    break;

                /// If this source is not run out yet, then put the resulting block in the ready queue.
                {
                    std::lock_guard<std::mutex> lock(available_inputs_mutex);

                    if (block)
                    {
                        available_inputs.push(input);
                    }
                    else
                    {
                        if (available_inputs.empty())
                            break;
                    }
                }

                if (finish)
                    break;

                if (block)
                    publishPayload(input.in, block, thread_num);
            }
        }
    }

    BlockInputStreams inputs;
    BlockInputStreamPtr additional_input_at_end;
    unsigned max_threads;

    Handler & handler;

    /// Streams.
    using ThreadsData = std::vector<std::thread>;
    ThreadsData threads;

    /** A set of available sources that are not currently processed by any thread.
      * Each thread takes one source from this set, takes a block out of the source (at this moment the source does the calculations)
      *  and (if the source is not run out), puts it back into the set of available sources.
      *
      * The question arises what is better to use:
      * - the queue (just processed source will be processed the next time later than the rest)
      * - stack (just processed source will be processed as soon as possible).
      *
      * The stack is better than the queue when you need to do work on reading one source more consequentially,
      *  and theoretically, this allows you to achieve more consequent/consistent reads from the disk.
      *
      * But when using the stack, there is a problem with distributed query processing:
      *  data is read only from a part of the servers, and on the other servers
      * a timeout occurs during send, and the request processing ends with an exception.
      *
      * Therefore, a queue is used. This can be improved in the future.
      */
    using AvailableInputs = std::queue<InputData>;
    AvailableInputs available_inputs;

    /// For operations with available_inputs.
    std::mutex available_inputs_mutex;

    /// How many sources ran out.
    std::atomic<size_t> active_threads { 0 };
    /// Finish the threads work (before the sources run out).
    std::atomic<bool> finish { false };
    /// Wait for the completion of all threads.
    std::atomic<bool> joined_threads { false };

    Logger * log = &Logger::get("ParallelInputsProcessor");
};


}
