#pragma once

#include <common/logger_useful.h>

#include <Common/ConcurrentBoundedQueue.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{

template <StreamUnionMode mode>
struct OutputData;

/// A block or an exception.
template <>
struct OutputData<StreamUnionMode::Basic>
{
    Block block;
    std::exception_ptr exception;

    OutputData() {}
    OutputData(Block & block_) : block(block_) {}
    OutputData(std::exception_ptr & exception_) : exception(exception_) {}
};

/// Block + additional information or an exception.
template <>
struct OutputData<StreamUnionMode::ExtraInfo>
{
    Block block;
    BlockExtraInfo extra_info;
    std::exception_ptr exception;

    OutputData() {}
    OutputData(Block & block_, BlockExtraInfo & extra_info_) : block(block_), extra_info(extra_info_) {}
    OutputData(std::exception_ptr & exception_) : exception(exception_) {}
};

}

/** Merges several sources into one.
  * Blocks from different sources are interleaved with each other in an arbitrary way.
  * You can specify the number of threads (max_threads),
  *  in which data will be retrieved from different sources.
  *
  * It's managed like this:
  * - with the help of ParallelInputsProcessor in several threads it takes out blocks from the sources;
  * - the completed blocks are added to a limited queue of finished blocks;
  * - the main thread takes out completed blocks from the queue of finished blocks;
  * - if the StreamUnionMode::ExtraInfo mode is specified, in addition to the UnionBlockInputStream
  *   extracts blocks information; In this case all sources should support such mode.
  */

template <StreamUnionMode mode = StreamUnionMode::Basic>
class UnionBlockInputStream final : public IProfilingBlockInputStream
{
public:
    using ExceptionCallback = std::function<void()>;

private:
    using Self = UnionBlockInputStream<mode>;

public:
    UnionBlockInputStream(BlockInputStreams inputs, BlockInputStreamPtr additional_input_at_end, size_t max_threads,
        ExceptionCallback exception_callback_ = ExceptionCallback()) :
        output_queue(std::min(inputs.size(), max_threads)),
        handler(*this),
        processor(inputs, additional_input_at_end, max_threads, handler),
        exception_callback(exception_callback_)
    {
        children = inputs;
        if (additional_input_at_end)
            children.push_back(additional_input_at_end);

        size_t num_children = children.size();
        if (num_children > 1)
        {
            Block header = children.at(0)->getHeader();
            for (size_t i = 1; i < num_children; ++i)
                assertBlocksHaveEqualStructure(children[i]->getHeader(), header, "UNION");
        }
    }

    String getName() const override { return "Union"; }

    ~UnionBlockInputStream() override
    {
        try
        {
            if (!all_read)
                cancel(false);

            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /** Different from the default implementation by trying to stop all sources,
      * skipping failed by execution.
      */
    void cancel(bool kill) override
    {
        if (kill)
            is_killed = true;

        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
            return;

        //std::cerr << "cancelling\n";
        processor.cancel(kill);
    }

    BlockExtraInfo getBlockExtraInfo() const override
    {
        return doGetBlockExtraInfo();
    }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    void finalize()
    {
        if (!started)
            return;

        LOG_TRACE(log, "Waiting for threads to finish");

        std::exception_ptr exception;
        if (!all_read)
        {
            /** Let's read everything up to the end, so that ParallelInputsProcessor is not blocked when trying to insert into the queue.
              * Maybe there is an exception in the queue.
              */
            OutputData<mode> res;
            while (true)
            {
                //std::cerr << "popping\n";
                output_queue.pop(res);

                if (res.exception)
                {
                    if (!exception)
                        exception = res.exception;
                    else if (Exception * e = exception_cast<Exception *>(exception))
                        e->addMessage("\n" + getExceptionMessage(res.exception, false));
                }
                else if (!res.block)
                    break;
            }

            all_read = true;
        }

        processor.wait();

        LOG_TRACE(log, "Waited for threads to finish");

        if (exception)
            std::rethrow_exception(exception);
    }

    /// Do nothing, to make the preparation for the query execution in parallel, in ParallelInputsProcessor.
    void readPrefix() override
    {
    }

    /** The following options are possible:
      * 1. `readImpl` function is called until it returns an empty block.
      *  Then `readSuffix` function is called and then destructor.
      * 2. `readImpl` function is called. At some point, `cancel` function is called perhaps from another thread.
      *  Then `readSuffix` function is called and then destructor.
      * 3. At any time, the object can be destroyed (destructor called).
      */

    Block readImpl() override
    {
        if (all_read)
            return received_payload.block;

        /// Run threads if this has not already been done.
        if (!started)
        {
            started = true;
            processor.process();
        }

        /// We will wait until the next block is ready or an exception is thrown.
        //std::cerr << "popping\n";
        output_queue.pop(received_payload);

        if (received_payload.exception)
        {
            if (exception_callback)
                exception_callback();
            std::rethrow_exception(received_payload.exception);
        }

        if (!received_payload.block)
            all_read = true;

        return received_payload.block;
    }

    /// Called either after everything is read, or after cancel.
    void readSuffix() override
    {
        //std::cerr << "readSuffix\n";
        if (!all_read && !isCancelled())
            throw Exception("readSuffix called before all data is read", ErrorCodes::LOGICAL_ERROR);

        finalize();

        for (size_t i = 0; i < children.size(); ++i)
            children[i]->readSuffix();
    }

private:
    BlockExtraInfo doGetBlockExtraInfo() const
    {
        if constexpr (mode == StreamUnionMode::ExtraInfo)
            return received_payload.extra_info;
        else
            throw Exception("Method getBlockExtraInfo is not supported for mode StreamUnionMode::Basic",
                ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    using Payload = OutputData<mode>;
    using OutputQueue = ConcurrentBoundedQueue<Payload>;

private:
    /** The queue of the finished blocks. Also, you can put an exception instead of a block.
      * When data is run out, an empty block is inserted into the queue.
      * Sooner or later, an empty block is always inserted into the queue (even after exception or query cancellation).
      * The queue is always (even after exception or canceling the query, even in destructor) you must read up to an empty block,
      *  otherwise ParallelInputsProcessor can be blocked during insertion into the queue.
      */
    OutputQueue output_queue;

    struct Handler
    {
        Handler(Self & parent_) : parent(parent_) {}

        void onBlock(Block & block, size_t /*thread_num*/)
        {
            parent.output_queue.push(Payload(block));
        }

        void onBlock(Block & block, BlockExtraInfo & extra_info, size_t /*thread_num*/)
        {
            parent.output_queue.push(Payload(block, extra_info));
        }

        void onFinish()
        {
            parent.output_queue.push(Payload());
        }

        void onFinishThread(size_t /*thread_num*/)
        {
        }

        void onException(std::exception_ptr & exception, size_t /*thread_num*/)
        {
            //std::cerr << "pushing exception\n";

            /// The order of the rows matters. If it is changed, then the situation is possible,
            ///  when before exception, an empty block (end of data) will be put into the queue,
            ///  and the exception is lost.

            parent.output_queue.push(exception);
            parent.cancel(false);    /// Does not throw exceptions.
        }

        Self & parent;
    };

    Handler handler;
    ParallelInputsProcessor<Handler, mode> processor;

    ExceptionCallback exception_callback;

    Payload received_payload;

    bool started = false;
    bool all_read = false;

    Logger * log = &Logger::get("UnionBlockInputStream");
};

}
