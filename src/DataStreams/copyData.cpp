#include <thread>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/ParallelInputsProcessor.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>


namespace DB
{

namespace
{

bool isAtomicSet(std::atomic<bool> * val)
{
    return ((val != nullptr) && val->load(std::memory_order_seq_cst));
}

}

template <typename TCancelCallback, typename TProgressCallback>
void copyDataImpl(IBlockInputStream & from, IBlockOutputStream & to, TCancelCallback && is_cancelled, TProgressCallback && progress)
{
    from.readPrefix();
    to.writePrefix();

    while (Block block = from.read())
    {
        if (is_cancelled())
            break;

        to.write(block);
        progress(block);
    }

    if (is_cancelled())
        return;

    /// For outputting additional information in some formats.
    if (from.getProfileInfo().hasAppliedLimit())
        to.setRowsBeforeLimit(from.getProfileInfo().getRowsBeforeLimit());

    to.setTotals(from.getTotals());
    to.setExtremes(from.getExtremes());

    if (is_cancelled())
        return;

    from.readSuffix();
    to.writeSuffix();
}


inline void doNothing(const Block &) {}

namespace
{


struct ParallelInsertsHandler
{
    using CencellationHook = std::function<void()>;

    explicit ParallelInsertsHandler(BlockOutputStreams & output_streams, CencellationHook cancellation_hook_, size_t num_threads)
        : outputs(output_streams.size()), cancellation_hook(std::move(cancellation_hook_))
    {
        exceptions.resize(num_threads);

        for (auto & output : output_streams)
            outputs.push(output.get());
    }

    void onBlock(Block & block, size_t /*thread_num*/)
    {
        IBlockOutputStream * out = nullptr;

        outputs.pop(out);
        out->write(block);
        outputs.push(out);
    }

    void onFinishThread(size_t /*thread_num*/) {}
    void onFinish() {}

    void onException(std::exception_ptr & exception, size_t thread_num)
    {
        exceptions[thread_num] = exception;
        cancellation_hook();
    }

    void rethrowFirstException()
    {
        for (auto & exception : exceptions)
            if (exception)
                std::rethrow_exception(exception);
    }

    ConcurrentBoundedQueue<IBlockOutputStream *> outputs;
    std::vector<std::exception_ptr> exceptions;
    CencellationHook cancellation_hook;
};

}

static void copyDataImpl(BlockInputStreams & inputs, BlockOutputStreams & outputs)
{
    for (auto & output : outputs)
        output->writePrefix();

    using Processor = ParallelInputsProcessor<ParallelInsertsHandler>;
    Processor * processor_ptr = nullptr;

    ParallelInsertsHandler handler(outputs, [&processor_ptr]() { processor_ptr->cancel(false); }, inputs.size());
    ParallelInputsProcessor<ParallelInsertsHandler> processor(inputs, nullptr, inputs.size(), handler);
    processor_ptr = &processor;

    processor.process();
    processor.wait();
    handler.rethrowFirstException();

    /// readPrefix is called in ParallelInputsProcessor.
    for (auto & input : inputs)
        input->readSuffix();

    for (auto & output : outputs)
        output->writeSuffix();
}

void copyData(IBlockInputStream & from, IBlockOutputStream & to, std::atomic<bool> * is_cancelled)
{
    auto is_cancelled_pred = [is_cancelled] ()
    {
        return isAtomicSet(is_cancelled);
    };

    copyDataImpl(from, to, is_cancelled_pred, doNothing);
}

void copyData(BlockInputStreams & inputs, BlockOutputStreams & outputs)
{
    copyDataImpl(inputs, outputs);
}

void copyData(IBlockInputStream & from, IBlockOutputStream & to, const std::function<bool()> & is_cancelled)
{
    copyDataImpl(from, to, is_cancelled, doNothing);
}

void copyData(IBlockInputStream & from, IBlockOutputStream & to, const std::function<bool()> & is_cancelled,
              const std::function<void(const Block & block)> & progress)
{
    copyDataImpl(from, to, is_cancelled, progress);
}

}
