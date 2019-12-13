#include <thread>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
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


void copyDataImpl(BlockInputStreams & froms, BlockOutputStreams & tos)
{
    if (froms.size() == tos.size())
    {
        std::vector<ThreadFromGlobalPool> threads;
        threads.reserve(froms.size());
        for (size_t i = 0; i < froms.size(); i++)
        {
            threads.emplace_back([from = froms.at(i), to = tos.at(i)]()
            {
                from->readPrefix();
                to->writePrefix();
                while (Block block = from->read())
                    to->write(block);
                from->readSuffix();
                to->writeSuffix();
            });
        }
        for (auto & thread : threads)
            thread.join();
    }
    else
    {
        ConcurrentBoundedQueue<Block> queue(froms.size());
        ThreadFromGlobalPool from_threads([&]()
        {
            std::vector<ThreadFromGlobalPool> from_threads_;
            from_threads_.reserve(froms.size());
            for (auto & from : froms)
            {
                from_threads_.emplace_back([&queue, from]()
                {
                    from->readPrefix();
                    while (Block block = from->read())
                        queue.push(block);
                    from->readSuffix();
                });
            }
            for (auto & thread : from_threads_)
                thread.join();
            for (size_t i = 0; i < tos.size(); i++)
                queue.push({});
        });
        std::vector<ThreadFromGlobalPool> to_threads;
        for (auto & to : tos)
        {
            to_threads.emplace_back([&queue, to]()
            {
                to->writePrefix();
                Block block;
                while (true)
                {
                    queue.pop(block);
                    if (!block)
                        break;
                    to->write(block);
                }
                to->writeSuffix();
            });
        }

        from_threads.join();
        for (auto & thread : to_threads)
            thread.join();
    }
}

void copyData(IBlockInputStream & from, IBlockOutputStream & to, std::atomic<bool> * is_cancelled)
{
    auto is_cancelled_pred = [is_cancelled] ()
    {
        return isAtomicSet(is_cancelled);
    };

    copyDataImpl(from, to, is_cancelled_pred, doNothing);
}

void copyData(BlockInputStreams & froms, BlockOutputStreams & tos)
{
    copyDataImpl(froms, tos);
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
