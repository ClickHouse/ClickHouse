#include <thread>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Common/ConcurrentBoundedQueue.h>


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
        std::vector<std::thread> threads(froms.size());
        for (size_t i = 0; i < froms.size(); i++)
        {
            threads[i] = std::thread(
                [&](BlockInputStreamPtr from, BlockOutputStreamPtr to) {
                    from->readPrefix();
                    to->writePrefix();
                    while (Block block = from->read())
                    {
                        to->write(block);
                    }
                    from->readSuffix();
                    to->writeSuffix();
                }, froms.at(i), tos.at(i));
        }
        for (auto & thread : threads)
            thread.join();
    }
    else
    {
        ConcurrentBoundedQueue<Block> queue(froms.size());
        std::thread from_threads([&]() {
            std::vector<std::thread> _from_threads;
            for (auto & _from : froms)
            {
                _from_threads.emplace_back([&](BlockInputStreamPtr from) {
                    from->readPrefix();
                    while (Block block = from->read())
                    {
                        queue.push(block);
                    }
                    from->readSuffix();
                }, _from);
            }
            for (auto & thread : _from_threads)
                thread.join();
            for (size_t i = 0; i < tos.size(); i++)
                queue.push({});
        });
        std::vector<std::thread> _to_threads;
        for (auto & _to : tos)
        {
            _to_threads.emplace_back([&](BlockOutputStreamPtr to) {
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
            }, _to);
        }

        from_threads.join();
        for (auto & thread : _to_threads)
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
