#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
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

void copyData(IBlockInputStream & from, IBlockOutputStream & to, std::atomic<bool> * is_cancelled)
{
    auto is_cancelled_pred = [is_cancelled] ()
    {
        return isAtomicSet(is_cancelled);
    };

    copyDataImpl(from, to, is_cancelled_pred, doNothing);
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
