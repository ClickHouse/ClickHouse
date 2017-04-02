#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>


namespace DB
{

namespace
{

bool isAtomicSet(std::atomic<bool> * val)
{
    return ((val != nullptr) && val->load(std::memory_order_seq_cst));
}

}

void copyData(IBlockInputStream & from, IBlockOutputStream & to, std::atomic<bool> * is_cancelled)
{
    from.readPrefix();
    to.writePrefix();

    while (Block block = from.read())
    {
        if (isAtomicSet(is_cancelled))
            break;

        to.write(block);
    }

    if (isAtomicSet(is_cancelled))
        return;

    /// For outputting additional information in some formats.
    if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(&from))
    {
        if (input->getProfileInfo().hasAppliedLimit())
            to.setRowsBeforeLimit(input->getProfileInfo().getRowsBeforeLimit());

        to.setTotals(input->getTotals());
        to.setExtremes(input->getExtremes());
    }

    if (isAtomicSet(is_cancelled))
        return;

    from.readSuffix();
    to.writeSuffix();
}

}
