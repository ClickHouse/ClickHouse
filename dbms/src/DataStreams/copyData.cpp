#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>
#include <iomanip>


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

    size_t num_blocks = 0;
    double total_blocks_time = 0;
    size_t slowest_block_number = 0;
    double slowest_block_time = 0;

    while (true)
    {
        Stopwatch watch;
        Block block = from.read();
        double elapsed = watch.elapsedSeconds();

        if (num_blocks == 0 || elapsed > slowest_block_time)
        {
            slowest_block_number = num_blocks;
            slowest_block_time = elapsed;
        }

        total_blocks_time += elapsed;
        ++num_blocks;

        if (!block)
            break;

        if (is_cancelled())
            break;

        to.write(block);
        progress(block);
    }

    if (is_cancelled())
        return;

    /// For outputting additional information in some formats.
    if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(&from))
    {
        if (input->getProfileInfo().hasAppliedLimit())
            to.setRowsBeforeLimit(input->getProfileInfo().getRowsBeforeLimit());

        to.setTotals(input->getTotals());
        to.setExtremes(input->getExtremes());
    }

    if (is_cancelled())
        return;

    auto log = &Poco::Logger::get("copyData");
    bool print_dbg = num_blocks > 2;

    if (print_dbg)
    {
        LOG_DEBUG(log, "Read " << num_blocks << " blocks. It took " << std::fixed << total_blocks_time << " seconds, average "
                               << std::fixed << total_blocks_time / num_blocks * 1000 << " ms, the slowest block #" << slowest_block_number
                               << " was read for " << std::fixed << slowest_block_time * 1000 << " ms ");
    }

    {
        Stopwatch watch;
        to.writeSuffix();
        if (num_blocks > 1)
            LOG_DEBUG(log, "It took " << std::fixed << watch.elapsedSeconds() << " for writeSuffix()");
    }
    {
        Stopwatch watch;
        from.readSuffix();
        if (num_blocks > 1)
            LOG_DEBUG(log, "It took " << std::fixed << watch.elapsedSeconds() << " seconds for readSuffix()");
    }
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
