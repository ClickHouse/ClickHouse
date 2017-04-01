#include <Interpreters/Set.h>
#include <Interpreters/Join.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Storages/IStorage.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}


Block CreatingSetsBlockInputStream::readImpl()
{
    Block res;

    createAll();

    if (isCancelled())
        return res;

    return children.back()->read();
}


void CreatingSetsBlockInputStream::readPrefixImpl()
{
    createAll();
}


const Block & CreatingSetsBlockInputStream::getTotals()
{
    auto input = dynamic_cast<IProfilingBlockInputStream *>(children.back().get());

    if (input)
        return input->getTotals();
    else
        return totals;
}


void CreatingSetsBlockInputStream::createAll()
{
    if (!created)
    {
        for (auto & elem : subqueries_for_sets)
        {
            if (elem.second.source) /// There could be prepared in advance Set/Join - no source is specified for them.
            {
                if (isCancelled())
                    return;

                createOne(elem.second);
            }
        }

        created = true;
    }
}


void CreatingSetsBlockInputStream::createOne(SubqueryForSet & subquery)
{
    LOG_TRACE(log, (subquery.set ? "Creating set. " : "")
        << (subquery.join ? "Creating join. " : "")
        << (subquery.table ? "Filling temporary table. " : ""));
    Stopwatch watch;

    BlockOutputStreamPtr table_out;
    if (subquery.table)
        table_out = subquery.table->write({}, {});

    bool done_with_set = !subquery.set;
    bool done_with_join = !subquery.join;
    bool done_with_table = !subquery.table;

    if (done_with_set && done_with_join && done_with_table)
        throw Exception("Logical error: nothing to do with subquery", ErrorCodes::LOGICAL_ERROR);

    subquery.source->readPrefix();
    if (table_out)
        table_out->writePrefix();

    while (Block block = subquery.source->read())
    {
        if (isCancelled())
        {
            LOG_DEBUG(log, "Query was cancelled during set / join or temporary table creation.");
            return;
        }

        if (!done_with_set)
        {
            if (!subquery.set->insertFromBlock(block))
                done_with_set = true;
        }

        if (!done_with_join)
        {
            if (!subquery.join->insertFromBlock(block))
                done_with_join = true;
        }

        if (!done_with_table)
        {
            table_out->write(block);

            rows_to_transfer += block.rows();
            bytes_to_transfer += block.bytes();

            if ((max_rows_to_transfer && rows_to_transfer > max_rows_to_transfer)
                || (max_bytes_to_transfer && bytes_to_transfer > max_bytes_to_transfer))
            {
                if (transfer_overflow_mode == OverflowMode::THROW)
                    throw Exception("IN/JOIN external table size limit exceeded."
                        " Rows: " + toString(rows_to_transfer)
                        + ", limit: " + toString(max_rows_to_transfer)
                        + ". Bytes: " + toString(bytes_to_transfer)
                        + ", limit: " + toString(max_bytes_to_transfer) + ".",
                        ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

                if (transfer_overflow_mode == OverflowMode::BREAK)
                    done_with_table = true;

                throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (done_with_set && done_with_join && done_with_table)
        {
            if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
                profiling_in->cancel();

            break;
        }
    }

// subquery.source->readSuffix();  /// TODO Blocked in `RemoteBlockInputStream::readSuffixImpl` when querying `SELECT number FROM system.numbers WHERE number IN (SELECT number FROM remote(`127.0.0.{1,2}', system, numbers) WHERE number % 2 = 1 LIMIT 10) LIMIT 10`
    if (table_out)
        table_out->writeSuffix();

    /// We will display information about how many rows and bytes are read.
    size_t rows = 0;
    size_t bytes = 0;

    watch.stop();

    subquery.source->getLeafRowsBytes(rows, bytes);

    size_t head_rows = 0;
    if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
    {
        head_rows = profiling_in->getProfileInfo().rows;

        if (subquery.join)
            subquery.join->setTotals(profiling_in->getTotals());
    }

    if (rows != 0)
    {
        std::stringstream msg;
        msg << std::fixed << std::setprecision(3);
        msg << "Created. ";

        if (subquery.set)
            msg << "Set with " << subquery.set->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.join)
            msg << "Join with " << subquery.join->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.table)
            msg << "Table with " << head_rows << " rows. ";

        msg << "Read " << rows << " rows, " << bytes / 1048576.0 << " MiB in " << watch.elapsedSeconds() << " sec., "
            << static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.";

        LOG_DEBUG(log, msg.rdbuf());
    }
    else
    {
        LOG_DEBUG(log, "Subquery has empty result.");
    }
}

}
