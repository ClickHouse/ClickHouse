#include <Processors/Transforms/CreatingSetsTransform.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Interpreters/Set.h>
#include <Interpreters/IJoin.h>
#include <Storages/IStorage.h>

#include <iomanip>
#include <DataStreams/materializeBlock.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}


CreatingSetsTransform::CreatingSetsTransform(
    Block in_header_,
    Block out_header_,
    SubqueryForSet subquery_for_set_,
    SizeLimits network_transfer_limits_,
    const Context & context_)
    : IAccumulatingTransform(std::move(in_header_), std::move(out_header_))
    , subquery(std::move(subquery_for_set_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , context(context_)
{
}

void CreatingSetsTransform::addTotalsPort()
{
    if (inputs.size() > 1)
        throw Exception("Totals port was already added to CreatingSetsTransform", ErrorCodes::LOGICAL_ERROR);

    inputs.emplace_back(getInputPort().getHeader(), this);
}

IProcessor::Status CreatingSetsTransform::prepare()
{
    auto status = IAccumulatingTransform::prepare();
    if (status == IProcessor::Status::Finished && inputs.size() > 1)
    {
        auto & totals_input = inputs.back();
        if (totals_input.isFinished())
            return IProcessor::Status::Finished;

        totals_input.setNeeded();
        if (!totals_input.hasData())
            return IProcessor::Status::NeedData;

        auto totals = totals_input.pull();
        subquery.setTotals(getInputPort().getHeader().cloneWithColumns(totals.detachColumns()));
        totals_input.close();
    }

    return status;
}

void CreatingSetsTransform::work()
{
    if (!is_initialized)
        init();

    IAccumulatingTransform::work();
}

void CreatingSetsTransform::startSubquery()
{
    if (subquery.set)
        LOG_TRACE(log, "Creating set.");
    if (subquery.join)
        LOG_TRACE(log, "Creating join.");
    if (subquery.table)
        LOG_TRACE(log, "Filling temporary table.");

    if (subquery.table)
        table_out = subquery.table->write({}, subquery.table->getInMemoryMetadataPtr(), context);

    done_with_set = !subquery.set;
    done_with_join = !subquery.join;
    done_with_table = !subquery.table;

    if (done_with_set && done_with_join && done_with_table)
        throw Exception("Logical error: nothing to do with subquery", ErrorCodes::LOGICAL_ERROR);

    if (table_out)
        table_out->writePrefix();
}

void CreatingSetsTransform::finishSubquery()
{
    if (read_rows != 0)
    {
        auto seconds = watch.elapsedNanoseconds() / 1e9;

        if (subquery.set)
            LOG_DEBUG(log, "Created Set with {} entries from {} rows in {} sec.", subquery.set->getTotalRowCount(), read_rows, seconds);
        if (subquery.join)
            LOG_DEBUG(log, "Created Join with {} entries from {} rows in {} sec.", subquery.join->getTotalRowCount(), read_rows, seconds);
        if (subquery.table)
            LOG_DEBUG(log, "Created Table with {} rows in {} sec.", read_rows, seconds);
    }
    else
    {
        LOG_DEBUG(log, "Subquery has empty result.");
    }
}

void CreatingSetsTransform::init()
{
    is_initialized = true;

    if (subquery.set)
        subquery.set->setHeader(getInputPort().getHeader());

    watch.restart();
    startSubquery();
}

void CreatingSetsTransform::consume(Chunk chunk)
{
    read_rows += chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (!done_with_set)
    {
        if (!subquery.set->insertFromBlock(block))
            done_with_set = true;
    }

    if (!done_with_join)
    {
        if (!subquery.insertJoinedBlock(block))
            done_with_join = true;
    }

    if (!done_with_table)
    {
        block = materializeBlock(block);
        table_out->write(block);

        rows_to_transfer += block.rows();
        bytes_to_transfer += block.bytes();

        if (!network_transfer_limits.check(rows_to_transfer, bytes_to_transfer, "IN/JOIN external table",
                ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
            done_with_table = true;
    }

    if (done_with_set && done_with_join && done_with_table)
        finishConsume();
}

Chunk CreatingSetsTransform::generate()
{
    if (subquery.set)
        subquery.set->finishInsert();

    if (table_out)
        table_out->writeSuffix();

    finishSubquery();
    finished = true;
    return {};
}

}
