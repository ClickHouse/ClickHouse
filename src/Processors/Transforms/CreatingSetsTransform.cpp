#include <Processors/Transforms/CreatingSetsTransform.h>

#include <DataStreams/BlockStreamProfileInfo.h>
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
    Block out_header_,
    SubqueriesForSets subqueries_for_sets_,
    SizeLimits network_transfer_limits_,
    const Context & context_)
    : IProcessor({}, {std::move(out_header_)})
    , subqueries_for_sets(std::move(subqueries_for_sets_))
    , cur_subquery(subqueries_for_sets.begin())
    , network_transfer_limits(std::move(network_transfer_limits_))
    , context(context_)
{
}

IProcessor::Status CreatingSetsTransform::prepare()
{
    auto & output = outputs.front();

    if (finished)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    return Status::Ready;
}

void CreatingSetsTransform::startSubquery(SubqueryForSet & subquery)
{
    if (subquery.set)
        LOG_TRACE(log, "Creating set.");
    if (subquery.join)
        LOG_TRACE(log, "Creating join.");
    if (subquery.table)
        LOG_TRACE(log, "Filling temporary table.");

    elapsed_nanoseconds = 0;

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

void CreatingSetsTransform::finishSubquery(SubqueryForSet & subquery)
{
    size_t head_rows = 0;
    const BlockStreamProfileInfo & profile_info = subquery.source->getProfileInfo();

    head_rows = profile_info.rows;

    subquery.setTotals();

    if (head_rows != 0)
    {
        auto seconds = elapsed_nanoseconds / 1e9;

        if (subquery.set)
            LOG_DEBUG(log, "Created Set with {} entries from {} rows in {} sec.", subquery.set->getTotalRowCount(), head_rows, seconds);
        if (subquery.join)
            LOG_DEBUG(log, "Created Join with {} entries from {} rows in {} sec.", subquery.join->getTotalRowCount(), head_rows, seconds);
        if (subquery.table)
            LOG_DEBUG(log, "Created Table with {} rows in {} sec.", head_rows, seconds);
    }
    else
    {
        LOG_DEBUG(log, "Subquery has empty result.");
    }
}

void CreatingSetsTransform::init()
{
    is_initialized = true;

    for (auto & elem : subqueries_for_sets)
        if (elem.second.source && elem.second.set)
            elem.second.set->setHeader(elem.second.source->getHeader());
}

void CreatingSetsTransform::work()
{
    if (!is_initialized)
        init();

    Stopwatch watch;

    while (cur_subquery != subqueries_for_sets.end() && cur_subquery->second.source == nullptr)
        ++cur_subquery;

    if (cur_subquery == subqueries_for_sets.end())
    {
        finished = true;
        return;
    }

    SubqueryForSet & subquery = cur_subquery->second;

    if (!started_cur_subquery)
    {
        startSubquery(subquery);
        started_cur_subquery = true;
    }

    auto finish_current_subquery = [&]()
    {
        if (subquery.set)
            subquery.set->finishInsert();

        if (table_out)
            table_out->writeSuffix();

        watch.stop();
        elapsed_nanoseconds += watch.elapsedNanoseconds();

        finishSubquery(subquery);

        ++cur_subquery;
        started_cur_subquery = false;

        while (cur_subquery != subqueries_for_sets.end() && cur_subquery->second.source == nullptr)
            ++cur_subquery;

        if (cur_subquery == subqueries_for_sets.end())
            finished = true;
    };

    auto block = subquery.source->read();
    if (!block)
    {
        finish_current_subquery();
        return;
    }

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
    {
        subquery.source->cancel(false);
        finish_current_subquery();
    }
    else
        elapsed_nanoseconds += watch.elapsedNanoseconds();
}

void CreatingSetsTransform::setProgressCallback(const ProgressCallback & callback)
{
    for (auto & elem : subqueries_for_sets)
        if (elem.second.source)
            elem.second.source->setProgressCallback(callback);
}

void CreatingSetsTransform::setProcessListElement(QueryStatus * status)
{
    for (auto & elem : subqueries_for_sets)
        if (elem.second.source)
            elem.second.source->setProcessListElement(status);
}

}
