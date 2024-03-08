#include <Processors/Transforms/MaterializingCTETransform.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Interpreters/Set.h>
#include <Interpreters/IJoin.h>
#include <Storages/IStorage.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <exception>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int UNKNOWN_EXCEPTION;
}

MaterializingCTETransform::~MaterializingCTETransform()
{
    if (executor)
    {
        try
        {
            executor->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to cancel PushingPipelineExecutor");
        }
    }
}

MaterializingCTETransform::MaterializingCTETransform(
    Block in_header_,
    Block out_header_,
    StoragePtr external_table_,
    const String & cte_table_name_,
    SizeLimits network_transfer_limits_)
    : IAccumulatingTransform(std::move(in_header_), std::move(out_header_))
    , cte_table_name(cte_table_name_)
    , external_table(std::move(external_table_))
    , network_transfer_limits(std::move(network_transfer_limits_))
{
}

void MaterializingCTETransform::work()
{
    if (!is_initialized)
        init();

    if (done_with_table)
    {
        finishConsume();
        input.close();
    }

    IAccumulatingTransform::work();
}

void MaterializingCTETransform::startSubquery()
{
    done_with_table = !external_table;

    if (done_with_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: nothing to do with subquery");

    LOG_TRACE(log, "Filling cte table {}", cte_table_name);

    if (external_table)
        /// TODO: make via port
        table_out = QueryPipeline(external_table->write({}, external_table->getInMemoryMetadataPtr(), nullptr, /*async_insert=*/false));


    if (table_out.initialized())
    {
        executor = std::make_unique<PushingPipelineExecutor>(table_out);
        executor->start();
    }
}

void MaterializingCTETransform::finishSubquery()
{
    auto seconds = watch.elapsedNanoseconds() / 1e9;

    if (read_rows != 0)
    {
        if (external_table)
            LOG_DEBUG(log, "Created CTE table `{}` with {} rows in {} sec, actual table in temporary database is: {}", cte_table_name, read_rows, seconds, external_table->getStorageID().getNameForLogs());
    }
    else
    {
        LOG_DEBUG(log, "Subquery has empty result.");
    }
}

void MaterializingCTETransform::init()
{
    is_initialized = true;

    watch.restart();
    startSubquery();
}

void MaterializingCTETransform::consume(Chunk chunk)
{
    read_rows += chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (!done_with_table)
    {
        block = materializeBlock(block);
        executor->push(block);

        rows_to_transfer += block.rows();
        bytes_to_transfer += block.bytes();

        if (!network_transfer_limits.check(rows_to_transfer, bytes_to_transfer, "IN/JOIN external table",
                ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
            done_with_table = true;
    }

    if (done_with_table)
        finishConsume();
}

Chunk MaterializingCTETransform::generate()
{
    if (table_out.initialized())
    {
        executor->finish();
        executor.reset();
        table_out.reset();
    }

    finishSubquery();
    return {};
}

}
