#include <Processors/Transforms/MaterializingCTETransform.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Interpreters/Set.h>
#include <Interpreters/IJoin.h>
#include <Storages/IStorage.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "Interpreters/MaterializedTableFromCTE.h"
#include <Interpreters/Context_fwd.h>

#include <exception>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

MaterializingCTETransform::~MaterializingCTETransform()
{
    if (executor)
    {
        try
        {
            future_table->setExceptionWhileMaterializing(
                std::make_exception_ptr(Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while materializing CTE table")));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to set exception for CTE table");
        }

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
    ContextPtr context_,
    Block in_header_,
    Block out_header_,
    FutureTableFromCTEPtr future_table_,
    SizeLimits network_transfer_limits_)
    : IAccumulatingTransform(std::move(in_header_), std::move(out_header_))
    , WithContext(context_)
    , future_table(std::move(future_table_))
    , external_table(future_table->external_table)
    , table_name(future_table->name)
    , network_transfer_limits(std::move(network_transfer_limits_))
{
}

void MaterializingCTETransform::work()
{
    if (!is_initialized)
        init();

    IAccumulatingTransform::work();
}

void MaterializingCTETransform::startSubquery()
{
    if (!external_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "External table fof CTE cannot be NULL.");

    LOG_TRACE(log, "Materializing CTE table {}", table_name);

    table_out = QueryPipeline(external_table->write({}, external_table->getInMemoryMetadataPtr(), getContext(), /*async_insert=*/false));

    if (!table_out.initialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query pipeline to materialize table {} is empty", table_name);

    executor = std::make_unique<PushingPipelineExecutor>(table_out);
    executor->start();
}

void MaterializingCTETransform::finishSubquery()
{
    auto seconds = watch.elapsedNanoseconds() / 1e9;

    if (read_rows != 0)
        LOG_DEBUG(log, "Created CTE table `{}` with {} rows in {} sec, actual table in temporary database is: {}", table_name, read_rows, seconds, external_table->getStorageID().getNameForLogs());
    else
        LOG_DEBUG(log, "Subquery has empty result.");
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

    block = materializeBlock(block);
    executor->push(block);

    rows_to_transfer += block.rows();
    bytes_to_transfer += block.bytes();

    if (!network_transfer_limits.check(rows_to_transfer, bytes_to_transfer, "IN/JOIN external table",
            ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
    {
        consume_all_block = false;
        finishConsume();
    }
}

Chunk MaterializingCTETransform::generate()
{
    if (consume_all_block)
        future_table->setFullyMaterialized();
    else
        future_table->setPartiallyMaterialized();

    executor->finish();
    executor.reset();
    table_out.reset();

    finishSubquery();
    return {};
}

}
