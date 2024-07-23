#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Interpreters/Set.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include <Common/logger_useful.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

CreatingSetsTransform::~CreatingSetsTransform() = default;

CreatingSetsTransform::CreatingSetsTransform(
    Block in_header_,
    Block out_header_,
    SetAndKeyPtr set_and_key_,
    StoragePtr external_table_,
    SizeLimits network_transfer_limits_,
    PreparedSetsCachePtr prepared_sets_cache_)
    : IAccumulatingTransform(std::move(in_header_), std::move(out_header_))
    , set_and_key(std::move(set_and_key_))
    , external_table(std::move(external_table_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , prepared_sets_cache(std::move(prepared_sets_cache_))
{
}

void CreatingSetsTransform::work()
{
    if (!is_initialized)
        init();

    if (done_with_set && done_with_table)
    {
        finishConsume();
        input.close();
    }

    IAccumulatingTransform::work();
}

void CreatingSetsTransform::startSubquery()
{
    /// Lookup the set in the cache if we don't need to build table.
    if (prepared_sets_cache && !external_table)
    {
        /// Try to find the set in the cache and wait for it to be built.
        /// Retry if the set from cache fails to be built.
        while (true)
        {
            auto from_cache = prepared_sets_cache->findOrPromiseToBuild(set_and_key->key);
            if (from_cache.index() == 0)
            {
                LOG_TRACE(log, "Building set, key: {}", set_and_key->key);
                promise_to_build = std::move(std::get<0>(from_cache));
            }
            else
            {
                LOG_TRACE(log, "Waiting for set to be build by another thread, key: {}", set_and_key->key);
                SharedSet set_built_by_another_thread = std::move(std::get<1>(from_cache));
                const SetPtr & ready_set = set_built_by_another_thread.get();
                if (!ready_set)
                {
                    LOG_TRACE(log, "Failed to use set from cache, key: {}", set_and_key->key);
                    continue;
                }

                set_and_key->set = ready_set;
                done_with_set = true;
                set_from_cache = true;
            }
            break;
        }
    }

    if (set_and_key->set && !set_from_cache)
        LOG_TRACE(log, "Creating set, key: {}", set_and_key->key);
    if (external_table)
        LOG_TRACE(log, "Filling temporary table.");

    if (external_table)
        /// TODO: make via port
        table_out = QueryPipeline(external_table->write({}, external_table->getInMemoryMetadataPtr(), nullptr, /*async_insert=*/false));

    done_with_set = !set_and_key->set || set_from_cache;
    done_with_table = !external_table;

    if ((done_with_set && !set_from_cache) && done_with_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: nothing to do with subquery");

    if (table_out.initialized())
    {
        executor = std::make_unique<PushingPipelineExecutor>(table_out);
        executor->start();
    }
}

void CreatingSetsTransform::finishSubquery()
{
    auto seconds = watch.elapsedNanoseconds() / 1e9;

    if (set_from_cache)
    {
        LOG_DEBUG(log, "Got set from cache in {} sec.", seconds);
    }
    else if (read_rows != 0)
    {
        if (set_and_key->set)
            LOG_DEBUG(log, "Created Set with {} entries from {} rows in {} sec.", set_and_key->set->getTotalRowCount(), read_rows, seconds);
        if (external_table)
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

    watch.restart();
    startSubquery();
}

void CreatingSetsTransform::consume(Chunk chunk)
{
    read_rows += chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (!done_with_set)
    {
        if (!set_and_key->set->insertFromBlock(block.getColumnsWithTypeAndName()))
            done_with_set = true;
    }

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

    if (done_with_set && done_with_table)
        finishConsume();
}

Chunk CreatingSetsTransform::generate()
{
    if (set_and_key->set && !set_from_cache)
    {
        set_and_key->set->finishInsert();
        if (promise_to_build)
            promise_to_build->set_value(set_and_key->set);
    }

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
