#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Interpreters/Set.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
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

namespace FailPoints
{
    extern const char prepared_sets_build_ordered_set_inplace_fail[];
}

CreatingSetsTransform::~CreatingSetsTransform()
{
    if (promise_to_build)
    {
        /// set_exception can also throw
        try
        {
            promise_to_build->set_exception(std::make_exception_ptr(
                Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to build set, most likely pipeline executor was stopped")));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to set_exception for promise");
        }
    }

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

CreatingSetsTransform::CreatingSetsTransform(
    SharedHeader in_header_,
    SharedHeader out_header_,
    SetAndKeyPtr set_and_key_,
    SizeLimits network_transfer_limits_,
    PreparedSetsCachePtr prepared_sets_cache_)
    : IAccumulatingTransform(std::move(in_header_), std::move(out_header_))
    , set_and_key(std::move(set_and_key_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , prepared_sets_cache(std::move(prepared_sets_cache_))
{
}

void CreatingSetsTransform::work()
{
    try
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
    catch (...)
    {
        if (promise_to_build)
        {
            /// set_exception can also throw
            try
            {
                promise_to_build->set_exception(std::current_exception());
                promise_to_build.reset();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to set_exception for promise");
            }
        }
        throw;
    }
}

void CreatingSetsTransform::startSubquery()
{
    /// Lookup the set in the cache if we don't need to build table.
    if (prepared_sets_cache && !set_and_key->external_table)
    {
        /// Try to find the set in the cache and wait for it to be built.
        /// Retry if the set from cache fails to be built.
        while (true)
        {
            try
            {
                auto from_cache = prepared_sets_cache->findOrPromiseToBuild(set_and_key->key);
                if (from_cache.index() == 0)
                {
                    LOG_TRACE(log, "Building set, key: {}", set_and_key->key);
                    promise_to_build = std::move(std::get<0>(from_cache));
                }
                else
                {
                    LOG_TRACE(log, "Waiting for set to be built by another thread, key: {}", set_and_key->key);
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
            /// Exception that is thrown by the shared_future::get() is shared across all waiters and cannot be modified from multiple threads.
            /// Re-create exception to allow later concurrent modify (i.e. addMessage() during pipeline execution)
            ///
            /// Note, that findOrPromiseToBuild() can also call shared_future::get()
            catch (const Exception & e)
            {
                throw Exception(e);
            }
            catch (...)
            {
                throw Exception::createRuntime(ErrorCodes::UNKNOWN_EXCEPTION, getExceptionMessage(std::current_exception(), /* with_stacktrace= */ false));
            }
        }
    }

    if (set_and_key->set && !set_from_cache)
        LOG_TRACE(log, "Creating set, key: {}", set_and_key->key);
    if (set_and_key->external_table)
        LOG_TRACE(log, "Filling temporary table.");

    if (set_and_key->external_table)
    {
        /// TODO: make via port
        const auto metadata_snapshot = set_and_key->external_table->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
        table_out = QueryPipeline(set_and_key->external_table->write({}, metadata_snapshot, nullptr, /*async_insert=*/false));
    }

    done_with_set = !set_and_key->set || set_from_cache;
    done_with_table = !set_and_key->external_table;

    if ((done_with_set && !set_from_cache) && done_with_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nothing to do with subquery");

    if (table_out.initialized())
    {
        executor = std::make_unique<PushingPipelineExecutor>(table_out);
        executor->start();
    }
}

void CreatingSetsTransform::finishSubquery()
{
    auto seconds = static_cast<double>(watch.elapsedNanoseconds()) / 1e9;

    if (set_from_cache)
    {
        LOG_DEBUG(log, "Got set from cache in {:.3f} sec.", seconds);
    }
    else if (read_rows != 0)
    {
        if (set_and_key->set)
            LOG_DEBUG(log, "Created Set with {} entries from {} rows in {:.3f} sec.", set_and_key->set->getTotalRowCount(), read_rows, seconds);
        if (set_and_key->external_table)
            LOG_DEBUG(log, "Created Table with {} rows in {:.3f} sec.", read_rows, seconds);
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
        /// Simulate a silent in-place build failure: skip `finishInsert`, leaving the set not created
        /// (the same observable state as a subquery timeout with `overflow_mode = 'break'`). Fires once,
        /// so the in-place build during primary key analysis fails while the deferred build succeeds.
        fiu_do_on(FailPoints::prepared_sets_build_ordered_set_inplace_fail,
        {
            finishSubquery();
            return {};
        });

        set_and_key->set->finishInsert();
        if (promise_to_build)
        {
            promise_to_build->set_value(set_and_key->set);
            promise_to_build.reset();
        }
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
