#include <Processors/Transforms/MaterializingCTETransform.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/Logger.h>
#include <Processors/Port.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int QUERY_WAS_CANCELLED;

}

MaterializingCTETransform::MaterializingCTETransform(
    const SharedHeader & input_header_,
    const SharedHeader & output_header_,
    MaterializedCTEPtr materialized_cte_
)
    : IAccumulatingTransform(input_header_, output_header_)
    , materialized_cte(std::move(materialized_cte_))
    , is_initialized(false)
{
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
            tryLogCurrentException(getLogger("MaterializingCTETransform"), "Failed to cancel PushingPipelineExecutor");
        }
    }

    /// Fallback for the rare path where the transform is destroyed without
    /// either `generate()` setting the promise's value or `onCancel()` setting
    /// its exception (e.g. an exception during pipeline construction before
    /// any worker thread runs this transform). Readers blocked in
    /// `MemorySource::generate` waiting on `build_future.get()` must always
    /// unblock — failing to fulfil the promise here would hang the query.
    if (!materialized_cte->isBuilt())
    {
        try
        {
            materialized_cte->build_promise.set_exception(std::make_exception_ptr(
                Exception(ErrorCodes::LOGICAL_ERROR,
                          "Materialization of CTE '{}' was aborted before completion",
                          materialized_cte->cte_name)));
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Promise was already fulfilled between the wait_for check and
            /// this set_exception; this is benign — some other path won the
            /// race to signal completion. Suppress the future_error.
            tryLogCurrentException(getLogger("MaterializingCTETransform"), "Failed to set_exception for promise");
        }
    }
}

void MaterializingCTETransform::work()
{
    /// Single exception-handling boundary for the transform. Any exception
    /// from `init()` (storage write setup, executor start), `consume()`
    /// (push failure), or `generate()` (executor finish failure) is funnelled
    /// into `build_promise.set_exception(...)` so that readers blocked in
    /// `MemorySource::generate` on `build_future.get()` see the failure and
    /// unwind cleanly - identical structure to `CreatingSetsTransform::work`.
    try
    {
        if (!is_initialized)
            init();

        IAccumulatingTransform::work();
    }
    catch (...)
    {
        try
        {
            materialized_cte->build_promise.set_exception(std::current_exception());
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Already fulfilled (e.g. `onCancel` won the race); readers have
            /// already observed an outcome - swallow the `future_error`.
            tryLogCurrentException(getLogger("MaterializingCTETransform"), "Failed to set_exception for promise");
        }
        throw;
    }
}

void MaterializingCTETransform::consume(Chunk chunk)
{
    executor->push(std::move(chunk));
}

Chunk MaterializingCTETransform::generate()
{
    /// No local try/catch: every throwing operation below propagates to
    /// `MaterializingCTETransform::work()`'s outer catch, which fulfils
    /// `build_promise` with `set_exception(current_exception())`. That
    /// notifies all readers blocked in `MemorySource::generate` on
    /// `build_future.get()` of the exact failure - whether it came from
    /// `executor->finish()` (inner write pipeline failure), the cleanup
    /// resets (theoretical), or `set_value()` itself (`future_error`
    /// from a concurrent `onCancel` race, or a `system_error` from the
    /// shared-state mutex).

    /// `executor->finish()` runs the inner pushing pipeline to completion
    /// and triggers `MemorySink::onFinish`, which commits `storage.data`
    /// under `storage.mutex`. After this returns successfully readers
    /// observing `build_future` fulfilled with a value are guaranteed to
    /// see the committed data via `MultiVersion::get()`.
    executor->finish();
    executor.reset();
    table_out.reset();

    auto seconds = static_cast<double>(watch.elapsedNanoseconds()) / 1e9;
    LOG_DEBUG(getLogger("MaterializingCTETransform"), "Finished materializing CTE with name '{}' in {} seconds", materialized_cte->cte_name, seconds);

    /// Releases every reader blocked in `MemorySource::generate` on
    /// `build_future.get()`. Throws only if the promise was already
    /// fulfilled by `onCancel`, in which case `work()`'s catch routes
    /// the `future_error` through `set_exception` (a no-op because the
    /// promise is already set) and rethrows - readers have already
    /// observed the cancellation exception, so correctness is preserved.
    materialized_cte->build_promise.set_value();

    return {};
}

void MaterializingCTETransform::onCancel() noexcept
{
    /// Reached from `ExecutingGraph::cancel` (via `PipelineExecutor::cancel`
    /// -> `graph->cancel` -> `processor->cancel` -> `IProcessor::cancel` ->
    /// `onCancel`). When real cancellation fires while the writer hasn't yet
    /// fulfilled `build_promise`, this gives any reader blocked in
    /// `MemorySource::generate`'s `build_future.get()` a chance to unwind -
    /// without it, single-threaded inner pipelines (e.g. those constructed
    /// for `FutureSetFromSubquery::buildOrderedSetInplace`) could deadlock
    /// with the reader holding the only worker thread.
    ///
    /// Skip if the writer has already finished. `CompletedPipelineExecutor`'s
    /// destructor (`src/Processors/Executors/PipelineExecutor.h:67`) calls
    /// `cancel` defensively after every successful `execute()`, so this
    /// codepath runs on every successful inplace materialization. Without
    /// this early return we'd hit `set_exception` on an already-fulfilled
    /// promise and log a benign `future_error` as `<Error>` - the kind of
    /// noise that fails Fast Test's "stderr must be empty" assertion. The
    /// genuine cancel-before-completion case still proceeds below: there
    /// `isBuilt()` returns false, `set_exception` succeeds, and waiters
    /// unblock with `QUERY_WAS_CANCELLED`.
    if (materialized_cte->isBuilt())
        return;

    try
    {
        materialized_cte->build_promise.set_exception(std::make_exception_ptr(
            Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                      "Materialization of CTE '{}' was cancelled",
                      materialized_cte->cte_name)));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Promise was fulfilled between the `isBuilt()` check and this
        /// `set_exception` - genuine race with `generate` or another
        /// concurrent `onCancel`. Readers have already observed an outcome;
        /// the log line here is actually diagnostic (rare race), not the
        /// spurious teardown noise.
        tryLogCurrentException(getLogger("MaterializingCTETransform"), "Failed to set_exception for promise");
    }
}

void MaterializingCTETransform::init()
{
    is_initialized = true;

    /// Prepare writing to temporary table
    auto storage = materialized_cte->storage;
    table_out = QueryPipeline(storage->write({}, storage->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false), nullptr, /*async_insert=*/false));
    executor = std::make_unique<PushingPipelineExecutor>(table_out);

    LOG_DEBUG(getLogger("MaterializingCTETransform"), "Starting materialization of CTE with name '{}'", materialized_cte->cte_name);

    watch.restart();
    executor->start();
}

}
