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
    /// Destructor must not throw. `PushingPipelineExecutor::cancel` can throw
    /// (e.g. on a sink finish or a downstream destruction failure), so the
    /// exception is logged and swallowed. No reader/writer-handshake fallback
    /// is needed - ordering is enforced by `DelayedPortsProcessor` at the
    /// scheduler level, not by anything this destructor does.
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
}

void MaterializingCTETransform::work()
{
    if (!is_initialized)
        init();

    IAccumulatingTransform::work();
}

void MaterializingCTETransform::consume(Chunk chunk)
{
    executor->push(std::move(chunk));
}

Chunk MaterializingCTETransform::generate()
{
    /// `executor->finish()` runs the inner pushing pipeline to completion
    /// and triggers `MemorySink::onFinish`, which commits `storage.data`
    /// under `storage.mutex`. After this returns successfully, the
    /// release-store below makes the committed data visible to any
    /// `MemorySource` that subsequently acquire-loads `is_built` -
    /// pairing with the assertion in `ReadFromMemoryStorageStep`.
    executor->finish();
    executor.reset();
    table_out.reset();

    auto seconds = static_cast<double>(watch.elapsedNanoseconds()) / 1e9;
    LOG_DEBUG(getLogger("MaterializingCTETransform"), "Finished materializing CTE with name '{}' in {} seconds", materialized_cte->cte_name, seconds);

    /// `exchange(true, release)` returns the previous value: if `true`, another
    /// `MaterializingCTETransform` already finished for this same `MaterializedCTE`,
    /// which violates the single-writer invariant enforced at plan time by
    /// `is_materialization_planned.exchange(true)` in
    /// `DelayedMaterializingCTEsStep::makePlansForCTEs`. Two writers would both
    /// append to the same `StorageMemory`, producing duplicated CTE rows.
    /// Throw on detection rather than silently double-publish.
    if (materialized_cte->is_built.exchange(true, std::memory_order_release))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Materialized CTE '{}' was built twice",
            materialized_cte->cte_name);

    return {};
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
