#include <Processors/Transforms/MaterializingCTETransform.h>

#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <Common/Logger.h>
#include <Processors/Port.h>
#include <Storages/IStorage.h>

namespace DB
{

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

    materialized_cte->is_built.store(true, std::memory_order_release);

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
