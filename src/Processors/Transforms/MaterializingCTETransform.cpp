#include <Processors/Transforms/MaterializingCTETransform.h>

#include <Common/CurrentThread.h>
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
{
    auto storage = materialized_cte->storage;
    table_out = QueryPipeline(storage->write({}, storage->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false), nullptr, /*async_insert=*/false));
    executor = std::make_unique<PushingPipelineExecutor>(table_out);
    executor->start();
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

void MaterializingCTETransform::consume(Chunk chunk)
{
    executor->push(std::move(chunk));
}

Chunk MaterializingCTETransform::generate()
{
    executor->finish();
    executor.reset();
    table_out.reset();

    LOG_DEBUG(getLogger("MaterializingCTETransform"), "Finished materializing CTE with name '{}'", materialized_cte->cte_name);

    if (materialized_cte->is_built.exchange(true))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CTE is already built");

    return {};
}

}
