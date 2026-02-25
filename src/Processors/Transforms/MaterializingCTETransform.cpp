#include <Processors/Transforms/MaterializingCTETransform.h>

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
    auto storage = materialized_cte->holder.getTable();
    table_out = QueryPipeline(storage->write({}, storage->getInMemoryMetadataPtr(), nullptr, /*async_insert=*/false));
    executor = std::make_unique<PushingPipelineExecutor>(table_out);
    executor->start();
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

    if (materialized_cte->is_built.exchange(true))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CTE is already built");

    return {};
}

}
