#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Transforms/MaterializingCTETransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Storages/IStorage.h>

namespace DB
{

MaterializingCTETransform::MaterializingCTETransform(
    const SharedHeader & input_header_,
    TemporaryTableHolderPtr temporary_table_holder_
)
    : IAccumulatingTransform(input_header_, input_header_)
    , temporary_table_holder(std::move(temporary_table_holder_))
{
    auto storage = temporary_table_holder->getTable();
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

    return {};
}

}
