#include <Processors/Transforms/AddingMissedTransform.h>
#include <Interpreters/addMissingDefaults.h>


namespace DB
{

AddingMissedTransform::AddingMissedTransform(
    Block header_,
    Block result_header_,
    const ColumnsDescription & columns_,
    const Context & context_)
    : ISimpleTransform(std::move(header_), std::move(result_header_), false)
    , columns(columns_), context(context_)
{
}

void AddingMissedTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    Block src = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    auto res = addMissingDefaults(src, getOutputPort().getHeader().getNamesAndTypesList(), columns, context);
    chunk.setColumns(res.getColumns(), num_rows);
}

}
