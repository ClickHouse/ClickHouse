#include <Processors/Transforms/DefaultTotalsColumnsTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

DefaultTotalsColumnsTransform::DefaultTotalsColumnsTransform(
    SharedHeader header_, std::vector<size_t> positions_, bool on_totals_)
    : ISimpleTransform(header_, header_, /*skip_empty_chunks_=*/false)
    , positions(std::move(positions_))
    , on_totals(on_totals_)
{
}

void DefaultTotalsColumnsTransform::transform(Chunk & chunk)
{
    if (!on_totals || positions.empty())
        return;

    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    const auto & header = getInputPort().getHeader();

    for (const size_t position : positions)
    {
        const auto & type = header.getByPosition(position).type;
        columns[position] = type->createColumnConst(num_rows, type->getDefault())->convertToFullColumnIfConst();
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
