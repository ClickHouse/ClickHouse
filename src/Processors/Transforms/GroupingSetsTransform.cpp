#include <cstddef>
#include <Processors/Transforms/GroupingSetsTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

GroupingSetsTransform::GroupingSetsTransform(
    Block input_header,
    Block output_header,
    AggregatingTransformParamsPtr params_,
    ColumnNumbersList const & missing_columns_,
    size_t set_id_
)
    : ISimpleTransform(std::move(input_header), std::move(output_header), true)
    , params(std::move(params_))
    , missing_columns(missing_columns_[set_id_])
    , set_id(set_id_)
    , output_size(getOutputPort().getHeader().columns())
{}

void GroupingSetsTransform::transform(Chunk & chunk)
{
    size_t rows = chunk.getNumRows();

    auto columns = chunk.detachColumns();
    Columns result_columns;
    auto const & output_header = getOutputPort().getHeader();

    result_columns.reserve(output_header.columns());
    size_t real_column_index = 0, missign_column_index = 0;
    for (size_t i = 0; i < output_header.columns() - 1; ++i)
    {
        if (missign_column_index < missing_columns.size() && missing_columns[missign_column_index] == i)
            result_columns.push_back(output_header.getByPosition(missing_columns[missign_column_index++]).column->cloneResized(rows));
        else
            result_columns.push_back(std::move(columns[real_column_index++]));
    }
    result_columns.push_back(ColumnUInt64::create(rows, set_id));

    chunk.setColumns(std::move(result_columns), rows);
}

}
