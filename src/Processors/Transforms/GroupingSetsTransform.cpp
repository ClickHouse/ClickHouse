#include <cstddef>
#include <Processors/Transforms/GroupingSetsTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

Block GroupingSetsTransform::appendGroupingColumn(Block block)
{
    Block res;

    size_t rows = block.rows();
    auto column = ColumnUInt64::create(rows);

    res.insert({ColumnPtr(std::move(column)), std::make_shared<DataTypeUInt64>(), "__grouping_set"});

    for (auto & col : block)
        res.insert(std::move(col));

    return res;
}

GroupingSetsTransform::GroupingSetsTransform(
    Block input_header,
    Block output_header,
    AggregatingTransformParamsPtr params_,
    ColumnNumbers const & missing_columns_,
    size_t set_id_
)
    : ISimpleTransform(std::move(input_header), appendGroupingColumn(std::move(output_header)), true)
    , params(std::move(params_))
    , missing_columns(missing_columns_)
    , set_id(set_id_)
{}

void GroupingSetsTransform::transform(Chunk & chunk)
{
    size_t rows = chunk.getNumRows();

    auto columns = chunk.detachColumns();
    Columns result_columns;
    auto const & output_header = getOutputPort().getHeader();

    result_columns.reserve(output_header.columns());
    const size_t grouping_set_pos = 0;

    size_t real_column_index = 0, missign_column_index = 0;
    for (size_t i = 0; i < output_header.columns(); ++i)
    {
        if (i == grouping_set_pos)
        {
            result_columns.push_back(ColumnConst::create(ColumnUInt64::create(1, set_id), rows));
            continue;
        }
        if (missign_column_index < missing_columns.size() && missing_columns[missign_column_index] + 1 == i)
            result_columns.push_back(output_header.getByPosition(missing_columns[missign_column_index++] + 1).column->cloneResized(rows));
        else
            result_columns.push_back(std::move(columns[real_column_index++]));
    }

    chunk.setColumns(std::move(result_columns), rows);
}

}
