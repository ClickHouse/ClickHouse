#include <Processors/Transforms/FilterSortedStreamByRange.h>

#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

FilterSortedStreamByRange::FilterSortedStreamByRange(
    SharedHeader header_, ExpressionActionsPtr expression_, String filter_column_name_, bool remove_filter_column_, bool on_totals_)
    : ISimpleTransform(
          header_,
          std::make_shared<const Block>(
              FilterTransform::transformHeader(*header_, &expression_->getActionsDAG(), filter_column_name_, remove_filter_column_)),
          true)
    , filter_transform(header_, expression_, filter_column_name_, remove_filter_column_, on_totals_)
{
    assertBlocksHaveEqualStructure(
        *header_, getOutputPort().getHeader(), "Expression for FilterSortedStreamByRange should not change header");
}

void FilterSortedStreamByRange::transform(Chunk & chunk)
{
    const UInt64 rows_before_filtration = chunk.getNumRows();
    if (rows_before_filtration < 2)
    {
        filter_transform.transform(chunk);
        return;
    }

    // Evaluate expression on just the first and the last row.
    // If both of them satisfies conditions, then skip calculation for all the rows in between.
    auto quick_check_columns = chunk.cloneEmptyColumns();
    auto src_columns = chunk.detachColumns();
    for (auto row : {static_cast<UInt64>(0), rows_before_filtration - 1})
    {
        for (size_t col = 0; col < quick_check_columns.size(); ++col)
            quick_check_columns[col]->insertFrom(*src_columns[col].get(), row);
    }
    chunk.setColumns(std::move(quick_check_columns), 2);
    filter_transform.transform(chunk);
    const bool all_rows_will_pass_filter = chunk.getNumRows() == 2;

    chunk.setColumns(std::move(src_columns), rows_before_filtration);

    // Not all rows satisfy conditions.
    if (!all_rows_will_pass_filter)
        filter_transform.transform(chunk);
}

}
