#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnConst.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/typeid_cast.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


AddingDefaultsBlockInputStream::AddingDefaultsBlockInputStream(const BlockInputStreamPtr & input,
                                                               const ColumnDefaults & column_defaults_,
                                                               const Context & context_)
    : column_defaults(column_defaults_),
      context(context_)
{
    children.push_back(input);
    header = input->getHeader();
}


Block AddingDefaultsBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (!res)
        return res;

    if (column_defaults.empty())
        return res;

    const BlockMissingValues & delayed_defaults = children.back()->getMissingValues();
    if (delayed_defaults.empty())
        return res;

    Block evaluate_block{res};
    for (const auto & column : column_defaults)
    {
        /// column_defaults contain aliases that could be ommited in evaluate_block
        if (evaluate_block.has(column.first))
            evaluate_block.erase(column.first);
    }

    evaluateMissingDefaults(evaluate_block, header.getNamesAndTypesList(), column_defaults, context, false);

    std::unordered_map<size_t, MutableColumnPtr> mixed_columns;

    for (const ColumnWithTypeAndName & column_def : evaluate_block)
    {
        const String & column_name = column_def.name;

        if (column_defaults.count(column_name) == 0)
            continue;

        size_t block_column_position = res.getPositionByName(column_name);
        const ColumnWithTypeAndName & column_read = res.getByPosition(block_column_position);

        if (column_read.column->size() != column_def.column->size())
            throw Exception("Mismach column sizes while adding defaults", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        const auto & defaults_mask = delayed_defaults.getDefaultsBitmask(block_column_position);
        if (!defaults_mask.empty())
        {
            MutableColumnPtr column_mixed = column_read.column->cloneEmpty();

            for (size_t row_idx = 0; row_idx < column_read.column->size(); ++row_idx)
            {
                if (row_idx < defaults_mask.size() && defaults_mask[row_idx])
                {
                    if (column_def.column->isColumnConst())
                        column_mixed->insert((*column_def.column)[row_idx]);
                    else
                        column_mixed->insertFrom(*column_def.column, row_idx);
                }
                else
                    column_mixed->insertFrom(*column_read.column, row_idx);
            }

            mixed_columns.emplace(std::make_pair(block_column_position, std::move(column_mixed)));
        }
    }

    if (!mixed_columns.empty())
    {
        /// replace columns saving block structure
        MutableColumns mutation = res.mutateColumns();
        for (size_t position = 0; position < mutation.size(); ++position)
        {
            auto it = mixed_columns.find(position);
            if (it != mixed_columns.end())
                mutation[position] = std::move(it->second);
        }
        res.setColumns(std::move(mutation));
    }

    return res;
}

}
