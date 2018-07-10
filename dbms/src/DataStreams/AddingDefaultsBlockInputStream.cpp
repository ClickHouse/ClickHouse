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

    BlockDelayedDefaults delayed_defaults = res.delayed_defaults;
    if (delayed_defaults.empty())
        return res;

    Block evaluate_block{res};
    for (const auto & column : column_defaults)
        evaluate_block.erase(column.first);

    evaluateMissingDefaultsUnsafe(evaluate_block, header.getNamesAndTypesList(), column_defaults, context);

    ColumnsWithTypeAndName mixed_columns;
    mixed_columns.reserve(std::min(column_defaults.size(), delayed_defaults.size()));

    for (const ColumnWithTypeAndName & column_def : evaluate_block)
    {
        const String & column_name = column_def.name;

        if (column_defaults.count(column_name) == 0)
            continue;

        size_t block_column_position = res.getPositionByName(column_name);
        const ColumnWithTypeAndName & column_read = res.getByPosition(block_column_position);

        if (column_read.column->size() != column_def.column->size())
            throw Exception("Mismach column sizes while adding defaults", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        const BlockDelayedDefaults::BitMask & mask = delayed_defaults.getColumnBitmask(block_column_position);
        MutableColumnPtr column_mixed = column_read.column->cloneEmpty();

        for (size_t row_idx = 0; row_idx < column_read.column->size(); ++row_idx)
        {
            if (mask[row_idx])
            {
                if (column_def.column->isColumnConst())
                    column_mixed->insert((*column_def.column)[row_idx]);
                else
                    column_mixed->insertFrom(*column_def.column, row_idx);
            }
            else
                column_mixed->insertFrom(*column_read.column, row_idx);
        }

        ColumnWithTypeAndName mix = column_read.cloneEmpty();
        mix.column = std::move(column_mixed);
        mixed_columns.emplace_back(std::move(mix));
    }

    for (auto & column : mixed_columns)
    {
        res.erase(column.name);
        res.insert(std::move(column));
    }

    return res;
}

}
