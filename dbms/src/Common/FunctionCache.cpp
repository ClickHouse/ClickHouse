#include <Common/FunctionCache.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

void FunctionCache::addBlock(Block && block)
{
    if (block.columns() != columns_num)
        throw Exception("Adding to cache block with " + toString(block.columns()) + " columns."
            " Expected: " + toString(columns_num), ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

    size_t rows = block.rows();
    blocks.push_back(std::make_shared<Block>(std::move(block)));
    for (size_t i = 0; i < columns_num; ++i)
    {
        const auto & column = *blocks.back()->getByPosition(i).column;
        for (size_t j = 0; j < rows; ++j)
            cache[column[j]] = {blocks.back().get(), j};
    }
}

void FunctionCache::get(const FunctionBasePtr & func, size_t column_index, const Field & arg_field, Field & res_field)
{
    if (!func || arg_field.isNull())
    {
        res_field = {};
        return;
    }

    if (column_index >= columns_num)
        throw Exception("Column index: " + DB::toString(column_index)
            + " is out of range [0" + toString(column_index) + ")", ErrorCodes::LOGICAL_ERROR);

    size_t num_arguments = func->getArgumentTypes().size();
    if (num_arguments != 1)
        throw Exception("Function " + func->getName() + " has "
            + toString(num_arguments) + "arguments. Expected 1.", ErrorCodes::LOGICAL_ERROR);

    size_t result_pos;
    auto result_name = "_" + func->getName() + "_" + toString(column_index);

    auto it = cache.find(arg_field);
    if (it == cache.end())
    {
        res_field = {};
        return;
    }

    auto [block, row] = it->second;
    if (!block->has(result_name))
    {
        // std::cerr << "executing func: " << func->getName() << "\n";
        result_pos = block->columns();
        block->insert({nullptr, func->getReturnType(), result_name});
        func->execute(*block, {column_index}, result_pos, block->rows());
    }
    else
        result_pos = block->getPositionByName(result_name);

    block->getByPosition(result_pos).column->get(row, res_field);
}

}
