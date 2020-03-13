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

static UInt128 hashField(const Field & field)
{
    SipHash hash;
    applyVisitor(FieldVisitorHash(hash), field);
    UInt128 key;
    hash.get128(key.low, key.high);
    return key;
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
            cache[hashField(column[j])] = {blocks.back().get(), i, j};
    }
}

static String getResultName(const FunctionBasePtr & func, size_t column_index)
{
    return "_" + func->getName() + "_" + toString(column_index);
}

void FunctionCache::get(const FunctionBasePtr & func, const Field & arg_field, Field & res_field)
{
    if (!func || arg_field.isNull())
    {
        res_field = {};
        return;
    }

    size_t num_arguments = func->getArgumentTypes().size();
    if (num_arguments != 1)
        throw Exception("Function " + func->getName() + " has "
            + toString(num_arguments) + "arguments. Expected 1.", ErrorCodes::LOGICAL_ERROR);

    auto it = cache.find(hashField(arg_field));
    if (it == cache.end())
        throw Exception("Field not found in cache. It's a bug: function cache was used in a wrong way.", ErrorCodes::LOGICAL_ERROR);

    size_t result_idx;
    auto [block, column_idx, row_idx] = it->getMapped();
    auto result_name = getResultName(func, column_idx);

    if (!block->has(result_name))
    {
        result_idx = block->columns();
        block->insert({nullptr, func->getReturnType(), result_name});
        func->execute(*block, {column_idx}, result_idx, block->rows());

        const auto & column = *block->getByPosition(result_idx).column;
        for (size_t i = 0; i < block->rows(); ++i)
            cache[hashField(column[i])] = {block, result_idx, i};
    }
    else
        result_idx = block->getPositionByName(result_name);

    block->getByPosition(result_idx).column->get(row_idx, res_field);
}

}
