#include <Functions/Helpers/WrapLowCardinalityTransform.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>

namespace DB
{

static DataTypePtr recursiveRemoveLowCardinality(const DataTypePtr & type)
{
    if (!type)
        return type;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(recursiveRemoveLowCardinality(array_type->getNestedType()));

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = tuple_type->getElements();
        for (auto & element : elements)
            element = recursiveRemoveLowCardinality(element);

        if (tuple_type->haveExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, tuple_type->getElementNames());
        else
            return std::make_shared<DataTypeTuple>(elements);
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return low_cardinality_type->getDictionaryType();

    return type;
}

static ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
        return ColumnArray::create(recursiveRemoveLowCardinality(column_array->getDataPtr()), column_array->getOffsetsPtr());

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return ColumnConst::create(recursiveRemoveLowCardinality(column_const->getDataColumnPtr()), column_const->size());

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        Columns columns = column_tuple->getColumns();
        for (auto & element : columns)
            element = recursiveRemoveLowCardinality(element);
        return ColumnTuple::create(columns);
    }

    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return column_low_cardinality->convertToFullColumn();

    return column;
}

static const ColumnLowCardinality * findLowCardinalityArgument(const Block & block, const ColumnNumbers & args)
{
    const ColumnLowCardinality * result_column = nullptr;

    for (auto arg : args)
    {
        const ColumnWithTypeAndName & column = block.getByPosition(arg);
        if (auto * low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(column.column.get()))
        {
            if (result_column)
                throw Exception("Expected single dictionary argument for function.", ErrorCodes::LOGICAL_ERROR);

            result_column = low_cardinality_column;
        }
    }

    return result_column;
}

static Block wrapLowCardinality(Blocks && blocks, const ColumnNumbers & column_numbers, size_t result,
                                   bool can_be_executed_on_default_arguments,
                                   const PreparedFunctionLowCardinalityResultCachePtr & cache)
{
    Block & block_without_low_cardinality = blocks.at(0);
    Block & block = blocks.at(1);
    ColumnWithTypeAndName & res = block.getByPosition(result);

    if (auto * res_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(res.type.get()))
    {
        const auto * low_cardinality_column = findLowCardinalityArgument(block, column_numbers);

        auto & keys = block_without_low_cardinality.safeGetByPosition(result).column;
        if (auto full_column = keys->convertToFullColumnIfConst())
            keys = full_column;

        auto res_mut_dictionary = DataTypeLowCardinality::createColumnUnique(*res_low_cardinality_type->getDictionaryType());
        ColumnPtr res_indexes = res_mut_dictionary->uniqueInsertRangeFrom(*keys, 0, keys->size());
        ColumnUniquePtr res_dictionary = std::move(res_mut_dictionary);

        auto indexes = res.column;
        if (indexes)
        {
            bool use_cache = cache && can_be_executed_on_default_arguments
                             && low_cardinality_column && low_cardinality_column->isSharedDictionary();
            PreparedFunctionLowCardinalityResultCache::DictionaryKey key;

            if (use_cache)
            {
                const auto & dictionary = low_cardinality_column->getDictionary();
                key = {dictionary.getHash(), UInt64(dictionary.size())};

                auto cache_values = std::make_shared<PreparedFunctionLowCardinalityResultCache::CachedValues>();
                cache_values->dictionary_holder = low_cardinality_column->getDictionaryPtr();
                cache_values->function_result = res_dictionary;
                cache_values->index_mapping = res_indexes;

                cache_values = cache->getOrSet(key, cache_values);
                res_dictionary = cache_values->function_result;
                res_indexes = cache_values->index_mapping;
            }

            res.column = ColumnLowCardinality::create(res_dictionary, res_indexes->index(*indexes, 0), use_cache);
        }
        else
            res.column = ColumnLowCardinality::create(res_dictionary, res_indexes);
    }
    else
        res.column = block_without_low_cardinality.safeGetByPosition(result).column;

    return block;
}


WrapLowCardinalityTransform::WrapLowCardinalityTransform(
    Blocks input_headers, const ColumnNumbers & column_numbers, size_t result, bool can_be_executed_on_default_arguments)
    : ITransform({input_headers},
                 {wrapLowCardinality(Blocks(input_headers), column_numbers, result, can_be_executed_on_default_arguments, nullptr)})
    , column_numbers(column_numbers)
    , result(result)
    , can_be_executed_on_default_arguments(can_be_executed_on_default_arguments)
{
}

Blocks WrapLowCardinalityTransform::transform(Blocks && blocks)
{
    return {wrapLowCardinality(std::move(blocks), column_numbers, result, can_be_executed_on_default_arguments, cache)};
}

}
