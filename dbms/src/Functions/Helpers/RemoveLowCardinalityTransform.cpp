#include <Functions/Helpers/RemoveLowCardinalityTransform.h>

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

static ColumnPtr replaceLowCardinalityColumnsByNestedAndGetDictionaryIndexes(
        Block & block, const ColumnNumbers & args, bool can_be_executed_on_default_arguments)
{
    size_t num_rows = block.getNumRows();
    ColumnPtr indexes;

    for (auto arg : args)
    {
        ColumnWithTypeAndName & column = block.getByPosition(arg);
        if (auto * low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(column.column.get()))
        {
            if (indexes)
                throw Exception("Expected single dictionary argument for function.", ErrorCodes::LOGICAL_ERROR);

            indexes = low_cardinality_column->getIndexesPtr();
            num_rows = low_cardinality_column->getDictionary().size();
        }
    }

    for (auto arg : args)
    {
        ColumnWithTypeAndName & column = block.getByPosition(arg);
        if (auto * column_const = checkAndGetColumn<ColumnConst>(column.column.get()))
            column.column = column_const->removeLowCardinality()->cloneResized(num_rows);
        else if (auto * low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(column.column.get()))
        {
            auto * low_cardinality_type = checkAndGetDataType<DataTypeLowCardinality>(column.type.get());

            if (!low_cardinality_type)
                throw Exception("Incompatible type for low cardinality column: " + column.type->getName(),
                                ErrorCodes::LOGICAL_ERROR);

            if (can_be_executed_on_default_arguments)
                column.column = low_cardinality_column->getDictionary().getNestedColumn();
            else
            {
                auto dict_encoded = low_cardinality_column->getMinimalDictionaryEncodedColumn(0, low_cardinality_column->size());
                column.column = dict_encoded.dictionary;
                indexes = dict_encoded.indexes;
            }
            column.type = low_cardinality_type->getDictionaryType();
        }
    }

    return indexes;
}

static void convertLowCardinalityColumnsToFull(Block & block, const ColumnNumbers & args)
{
    for (auto arg : args)
    {
        ColumnWithTypeAndName & column = block.getByPosition(arg);

        column.column = recursiveRemoveLowCardinality(column.column);
        column.type = recursiveRemoveLowCardinality(column.type);
    }
}

static Blocks removeLowCardinality(Block && block, const ColumnNumbers & column_numbers, size_t result,
                                   bool can_be_executed_on_default_arguments,
                                   const PreparedFunctionLowCardinalityResultCachePtr & cache)
{
    ColumnWithTypeAndName & res = block.getByPosition(result);

    Block block_without_low_cardinality = block.cloneWithoutColumns();
    for (auto arg : column_numbers)
        block_without_low_cardinality.safeGetByPosition(arg).column = block.safeGetByPosition(arg).column;

    if (auto * res_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(res.type.get()))
    {
        const auto * low_cardinality_column = findLowCardinalityArgument(block, column_numbers);

        bool use_cache = cache && can_be_executed_on_default_arguments
                         && low_cardinality_column && low_cardinality_column->isSharedDictionary();
        PreparedFunctionLowCardinalityResultCache::DictionaryKey key;

        if (use_cache)
        {
            const auto & dictionary = low_cardinality_column->getDictionary();
            key = {dictionary.getHash(), UInt64(dictionary.size())};

            auto cached_values = cache->get(key);
            if (cached_values)
            {
                auto indexes = cached_values->index_mapping->index(low_cardinality_column->getIndexes(), 0);
                res.column = ColumnLowCardinality::create(cached_values->function_result, indexes, true);
                return {{}, std::move(block)};
            }
        }

        block_without_low_cardinality.getByPosition(result).type = res_low_cardinality_type->getDictionaryType();
        ColumnPtr indexes = replaceLowCardinalityColumnsByNestedAndGetDictionaryIndexes(
                block_without_low_cardinality, column_numbers, can_be_executed_on_default_arguments);

        res.column = indexes;
        return {std::move(block_without_low_cardinality), std::move(block)};
    }
    else
    {
        convertLowCardinalityColumnsToFull(block_without_low_cardinality, column_numbers);
        return {std::move(block_without_low_cardinality), std::move(block)};
    }
}


RemoveLowCardinalityTransform::RemoveLowCardinalityTransform(
    Block input_header, const ColumnNumbers & column_numbers, size_t result, bool can_be_executed_on_default_arguments)
    : ITransform({input_header}, removeLowCardinality(Block(input_header.getColumnsWithTypeAndName()), column_numbers,
                                                      result, can_be_executed_on_default_arguments, nullptr))
    , column_numbers(column_numbers)
    , result(result)
    , can_be_executed_on_default_arguments(can_be_executed_on_default_arguments)
{
}

Blocks RemoveLowCardinalityTransform::transform(Blocks && blocks)
{
    return removeLowCardinality(std::move(blocks.at(0)), column_numbers, result,
                                can_be_executed_on_default_arguments, cache);
}

}
