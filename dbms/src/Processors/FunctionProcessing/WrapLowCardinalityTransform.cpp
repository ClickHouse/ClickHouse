#include <Processors/FunctionProcessing/WrapLowCardinalityTransform.h>
#include <Processors/FunctionProcessing/PreparedFunctionLowCardinalityResultCache.h>
#include <Processors/FunctionProcessing/removeLowCardinality.h>
#include <Functions/FunctionHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnLowCardinality.h>

namespace DB
{

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
    Blocks input_headers,
    const ColumnNumbers & column_numbers,
    size_t result,
    bool can_be_executed_on_default_arguments,
    PreparedFunctionLowCardinalityResultCachePtr cache)
    : ITransform({input_headers},
                 {wrapLowCardinality(Blocks(input_headers), column_numbers, result, can_be_executed_on_default_arguments, nullptr)})
    , column_numbers(column_numbers)
    , result(result)
    , can_be_executed_on_default_arguments(can_be_executed_on_default_arguments)
    , cache(std::move(cache))
{
}

Blocks WrapLowCardinalityTransform::transform(Blocks && blocks)
{
    return {wrapLowCardinality(std::move(blocks), column_numbers, result, can_be_executed_on_default_arguments, cache)};
}

}
