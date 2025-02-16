#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <base/types.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/MergeTree/MergeTreeIndexConditionBloomFilter.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/BloomFilterHash.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_QUERY;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

MergeTreeIndexBloomFilter::MergeTreeIndexBloomFilter(
    const IndexDescription & index_,
    size_t bits_per_row_,
    size_t hash_functions_)
    : IMergeTreeIndex(index_)
    , bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
{
    assert(bits_per_row != 0);
    assert(hash_functions != 0);
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, index.column_names.size());
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBloomFilter::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorBloomFilter>(bits_per_row, hash_functions, index.column_names);
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomFilter::createIndexCondition(const ActionsDAGPtr & filter_actions_dag, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionBloomFilter>(filter_actions_dag, context, index.sample_block, hash_functions);
}

static void assertIndexColumnsType(const Block & header)
{
    if (!header || !header.columns())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index must have columns.");

    const DataTypes & columns_data_types = header.getDataTypes();

    for (const auto & type : columns_data_types)
    {
        const IDataType * actual_type = BloomFilter::getPrimitiveType(type).get();
        WhichDataType which(actual_type);

        if (!which.isUInt() && !which.isInt() && !which.isString() && !which.isFixedString() && !which.isFloat() &&
            !which.isDate() && !which.isDateTime() && !which.isDateTime64() && !which.isEnum() && !which.isUUID() &&
            !which.isIPv4() && !which.isIPv6())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type {} of bloom filter index.", type->getName());
    }
}

MergeTreeIndexPtr bloomFilterIndexCreatorNew(
    const IndexDescription & index)
{
    double max_conflict_probability = 0.025;

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];
        max_conflict_probability = std::min<Float64>(1.0, std::max<Float64>(argument.safeGet<Float64>(), 0.0));
    }

    const auto & bits_per_row_and_size_of_hash_functions = BloomFilterHash::calculationBestPractices(max_conflict_probability);

    return std::make_shared<MergeTreeIndexBloomFilter>(
        index, bits_per_row_and_size_of_hash_functions.first, bits_per_row_and_size_of_hash_functions.second);
}

void bloomFilterIndexValidatorNew(const IndexDescription & index, bool attach)
{
    assertIndexColumnsType(index.sample_block);

    if (index.arguments.size() > 1)
    {
        if (!attach) /// This is for backward compatibility.
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "BloomFilter index cannot have more than one parameter.");
    }

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];

        if (!attach && (argument.getType() != Field::Types::Float64 || argument.get<Float64>() < 0 || argument.get<Float64>() > 1))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The BloomFilter false positive must be a double number between 0 and 1.");
    }
}

}
