#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <base/types.h>
#include <base/bit_cast.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/MergeTree/MergeTreeIndexConditionBloomFilter.h>
#include <Parsers/queryToString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Interpreters/BloomFilterHash.h>


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

bool MergeTreeIndexBloomFilter::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    Names required_columns = index.expression->getRequiredColumns();
    NameSet required_columns_set(required_columns.begin(), required_columns.end());

    std::vector<ASTPtr> nodes_to_check;
    nodes_to_check.emplace_back(node);

    while (!nodes_to_check.empty())
    {
        auto node_to_check = nodes_to_check.back();
        nodes_to_check.pop_back();

        const auto & column_name = node_to_check->getColumnName();
        if (required_columns_set.find(column_name) != required_columns_set.end())
            return true;

        if (const auto * function = typeid_cast<const ASTFunction *>(node_to_check.get()))
        {
            auto & function_arguments_children = function->arguments->children;
            nodes_to_check.insert(nodes_to_check.end(), function_arguments_children.begin(), function_arguments_children.end());
        }
    }

    return true;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBloomFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorBloomFilter>(bits_per_row, hash_functions, index.column_names);
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomFilter::createIndexCondition(const SelectQueryInfo & query_info, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionBloomFilter>(query_info, context, index.sample_block, hash_functions);
}

static void assertIndexColumnsType(const Block & header)
{
    if (!header || !header.columns())
        throw Exception("Index must have columns.", ErrorCodes::INCORRECT_QUERY);

    const DataTypes & columns_data_types = header.getDataTypes();

    for (const auto & type : columns_data_types)
    {
        const IDataType * actual_type = BloomFilter::getPrimitiveType(type).get();
        WhichDataType which(actual_type);

        if (!which.isUInt() && !which.isInt() && !which.isString() && !which.isFixedString() && !which.isFloat() &&
            !which.isDate() && !which.isDateTime() && !which.isDateTime64() && !which.isEnum() && !which.isUUID())
            throw Exception("Unexpected type " + type->getName() + " of bloom filter index.",
                            ErrorCodes::ILLEGAL_COLUMN);
    }
}

MergeTreeIndexPtr bloomFilterIndexCreatorNew(
    const IndexDescription & index)
{
    double max_conflict_probability = 0.025;

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];
        max_conflict_probability = std::min(Float64(1), std::max(argument.safeGet<Float64>(), Float64(0)));
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
            throw Exception("BloomFilter index cannot have more than one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];

        if (!attach && (argument.getType() != Field::Types::Float64 || argument.get<Float64>() < 0 || argument.get<Float64>() > 1))
            throw Exception("The BloomFilter false positive must be a double number between 0 and 1.", ErrorCodes::BAD_ARGUMENTS);
    }
}

}
