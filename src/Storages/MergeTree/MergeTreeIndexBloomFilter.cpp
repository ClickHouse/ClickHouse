#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Core/Types.h>
#include <ext/bit_cast.h>
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
    const StorageMetadataSkipIndexField & index_,
    size_t bits_per_row_,
    size_t hash_functions_)
    : IMergeTreeIndex(index)
    , bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, index.column_names.size());
}

bool MergeTreeIndexBloomFilter::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    const String & column_name = node->getColumnName();

    for (const auto & cname : index.column_names)
        if (column_name == cname)
            return true;

    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        for (const auto & children : func->arguments->children)
            if (mayBenefitFromIndexForIn(children))
                return true;
    }

    return false;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBloomFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorBloomFilter>(bits_per_row, hash_functions, index.column_names);
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomFilter::createIndexCondition(const SelectQueryInfo & query_info, const Context & context) const
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
            !which.isDateOrDateTime() && !which.isEnum())
            throw Exception("Unexpected type " + type->getName() + " of bloom filter index.",
                            ErrorCodes::ILLEGAL_COLUMN);
    }
}

std::unique_ptr<IMergeTreeIndex> bloomFilterIndexCreatorNew(
    const StorageMetadataSkipIndexField & index, bool attach)
{
    assertIndexColumnsType(index.sample_block);

    double max_conflict_probability = 0.025;

    if (index.arguments.size() > 1)
    {
        if (!attach)    /// This is for backward compatibility.
            throw Exception("BloomFilter index cannot have more than one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }


    if (!index.arguments.empty())
    {
        auto * argument = arguments->children[0]->as<ASTLiteral>();

        if (!argument || (argument->value.safeGet<Float64>() < 0 || argument->value.safeGet<Float64>() > 1))
        {
            if (!attach || !argument)   /// This is for backward compatibility.
                throw Exception("The BloomFilter false positive must be a double number between 0 and 1.", ErrorCodes::BAD_ARGUMENTS);

            argument->value = Field(std::min(Float64(1), std::max(argument->value.safeGet<Float64>(), Float64(0))));
        }

        max_conflict_probability = argument->value.safeGet<Float64>();
    }

    const auto & bits_per_row_and_size_of_hash_functions = BloomFilterHash::calculationBestPractices(max_conflict_probability);

    return std::make_unique<MergeTreeIndexBloomFilter>(
        node->name, std::move(index_expr), index_sample.getNames(), index_sample.getDataTypes(), index_sample, node->granularity,
        bits_per_row_and_size_of_hash_functions.first, bits_per_row_and_size_of_hash_functions.second);
}

}
