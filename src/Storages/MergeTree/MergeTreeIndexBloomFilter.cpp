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
    const String & name_, const ExpressionActionsPtr & expr_, const Names & columns_, const DataTypes & data_types_, const Block & header_,
    size_t granularity_, size_t bits_per_row_, size_t hash_functions_)
    : IMergeTreeIndex(name_, expr_, columns_, data_types_, header_, granularity_), bits_per_row(bits_per_row_),
      hash_functions(hash_functions_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, columns.size());
}

bool MergeTreeIndexBloomFilter::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    const String & column_name = node->getColumnName();

    for (const auto & cname : columns)
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
    return std::make_shared<MergeTreeIndexAggregatorBloomFilter>(bits_per_row, hash_functions, columns);
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomFilter::createIndexCondition(const SelectQueryInfo & query_info, const Context & context) const
{
    return std::make_shared<MergeTreeIndexConditionBloomFilter>(query_info, context, header, hash_functions);
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
    const NamesAndTypesList & columns, std::shared_ptr<ASTIndexDeclaration> node, const Context & context, bool attach)
{
    if (node->name.empty())
        throw Exception("Index must have unique name.", ErrorCodes::INCORRECT_QUERY);

    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());

    auto syntax = SyntaxAnalyzer(context).analyze(expr_list, columns);
    auto index_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);
    auto index_sample = ExpressionAnalyzer(expr_list, syntax, context).getActions(true)->getSampleBlock();

    assertIndexColumnsType(index_sample);

    double max_conflict_probability = 0.025;
    const auto & arguments = node->type->arguments;

    if (arguments && arguments->children.size() > 1)
    {
        if (!attach)    /// This is for backward compatibility.
            throw Exception("BloomFilter index cannot have more than one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        arguments->children = { arguments->children[0] };
    }


    if (arguments && !arguments->children.empty())
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
