#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Core/Types.h>
#include <ext/bit_cast.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/MergeTree/MergeTreeIndexConditionBloomFilter.h>
#include <Parsers/queryToString.h>
#include <Columns/ColumnConst.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

MergeTreeIndexBloomFilter::MergeTreeIndexBloomFilter(
    const String & name, const ExpressionActionsPtr & expr, const Names & columns, const DataTypes & data_types, const Block & header,
    size_t granularity, size_t bits_per_row_, size_t hash_functions_)
    : IMergeTreeIndex(name, expr, columns, data_types, header, granularity), bits_per_row(bits_per_row_), hash_functions(hash_functions_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, columns.size());
}

bool MergeTreeIndexBloomFilter::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    const String & column_name = node->getColumnName();

    for (const auto & name : columns)
        if (column_name == name)
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

IndexConditionPtr MergeTreeIndexBloomFilter::createIndexCondition(const SelectQueryInfo & query_info, const Context & context) const
{
    return std::make_shared<MergeTreeIndexConditionBloomFilter>(query_info, context, header, hash_functions);
}

static void assertIndexColumnsType(const Block &header)
{
    if (!header || !header.columns())
        throw Exception("Index must have columns.", ErrorCodes::INCORRECT_QUERY);

    const DataTypes & columns_data_types = header.getDataTypes();

    for (size_t index = 0; index < columns_data_types.size(); ++index)
    {
        WhichDataType which(columns_data_types[index]);

        if (!which.isUInt() && !which.isInt() && !which.isString() && !which.isFixedString() && !which.isFloat() &&
            !which.isDateOrDateTime() && !which.isEnum())
            throw Exception("Unexpected type " + columns_data_types[index]->getName() + " of bloom filter index.",
                            ErrorCodes::ILLEGAL_COLUMN);
    }
}

std::unique_ptr<IMergeTreeIndex> bloomFilterIndexCreatorNew(const NamesAndTypesList & columns, std::shared_ptr<ASTIndexDeclaration> node, const Context & context)
{
    if (node->name.empty())
        throw Exception("Index must have unique name.", ErrorCodes::INCORRECT_QUERY);

    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());

    auto syntax = SyntaxAnalyzer(context, {}).analyze(expr_list, columns);
    auto index_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);
    auto index_sample = ExpressionAnalyzer(expr_list, syntax, context).getActions(true)->getSampleBlock();

    assertIndexColumnsType(index_sample);

    double max_conflict_probability = 0.025;
    if (node->type->arguments && !node->type->arguments->children.empty())
        max_conflict_probability = typeid_cast<const ASTLiteral &>(*node->type->arguments->children[0]).value.get<Float64>();

    const auto & bits_per_row_and_size_of_hash_functions = calculationBestPractices(max_conflict_probability);

    return std::make_unique<MergeTreeIndexBloomFilter>(
        node->name, std::move(index_expr), index_sample.getNames(), index_sample.getDataTypes(), index_sample, node->granularity,
        bits_per_row_and_size_of_hash_functions.first, bits_per_row_and_size_of_hash_functions.second);
}

}
