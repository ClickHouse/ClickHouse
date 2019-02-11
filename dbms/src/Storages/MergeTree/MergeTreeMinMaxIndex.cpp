#include <Storages/MergeTree/MergeTreeMinMaxIndex.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>

#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}


MergeTreeMinMaxGranule::MergeTreeMinMaxGranule(const MergeTreeMinMaxIndex & index)
    : IMergeTreeIndexGranule(), index(index), parallelogram()
{
}

void MergeTreeMinMaxGranule::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
                "Attempt to write empty minmax index `" + index.name + "`", ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        type->serializeBinary(parallelogram[i].left, ostr);
        type->serializeBinary(parallelogram[i].right, ostr);
    }
}

void MergeTreeMinMaxGranule::deserializeBinary(ReadBuffer & istr)
{
    parallelogram.clear();
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        Field min_val;
        type->deserializeBinary(min_val, istr);
        Field max_val;
        type->deserializeBinary(max_val, istr);

        parallelogram.emplace_back(min_val, true, max_val, true);
    }
}

void MergeTreeMinMaxGranule::update(const Block & block, size_t * pos, UInt64 limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & column = block.getByName(index.columns[i]).column;

        Field field_min, field_max;
        column->cut(*pos, rows_read)->getExtremes(field_min, field_max);

        if (parallelogram.size() <= i)
        {
            parallelogram.emplace_back(field_min, true, field_max, true);
        }
        else
        {
            parallelogram[i].left = std::min(parallelogram[i].left, field_min);
            parallelogram[i].right = std::max(parallelogram[i].right, field_max);
        }
    }

    *pos += rows_read;
}


MinMaxCondition::MinMaxCondition(
    const SelectQueryInfo &query,
    const Context &context,
    const MergeTreeMinMaxIndex &index)
    : IIndexCondition(), index(index), condition(query, context, index.columns, index.expr) {}

bool MinMaxCondition::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MinMaxCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeMinMaxGranule> granule
        = std::dynamic_pointer_cast<MergeTreeMinMaxGranule>(idx_granule);
    if (!granule)
        throw Exception(
            "Minmax index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    return condition.mayBeTrueInParallelogram(granule->parallelogram, index.data_types);
}


MergeTreeIndexGranulePtr MergeTreeMinMaxIndex::createIndexGranule() const
{
    return std::make_shared<MergeTreeMinMaxGranule>(*this);
}

IndexConditionPtr MergeTreeMinMaxIndex::createIndexCondition(
    const SelectQueryInfo & query, const Context & context) const
{
    return std::make_shared<MinMaxCondition>(query, context, *this);
};


std::unique_ptr<IMergeTreeIndex> minmaxIndexCreator(
    const NamesAndTypesList & new_columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context)
{
    if (node->name.empty())
        throw Exception("Index must have unique name", ErrorCodes::INCORRECT_QUERY);

    if (node->type->arguments)
        throw Exception("Minmax index have not any arguments", ErrorCodes::INCORRECT_QUERY);

    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());
    auto syntax = SyntaxAnalyzer(context, {}).analyze(
        expr_list, new_columns);
    auto minmax_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);

    auto sample = ExpressionAnalyzer(expr_list, syntax, context)
        .getActions(true)->getSampleBlock();

    Names columns;
    DataTypes data_types;

    for (size_t i = 0; i < expr_list->children.size(); ++i)
    {
        const auto & column = sample.getByPosition(i);

        columns.emplace_back(column.name);
        data_types.emplace_back(column.type);
    }

    return std::make_unique<MergeTreeMinMaxIndex>(
        node->name, std::move(minmax_expr), columns, data_types, sample, node->granularity);
}

}
