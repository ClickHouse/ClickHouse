#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

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


MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(const MergeTreeIndexMinMax & index)
    : IMergeTreeIndexGranule(), index(index), parallelogram() {}

MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(
    const MergeTreeIndexMinMax & index, std::vector<Range> && parallelogram)
    : IMergeTreeIndexGranule(), index(index), parallelogram(std::move(parallelogram)) {}

void MergeTreeIndexGranuleMinMax::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
            "Attempt to write empty minmax index " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];
        if (!type->isNullable())
        {
            type->serializeBinary(parallelogram[i].left, ostr);
            type->serializeBinary(parallelogram[i].right, ostr);
        }
        else
        {
            bool is_null = parallelogram[i].left.isNull() || parallelogram[i].right.isNull(); // one is enough
            writeBinary(is_null, ostr);
            if (!is_null)
            {
                type->serializeBinary(parallelogram[i].left, ostr);
                type->serializeBinary(parallelogram[i].right, ostr);
            }
        }
    }
}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & istr)
{
    parallelogram.clear();
    Field min_val;
    Field max_val;
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];
        if (!type->isNullable())
        {
            type->deserializeBinary(min_val, istr);
            type->deserializeBinary(max_val, istr);
        }
        else
        {
            bool is_null;
            readBinary(is_null, istr);
            if (!is_null)
            {
                type->deserializeBinary(min_val, istr);
                type->deserializeBinary(max_val, istr);
            }
            else
            {
                min_val = Null();
                max_val = Null();
            }
        }
        parallelogram.emplace_back(min_val, true, max_val, true);
    }
}


MergeTreeIndexAggregatorMinMax::MergeTreeIndexAggregatorMinMax(const MergeTreeIndexMinMax & index)
    : index(index) {}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorMinMax::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index, std::move(parallelogram));
}

void MergeTreeIndexAggregatorMinMax::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    Field field_min;
    Field field_max;
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & column = block.getByName(index.columns[i]).column;
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


MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const SelectQueryInfo &query,
    const Context &context,
    const MergeTreeIndexMinMax &index)
    : IMergeTreeIndexCondition(), index(index), condition(query, context, index.columns, index.expr) {}

bool MergeTreeIndexConditionMinMax::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexConditionMinMax::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleMinMax> granule
        = std::dynamic_pointer_cast<MergeTreeIndexGranuleMinMax>(idx_granule);
    if (!granule)
        throw Exception(
            "Minmax index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    for (const auto & range : granule->parallelogram)
        if (range.left.isNull() || range.right.isNull())
            return true;
    return condition.checkInParallelogram(granule->parallelogram, index.data_types).can_be_true;
}


MergeTreeIndexGranulePtr MergeTreeIndexMinMax::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(*this);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexMinMax::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorMinMax>(*this);
}


MergeTreeIndexConditionPtr MergeTreeIndexMinMax::createIndexCondition(
    const SelectQueryInfo & query, const Context & context) const
{
    return std::make_shared<MergeTreeIndexConditionMinMax>(query, context, *this);
};

bool MergeTreeIndexMinMax::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    const String column_name = node->getColumnName();

    for (const auto & name : columns)
        if (column_name == name)
            return true;

    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
        if (func->arguments->children.size() == 1)
            return mayBenefitFromIndexForIn(func->arguments->children.front());

    return false;
}

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

    return std::make_unique<MergeTreeIndexMinMax>(
        node->name, std::move(minmax_expr), columns, data_types, sample, node->granularity);
}

}
