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


MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(const MergeTreeIndexMinMax & index_)
    : index(index_) {}

MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(
    const MergeTreeIndexMinMax & index_, std::vector<Range> && hyperrectangle_)
    : index(index_), hyperrectangle(std::move(hyperrectangle_)) {}

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
            type->serializeBinary(hyperrectangle[i].left, ostr);
            type->serializeBinary(hyperrectangle[i].right, ostr);
        }
        else
        {
            bool is_null = hyperrectangle[i].left.isNull() || hyperrectangle[i].right.isNull(); // one is enough
            writeBinary(is_null, ostr);
            if (!is_null)
            {
                type->serializeBinary(hyperrectangle[i].left, ostr);
                type->serializeBinary(hyperrectangle[i].right, ostr);
            }
        }
    }
}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & istr)
{
    hyperrectangle.clear();
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
        hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
}


MergeTreeIndexAggregatorMinMax::MergeTreeIndexAggregatorMinMax(const MergeTreeIndexMinMax & index_)
    : index(index_) {}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorMinMax::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index, std::move(hyperrectangle));
}

void MergeTreeIndexAggregatorMinMax::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    FieldRef field_min;
    FieldRef field_max;
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & column = block.getByName(index.columns[i]).column;
        column->cut(*pos, rows_read)->getExtremes(field_min, field_max);

        if (hyperrectangle.size() <= i)
        {
            hyperrectangle.emplace_back(field_min, true, field_max, true);
        }
        else
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, field_min);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, field_max);
        }
    }

    *pos += rows_read;
}


MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const SelectQueryInfo &query,
    const Context &context,
    const MergeTreeIndexMinMax &index_)
    : index(index_), condition(query, context, index.columns, index.expr) {}

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
    for (const auto & range : granule->hyperrectangle)
        if (range.left.isNull() || range.right.isNull())
            return true;
    return condition.checkInHyperrectangle(granule->hyperrectangle, index.data_types).can_be_true;
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

    for (const auto & cname : columns)
        if (column_name == cname)
            return true;

    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
        if (func->arguments->children.size() == 1)
            return mayBenefitFromIndexForIn(func->arguments->children.front());

    return false;
}

std::unique_ptr<IMergeTreeIndex> minmaxIndexCreator(
    const NamesAndTypesList & new_columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context,
    bool /*attach*/)
{
    if (node->name.empty())
        throw Exception("Index must have unique name", ErrorCodes::INCORRECT_QUERY);

    if (node->type->arguments)
        throw Exception("Minmax index have not any arguments", ErrorCodes::INCORRECT_QUERY);

    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());
    auto syntax = SyntaxAnalyzer(context).analyze(expr_list, new_columns);
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
