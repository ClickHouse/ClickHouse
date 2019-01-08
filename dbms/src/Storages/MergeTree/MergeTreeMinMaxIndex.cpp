#include <Storages/MergeTree/MergeTreeMinMaxIndex.h>


namespace DB
{

MergeTreeMinMaxGranule::MergeTreeMinMaxGranule(const MergeTreeMinMaxIndex & index)
    : MergeTreeIndexGranule(), emp(true), index(index)
{
    parallelogram.reserve(index.columns.size());
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
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        Field min_val;
        type->deserializeBinary(min_val, istr);
        Field max_val;
        type->deserializeBinary(max_val, istr);

        parallelogram.emplace_back(min_val, true, max_val, true);
    }
    emp = true;
}

void MergeTreeMinMaxGranule::update(const Block & block, size_t * pos, size_t limit)
{
    size_t rows_read = 0;
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        auto column = block.getByName(index.columns[i]).column;
        size_t cur;
        /// TODO: more effective (index + getExtremes??)
        for (cur = 0; cur < limit && cur + *pos < column->size(); ++cur)
        {
            Field field;
            column->get(i, field);
            if (parallelogram.size() < i)
            {
                parallelogram.emplace_back(field, true, field, true);
            }
            else
            {
                parallelogram[i].left = std::min(parallelogram[i].left, field);
                parallelogram[i].right = std::max(parallelogram[i].right, field);
            }
        }
        rows_read = cur;
    }

    *pos += rows_read;
    if (rows_read > 0)
        emp = false;
};


MinMaxCondition::MinMaxCondition(
    const SelectQueryInfo &query,
    const Context &context,
    const MergeTreeMinMaxIndex &index)
    : IndexCondition(), index(index), condition(query, context, index.columns, index.expr) {};

bool MinMaxCondition::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MinMaxCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeMinMaxGranule> granule
            = std::dynamic_pointer_cast<MergeTreeMinMaxGranule>(idx_granule);
    if (!granule) {
        throw Exception(
            "Minmax index condition got wrong granule", ErrorCodes::LOGICAL_ERROR);
    }

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


std::unique_ptr<MergeTreeIndex> MergeTreeMinMaxIndexCreator(
        const MergeTreeData & data,
        std::shared_ptr<ASTIndexDeclaration> node,
        const Context & context)
{
    if (node->name.empty())
        throw Exception("Index must have unique name", ErrorCodes::INCORRECT_QUERY);

    if (node->type->arguments)
        throw Exception("Minmax index have not any arguments", ErrorCodes::INCORRECT_QUERY);

    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());
    auto syntax = SyntaxAnalyzer(context, {}).analyze(
            expr_list, data.getColumns().getAllPhysical());
    auto minmax_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);


    auto minmax = std::make_unique<MergeTreeMinMaxIndex>(
            node->name, std::move(minmax_expr), node->granularity.get<size_t>());

    const auto & columns_with_types = minmax->expr->getRequiredColumnsWithTypes();

    for (const auto & column : columns_with_types)
    {
        minmax->columns.emplace_back(column.name);
        minmax->data_types.emplace_back(column.type);
    }

    return minmax;
}

}