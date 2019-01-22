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
    : MergeTreeIndexGranule(), index(index), parallelogram()
{
}

void MergeTreeMinMaxGranule::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
                "Attempt to write empty minmax index `" + index.name + "`", ErrorCodes::LOGICAL_ERROR);
    Poco::Logger * log = &Poco::Logger::get("minmax_idx");

    LOG_DEBUG(log, "serializeBinary Granule");

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        LOG_DEBUG(log, "parallel " << i << " :: "
            << applyVisitor(FieldVisitorToString(), parallelogram[i].left) << " "
            << applyVisitor(FieldVisitorToString(), parallelogram[i].right));

        type->serializeBinary(parallelogram[i].left, ostr);
        type->serializeBinary(parallelogram[i].right, ostr);
    }
}

void MergeTreeMinMaxGranule::deserializeBinary(ReadBuffer & istr)
{
    Poco::Logger * log = &Poco::Logger::get("minmax_idx");

    LOG_DEBUG(log, "deserializeBinary Granule");
    parallelogram.clear();
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        Field min_val;
        type->deserializeBinary(min_val, istr);
        Field max_val;
        type->deserializeBinary(max_val, istr);

        LOG_DEBUG(log, "parallel " << i << " :: "
            << applyVisitor(FieldVisitorToString(), min_val) << " "
            << applyVisitor(FieldVisitorToString(), max_val));

        parallelogram.emplace_back(min_val, true, max_val, true);
    }
}

String MergeTreeMinMaxGranule::toString() const
{
    String res = "minmax granule: ";

    for (size_t i = 0; i < parallelogram.size(); ++i)
    {
        res += "["
                + applyVisitor(FieldVisitorToString(), parallelogram[i].left) + ", "
                + applyVisitor(FieldVisitorToString(), parallelogram[i].right) + "]";
    }

    return res;
}

void MergeTreeMinMaxGranule::update(const Block & block, size_t * pos, size_t limit)
{
    Poco::Logger * log = &Poco::Logger::get("minmax_idx");

    LOG_DEBUG(log, "update Granule " << parallelogram.size()
    << " pos: "<< *pos << " limit: " << limit << " rows: " << block.rows());

    size_t rows_read = 0;
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        LOG_DEBUG(log, "granule column: " << index.columns[i]);

        auto column = block.getByName(index.columns[i]).column;
        size_t cur;
        /// TODO: more effective (index + getExtremes??)
        for (cur = 0; cur < limit && cur + *pos < column->size(); ++cur)
        {
            Field field;
            column->get(cur + *pos, field);
            LOG_DEBUG(log, "upd:: " << applyVisitor(FieldVisitorToString(), field));
            if (parallelogram.size() <= i)
            {
                LOG_DEBUG(log, "emplaced");
                parallelogram.emplace_back(field, true, field, true);
            }
            else
            {
                parallelogram[i].left = std::min(parallelogram[i].left, field);
                parallelogram[i].right = std::max(parallelogram[i].right, field);
            }
        }
        LOG_DEBUG(log, "res:: ["
            << applyVisitor(FieldVisitorToString(), parallelogram[i].left) << ", "
            << applyVisitor(FieldVisitorToString(), parallelogram[i].right) << "]");
        rows_read = cur;
    }
    LOG_DEBUG(log, "updated rows_read: " << rows_read);

    *pos += rows_read;
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

    auto sample = ExpressionAnalyzer(expr_list, syntax, context)
            .getActions(true)->getSampleBlock();

    Names columns;
    DataTypes data_types;

    Poco::Logger * log = &Poco::Logger::get("minmax_idx");
    LOG_DEBUG(log, "new minmax index" << node->name);
    for (size_t i = 0; i < expr_list->children.size(); ++i)
    {
        const auto & column = sample.getByPosition(i);

        columns.emplace_back(column.name);
        data_types.emplace_back(column.type);
        LOG_DEBUG(log, ">" << column.name << " " << column.type->getName());
    }

    return std::make_unique<MergeTreeMinMaxIndex>(
            node->name, std::move(minmax_expr), columns, data_types, node->granularity.get<size_t>());;
}

}