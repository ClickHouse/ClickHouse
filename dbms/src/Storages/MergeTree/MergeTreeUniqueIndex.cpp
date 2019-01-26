#include <Storages/MergeTree/MergeTreeUniqueIndex.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Parsers/ASTLiteral.h>

#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

MergeTreeUniqueGranule::MergeTreeUniqueGranule(const MergeTreeUniqueIndex & index)
        : MergeTreeIndexGranule(), index(index), block()
{
}

void MergeTreeUniqueGranule::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
                "Attempt to write empty unique index `" + index.name + "`", ErrorCodes::LOGICAL_ERROR);
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "serializeBinary Granule");

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        type->serializeBinaryBulk(*block.getByPosition(i).column, ostr, 0, 0);
    }
}

void MergeTreeUniqueGranule::deserializeBinary(ReadBuffer & istr)
{
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "deserializeBinary Granule");
    block.clear();
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const DataTypePtr & type = index.data_types[i];

        auto new_column = type->createColumn();
        type->deserializeBinaryBulk(*new_column, istr, 0, 0);

        block.insert(ColumnWithTypeAndName(new_column->getPtr(), type, index.columns[i]));
    }
}

String MergeTreeUniqueGranule::toString() const
{
    String res = "unique granule:\n";

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = block.getByPosition(i);
        res += column.name;
        res += " [";
        for (size_t j = 0; j < column.column->size(); ++j)
        {
            if (j != 0)
                res += ", ";
            Field field;
            column.column->get(j, field);
            res += applyVisitor(FieldVisitorToString(), field);
        }
        res += "]\n";
    }

    return res;
}

void MergeTreeUniqueGranule::update(const Block & new_block, size_t * pos, size_t limit)
{
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "update Granule " << new_block.columns()
                                     << " pos: "<< *pos << " limit: " << limit << " rows: " << new_block.rows());

    size_t block_size = new_block.getByPosition(0).column->size();

    if (!block.columns())
    {
        for (size_t i = 0; i < index.columns.size(); ++i)
        {
            const DataTypePtr & type = index.data_types[i];
            block.insert(ColumnWithTypeAndName(type->createColumn(), type, index.columns[i]));
        }
    }

    for (size_t cur = 0; cur < limit && cur + *pos < block_size; ++cur)
    {
        // TODO
        ++(*pos);
    }
};

UniqueCondition::UniqueCondition(
        const SelectQueryInfo &,
        const Context &,
        const MergeTreeUniqueIndex &index)
        : IndexCondition(), index(index) {};

bool UniqueCondition::alwaysUnknownOrTrue() const
{
    return true;
}

bool UniqueCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    auto granule = std::dynamic_pointer_cast<MergeTreeUniqueGranule>(idx_granule);
    if (!granule)
        throw Exception(
                "Unique index condition got wrong granule", ErrorCodes::LOGICAL_ERROR);

    return true;
}


MergeTreeIndexGranulePtr MergeTreeUniqueIndex::createIndexGranule() const
{
    return std::make_shared<MergeTreeUniqueGranule>(*this);
}

IndexConditionPtr MergeTreeUniqueIndex::createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const
{
    return std::make_shared<UniqueCondition>(query, context, *this);
};


std::unique_ptr<MergeTreeIndex> MergeTreeUniqueIndexCreator(
        const MergeTreeData & data,
        std::shared_ptr<ASTIndexDeclaration> node,
        const Context & context)
{
    if (node->name.empty())
        throw Exception("Index must have unique name", ErrorCodes::INCORRECT_QUERY);

    size_t max_rows = 0;
    if (node->type->arguments)
    {
        if (node->type->arguments->children.size() > 1)
            throw Exception("Unique index cannot have only 0 or 1 argument", ErrorCodes::INCORRECT_QUERY);
        else if (node->type->arguments->children.size() == 1)
            max_rows = typeid_cast<const ASTLiteral &>(
                    *node->type->arguments->children[0]).value.get<size_t>();
    }


    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());
    auto syntax = SyntaxAnalyzer(context, {}).analyze(
            expr_list, data.getColumns().getAllPhysical());
    auto unique_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);

    auto sample = ExpressionAnalyzer(expr_list, syntax, context)
            .getActions(true)->getSampleBlock();

    Names columns;
    DataTypes data_types;

    Poco::Logger * log = &Poco::Logger::get("unique_idx");
    LOG_DEBUG(log, "new unique index" << node->name);
    for (size_t i = 0; i < expr_list->children.size(); ++i)
    {
        const auto & column = sample.getByPosition(i);

        columns.emplace_back(column.name);
        data_types.emplace_back(column.type);
        LOG_DEBUG(log, ">" << column.name << " " << column.type->getName());
    }

    return std::make_unique<MergeTreeUniqueIndex>(
            node->name, std::move(unique_expr), columns, data_types, node->granularity.get<size_t>(), max_rows);;
}

}