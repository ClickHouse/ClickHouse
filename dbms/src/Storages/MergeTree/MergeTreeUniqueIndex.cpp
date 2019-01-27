#include <Storages/MergeTree/MergeTreeUniqueIndex.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
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
        : MergeTreeIndexGranule(), index(index), set(new Set(SizeLimits{}, true))
{
    set->setHeader(index.header);
}

void MergeTreeUniqueGranule::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
                "Attempt to write empty unique index `" + index.name + "`", ErrorCodes::LOGICAL_ERROR);
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "serializeBinary Granule");

    const auto & columns = set->getSetElements();

    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    size_type->serializeBinary(size(), ostr);

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & type = index.data_types[i];

        type->serializeBinaryBulk(*columns[i], ostr, 0, size());
    }
}

void MergeTreeUniqueGranule::deserializeBinary(ReadBuffer & istr)
{
    Poco::Logger * log = &Poco::Logger::get("unique_idx");

    LOG_DEBUG(log, "deserializeBinary Granule");
    if (!set->empty())
    {
        auto new_set = std::make_unique<Set>(SizeLimits{}, true);
        set.swap(new_set);
    }

    Block block;

    Field field_rows;
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    size_type->deserializeBinary(field_rows, istr);
    size_t rows_to_read = field_rows.get<size_t>();

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & type = index.data_types[i];

        auto new_column = type->createColumn();
        type->deserializeBinaryBulk(*new_column, istr, rows_to_read, 0);

        block.insert(ColumnWithTypeAndName(new_column->getPtr(), type, index.columns[i]));
    }

    set->insertFromBlock(block);
}

String MergeTreeUniqueGranule::toString() const
{
    String res = "unique granule:\n";

    const auto & columns = set->getSetElements();

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & column = columns[i];
        res += " [";
        for (size_t j = 0; j < column->size(); ++j)
        {
            if (j != 0)
                res += ", ";
            Field field;
            column->get(j, field);
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
    size_t rows_read = std::min(limit, new_block.rows() - *pos);

    if (index.max_rows && size() > index.max_rows)
    {
        *pos += rows_read;
        return;
    }

    Block key_block;
    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & name = index.columns[i];
        const auto & type = index.data_types[i];
        key_block.insert(
                ColumnWithTypeAndName(
                    new_block.getByName(name).column->cut(*pos, rows_read),
                    type,
                    name));
    }

    set->insertFromBlock(key_block);

    LOG_DEBUG(log, "unique rows: " << set->getTotalRowCount());

    *pos += rows_read;
}

Block MergeTreeUniqueGranule::getElementsBlock() const
{
    if (index.max_rows && size() > index.max_rows)
        return index.header;
    return index.header.cloneWithColumns(set->getSetElements());
}


UniqueCondition::UniqueCondition(
        const SelectQueryInfo & query,
        const Context & context,
        const MergeTreeUniqueIndex &index)
        : IndexCondition(), index(index)
{
    for (size_t i = 0, size = index.columns.size(); i < size; ++i)
    {
        std::string name = index.columns[i];
        if (!key_columns.count(name))
            key_columns[name] = i;
    }

    const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery &>(*query.query);

    /// Replace logical functions with bit functions.
    /// Working with UInt8: last bit -- can be true, previous -- can be false.
    ASTPtr new_expression;
    if (select.where_expression && select.prewhere_expression)
        new_expression = makeASTFunction(
                "and",
                select.where_expression->clone(),
                select.prewhere_expression->clone());
    else if (select.where_expression)
        new_expression = select.where_expression->clone();
    else if (select.prewhere_expression)
        new_expression = select.prewhere_expression->clone();
    else
        /// 11_2 -- can be true and false at the same time
        new_expression = std::make_shared<ASTLiteral>(Field(3));

    new_expression = makeASTFunction(
            "bitAnd",
            new_expression,
            std::make_shared<ASTLiteral>(Field(1)));

    traverseAST(new_expression, context);
}

bool UniqueCondition::alwaysUnknownOrTrue() const
{
    return false;
}

bool UniqueCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    auto granule = std::dynamic_pointer_cast<MergeTreeUniqueGranule>(idx_granule);
    if (!granule)
        throw Exception(
                "Unique index condition got wrong granule", ErrorCodes::LOGICAL_ERROR);

    return true;
}

void UniqueCondition::traverseAST(ASTPtr & node, const Context & context)
{
    if (ASTFunction * func = typeid_cast<ASTFunction *>(&*node))
    {
        if (operatorFromAST(func)) {
            auto & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;

            for (size_t i = 0, size = args.size(); i < size; ++i)
                traverseAST(args[i], context);
            return;
        }
    }

    if (!atomFromAST(node, context))
        *node = ASTLiteral(Field(3)); /// Unknown
}

bool UniqueCondition::termFromAST(const ASTPtr & node, const Context & context)
{
    /// Function, literal or column

    if (const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(node.get()))
        return true;

    if (const ASTIdentifier * identifier = typeid_cast<const ASTIdentifier *>(node.get()))
        return key_columns.count(identifier->name) != 0;

    if (ASTFunction * func = typeid_cast<ASTFunction *>(&*node)) {
        if (key_columns.count(func->name) != 0)
            return true;

        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        for (size_t i = 0, size = args.size(); i < size; ++i)
            if (!termFromAST(args[i], context))
                return false;

        return true;
    }

    return false;
}

bool UniqueCondition::atomFromAST(const ASTPtr & node, const Context & context)
{
    /// Functions < > = != <= >= in `notIn`
    if (termFromAST(node, context))
        return true;

    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        for (size_t i = 0, size = args.size(); i < size; ++i)
            if (!termFromAST(args[i], context))
                return false;

        return true;
    }

    return false;
}

bool UniqueCondition::operatorFromAST(ASTFunction * func)
{
    /// Functions AND, OR, NOT. Replace with bit*.
    const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

    if (func->name == "not")
    {
        if (args.size() != 1)
            return false;
        func->name = "bitNot";
    }
    else if (func->name == "and" || func->name == "indexHint")
        func->name = "bitAnd";
    else if (func->name == "or")
        func->name = "bitOR";
    else
        return false;

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

    Block header;

    Names columns;
    DataTypes data_types;

    Poco::Logger * log = &Poco::Logger::get("unique_idx");
    LOG_DEBUG(log, "new unique index" << node->name);
    for (size_t i = 0; i < expr_list->children.size(); ++i)
    {
        const auto & column = sample.getByPosition(i);

        columns.emplace_back(column.name);
        data_types.emplace_back(column.type);

        header.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
        LOG_DEBUG(log, ">" << column.name << " " << column.type->getName());
    }

    return std::make_unique<MergeTreeUniqueIndex>(
            node->name, std::move(unique_expr), columns, data_types, header, node->granularity.get<size_t>(), max_rows);;
}

}