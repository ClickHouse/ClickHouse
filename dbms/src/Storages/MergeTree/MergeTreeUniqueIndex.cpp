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

    const auto & columns = set->getSetElements();
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());

    if (index.max_rows && size() > index.max_rows)
    {
        size_type->serializeBinary(0, ostr);
        return;
    }

    size_type->serializeBinary(size(), ostr);

    for (size_t i = 0; i < index.columns.size(); ++i)
    {
        const auto & type = index.data_types[i];
        type->serializeBinaryBulk(*columns[i], ostr, 0, size());
    }
}

void MergeTreeUniqueGranule::deserializeBinary(ReadBuffer & istr)
{
    if (!set->empty())
    {
        auto new_set = std::make_unique<Set>(SizeLimits{}, true);
        new_set->setHeader(index.header);
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
    String res = "";

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
            key_columns.insert(name);
    }

    const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery &>(*query.query);

    /// Replace logical functions with bit functions.
    /// Working with UInt8: last bit = can be true, previous = can be false.
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
        /// 0b11 -- can be true and false at the same time
        new_expression = std::make_shared<ASTLiteral>(Field(3));

    useless = checkASTAlwaysUnknownOrTrue(new_expression);
    /// Do not proceed if index is useless for this query.
    if (useless)
        return;

    expression_ast = makeASTFunction(
            "bitAnd",
            new_expression,
            std::make_shared<ASTLiteral>(Field(1)));

    traverseAST(expression_ast);

    auto syntax_analyzer_result = SyntaxAnalyzer(context, {}).analyze(
            expression_ast, index.header.getNamesAndTypesList());
    actions = ExpressionAnalyzer(expression_ast, syntax_analyzer_result, context).getActions(true);
}

bool UniqueCondition::alwaysUnknownOrTrue() const
{
    return useless;
}

bool UniqueCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    auto granule = std::dynamic_pointer_cast<MergeTreeUniqueGranule>(idx_granule);
    if (!granule)
        throw Exception(
                "Unique index condition got wrong granule", ErrorCodes::LOGICAL_ERROR);

    if (useless)
        return true;

    if (index.max_rows && granule->size() > index.max_rows)
        return true;

    Block result = granule->getElementsBlock();
    actions->execute(result);


    const auto & column = result.getByName(expression_ast->getColumnName()).column;

    for (size_t i = 0; i < column->size(); ++i)
        if (column->getBool(i))
            return true;

    return false;
}

void UniqueCondition::traverseAST(ASTPtr & node) const
{
    if (operatorFromAST(node))
    {
        auto * func = typeid_cast<ASTFunction *>(&*node);
        auto & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;

        for (auto & arg : args)
            traverseAST(arg);
        return;
    }

    if (!atomFromAST(node))
        node = std::make_shared<ASTLiteral>(Field(3)); /// can_be_true=1 can_be_false=1
}

bool UniqueCondition::atomFromAST(ASTPtr & node) const
{
    /// Function, literal or column

    if (typeid_cast<const ASTLiteral *>(node.get()))
        return true;

    if (const auto * identifier = typeid_cast<const ASTIdentifier *>(node.get()))
        return key_columns.count(identifier->getColumnName()) != 0;

    if (auto * func = typeid_cast<ASTFunction *>(node.get()))
    {
        if (key_columns.count(func->getColumnName()))
        {
            /// Function is already calculated.
            node = std::make_shared<ASTIdentifier>(func->getColumnName());
            return true;
        }

        ASTs & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;

        for (auto & arg : args)
            if (!atomFromAST(arg))
                return false;

        return true;
    }

    return false;
}

bool UniqueCondition::operatorFromAST(ASTPtr & node) const
{
    /// Functions AND, OR, NOT. Replace with bit*.
    auto * func = typeid_cast<ASTFunction *>(&*node);
    if (!func)
        return false;

    const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

    if (func->name == "not")
    {
        if (args.size() != 1)
            return false;

        func->name = "__bitSwapLastTwo";
    }
    else if (func->name == "and" || func->name == "indexHint")
        func->name = "bitAnd";
    else if (func->name == "or")
        func->name = "bitOr";
    else
        return false;

    return true;
}

bool checkAtomName(const String & name)
{
    static std::set<String> atoms = {
            "notEquals",
            "equals",
            "less",
            "greater",
            "lessOrEquals",
            "greaterOrEquals",
            "in",
            "notIn",
            "like"
            };
    return atoms.find(name) != atoms.end();
}

bool UniqueCondition::checkASTAlwaysUnknownOrTrue(const ASTPtr & node, bool atomic) const
{
    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        if (key_columns.count(func->getColumnName()))
            return false;

        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        if (func->name == "and" || func->name == "indexHint")
            return checkASTAlwaysUnknownOrTrue(args[0], atomic) && checkASTAlwaysUnknownOrTrue(args[1], atomic);
        else if (func->name == "or")
            return checkASTAlwaysUnknownOrTrue(args[0], atomic) || checkASTAlwaysUnknownOrTrue(args[1], atomic);
        else if (func->name == "not")
            return checkASTAlwaysUnknownOrTrue(args[0], atomic);
        else if (!atomic && checkAtomName(func->name))
            return checkASTAlwaysUnknownOrTrue(node, true);
        else
            return std::any_of(args.begin(), args.end(),
                    [this, &atomic](const auto & arg) { return checkASTAlwaysUnknownOrTrue(arg, atomic); });
    }
    else if (const auto * literal = typeid_cast<const ASTLiteral *>(node.get()))
        return !atomic && literal->value.get<bool>();
    else if (const auto * identifier = typeid_cast<const ASTIdentifier *>(node.get()))
        return key_columns.find(identifier->getColumnName()) == key_columns.end();
    else
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
        const NamesAndTypesList & new_columns,
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
            expr_list, new_columns);
    auto unique_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);

    auto sample = ExpressionAnalyzer(expr_list, syntax, context)
            .getActions(true)->getSampleBlock();

    Block header;

    Names columns;
    DataTypes data_types;

    for (size_t i = 0; i < expr_list->children.size(); ++i)
    {
        const auto & column = sample.getByPosition(i);

        columns.emplace_back(column.name);
        data_types.emplace_back(column.type);

        header.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
    }

    return std::make_unique<MergeTreeUniqueIndex>(
        node->name, std::move(unique_expr), columns, data_types, header, node->granularity.get<size_t>(), max_rows);
}

}
