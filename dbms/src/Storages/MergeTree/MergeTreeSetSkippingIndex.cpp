#include <Storages/MergeTree/MergeTreeSetSkippingIndex.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

/// 0b11 -- can be true and false at the same time
const Field UNKNOWN_FIELD(3);


MergeTreeSetIndexGranule::MergeTreeSetIndexGranule(const MergeTreeSetSkippingIndex & index)
        : IMergeTreeIndexGranule(), index(index), set(new Set(SizeLimits{}, true))
{
    set->setHeader(index.header);
}

void MergeTreeSetIndexGranule::serializeBinary(WriteBuffer & ostr) const
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

void MergeTreeSetIndexGranule::deserializeBinary(ReadBuffer & istr)
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

void MergeTreeSetIndexGranule::update(const Block & new_block, size_t * pos, UInt64 limit)
{
    if (*pos >= new_block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(new_block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

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

Block MergeTreeSetIndexGranule::getElementsBlock() const
{
    if (index.max_rows && size() > index.max_rows)
        return index.header;
    return index.header.cloneWithColumns(set->getSetElements());
}


SetIndexCondition::SetIndexCondition(
        const SelectQueryInfo & query,
        const Context & context,
        const MergeTreeSetSkippingIndex &index)
        : IIndexCondition(), index(index)
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
    if (select.where_expression && select.prewhere_expression)
        expression_ast = makeASTFunction(
                "and",
                select.where_expression->clone(),
                select.prewhere_expression->clone());
    else if (select.where_expression)
        expression_ast = select.where_expression->clone();
    else if (select.prewhere_expression)
        expression_ast = select.prewhere_expression->clone();
    else
        expression_ast = std::make_shared<ASTLiteral>(UNKNOWN_FIELD);

    useless = checkASTUseless(expression_ast);
    /// Do not proceed if index is useless for this query.
    if (useless)
        return;

    traverseAST(expression_ast);

    auto syntax_analyzer_result = SyntaxAnalyzer(context, {}).analyze(
            expression_ast, index.header.getNamesAndTypesList());
    actions = ExpressionAnalyzer(expression_ast, syntax_analyzer_result, context).getActions(true);
}

bool SetIndexCondition::alwaysUnknownOrTrue() const
{
    return useless;
}

bool SetIndexCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    auto granule = std::dynamic_pointer_cast<MergeTreeSetIndexGranule>(idx_granule);
    if (!granule)
        throw Exception(
                "Unique index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    if (useless)
        return true;

    if (index.max_rows && granule->size() > index.max_rows)
        return true;

    Block result = granule->getElementsBlock();
    actions->execute(result);

    const auto & column = result.getByName(expression_ast->getColumnName()).column;

    for (size_t i = 0; i < column->size(); ++i)
        if (column->getInt(i) & 1)
            return true;

    return false;
}

void SetIndexCondition::traverseAST(ASTPtr & node) const
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
        node = std::make_shared<ASTLiteral>(UNKNOWN_FIELD);
}

bool SetIndexCondition::atomFromAST(ASTPtr & node) const
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

bool SetIndexCondition::operatorFromAST(ASTPtr & node) const
{
    /// Functions AND, OR, NOT. Replace with bit*.
    auto * func = typeid_cast<ASTFunction *>(&*node);
    if (!func)
        return false;

    ASTs & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;

    if (func->name == "not")
    {
        if (args.size() != 1)
            return false;

        func->name = "__bitSwapLastTwo";
    }
    else if (func->name == "and" || func->name == "indexHint")
    {
        auto last_arg = args.back();
        args.pop_back();

        ASTPtr new_func;
        if (args.size() > 1)
            new_func = makeASTFunction(
                    "bitAnd",
                    node,
                    last_arg);
        else
            new_func = makeASTFunction(
                    "bitAnd",
                    args.back(),
                    last_arg);

        node = new_func;
    }
    else if (func->name == "or")
    {
        auto last_arg = args.back();
        args.pop_back();

        ASTPtr new_func;
        if (args.size() > 1)
            new_func = makeASTFunction(
                    "bitOr",
                    node,
                    last_arg);
        else
            new_func = makeASTFunction(
                    "bitOr",
                    args.back(),
                    last_arg);

        node = new_func;
    }
    else
        return false;

    return true;
}

static bool checkAtomName(const String & name)
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

bool SetIndexCondition::checkASTUseless(const ASTPtr &node, bool atomic) const
{
    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        if (key_columns.count(func->getColumnName()))
            return false;

        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        if (func->name == "and" || func->name == "indexHint")
            return checkASTUseless(args[0], atomic) && checkASTUseless(args[1], atomic);
        else if (func->name == "or")
            return checkASTUseless(args[0], atomic) || checkASTUseless(args[1], atomic);
        else if (func->name == "not")
            return checkASTUseless(args[0], atomic);
        else if (!atomic && checkAtomName(func->name))
            return checkASTUseless(node, true);
        else
            return std::any_of(args.begin(), args.end(),
                    [this, &atomic](const auto & arg) { return checkASTUseless(arg, atomic); });
    }
    else if (const auto * literal = typeid_cast<const ASTLiteral *>(node.get()))
        return !atomic && literal->value.get<bool>();
    else if (const auto * identifier = typeid_cast<const ASTIdentifier *>(node.get()))
        return key_columns.find(identifier->getColumnName()) == key_columns.end();
    else
        return true;
}


MergeTreeIndexGranulePtr MergeTreeSetSkippingIndex::createIndexGranule() const
{
    return std::make_shared<MergeTreeSetIndexGranule>(*this);
}

IndexConditionPtr MergeTreeSetSkippingIndex::createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const
{
    return std::make_shared<SetIndexCondition>(query, context, *this);
};


std::unique_ptr<IMergeTreeIndex> setIndexCreator(
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

    return std::make_unique<MergeTreeSetSkippingIndex>(
        node->name, std::move(unique_expr), columns, data_types, header, node->granularity, max_rows);
}

}
