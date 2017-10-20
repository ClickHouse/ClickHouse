#include <Core/Block.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


std::pair<Field, std::shared_ptr<IDataType>> evaluateConstantExpression(const ASTPtr & node, const Context & context)
{
    ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(
        node, context, nullptr, NamesAndTypesList{{ "_dummy", std::make_shared<DataTypeUInt8>() }}).getConstActions();

    /// There must be at least one column in the block so that it knows the number of rows.
    Block block_with_constants{{ std::make_shared<ColumnConst>(std::make_shared<ColumnUInt8>(1, 0), 1), std::make_shared<DataTypeUInt8>(), "_dummy" }};

    expr_for_constant_folding->execute(block_with_constants);

    if (!block_with_constants || block_with_constants.rows() == 0)
        throw Exception("Logical error: empty block after evaluation of constant expression for IN or VALUES", ErrorCodes::LOGICAL_ERROR);

    String name = node->getColumnName();

    if (!block_with_constants.has(name))
        throw Exception("Element of set in IN or VALUES is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

    const ColumnWithTypeAndName & result = block_with_constants.getByName(name);
    const IColumn & result_column = *result.column;

    if (!result_column.isConst())
        throw Exception("Element of set in IN or VALUES is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

    return std::make_pair(result_column[0], result.type);
}


ASTPtr evaluateConstantExpressionAsLiteral(const ASTPtr & node, const Context & context)
{
    if (typeid_cast<const ASTLiteral *>(node.get()))
        return node;

    return std::make_shared<ASTLiteral>(node->range,
        evaluateConstantExpression(node, context).first);
}


ASTPtr evaluateConstantExpressionOrIdentifierAsLiteral(const ASTPtr & node, const Context & context)
{
    if (auto id = typeid_cast<const ASTIdentifier *>(node.get()))
        return std::make_shared<ASTLiteral>(node->range, Field(id->name));

    return evaluateConstantExpressionAsLiteral(node, context);
}


bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result)
{
    IParser::Pos begin = pos;
    ASTPtr res;

    if (!ParserIdentifier().parse(pos, res, expected))
    {
        pos = begin;
        if (!ParserStringLiteral().parse(pos, res, expected))
            return false;

        result = typeid_cast<const ASTLiteral &>(*res).value.safeGet<String>();
    }
    else
        result = typeid_cast<const ASTIdentifier &>(*res).name;

    return true;
}

}
