#include <DB/Core/Block.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Interpreters/evaluateConstantExpression.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int BAD_ARGUMENTS;
}


Field evaluateConstantExpression(ASTPtr & node, const Context & context)
{
	ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(
		node, context, nullptr, NamesAndTypesList{{ "_dummy", std::make_shared<DataTypeUInt8>() }}).getConstActions();

	/// В блоке должен быть хотя бы один столбец, чтобы у него было известно число строк.
	Block block_with_constants{{ std::make_shared<ColumnConstUInt8>(1, 0), std::make_shared<DataTypeUInt8>(), "_dummy" }};

	expr_for_constant_folding->execute(block_with_constants);

	if (!block_with_constants || block_with_constants.rows() == 0)
		throw Exception("Logical error: empty block after evaluation constant expression for IN or VALUES", ErrorCodes::LOGICAL_ERROR);

	String name = node->getColumnName();

	if (!block_with_constants.has(name))
		throw Exception("Element of set in IN or VALUES is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

	const IColumn & result_column = *block_with_constants.getByName(name).column;

	if (!result_column.isConst())
		throw Exception("Element of set in IN or VALUES is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

	return result_column[0];
}


ASTPtr evaluateConstantExpressionAsLiteral(ASTPtr & node, const Context & context)
{
	if (typeid_cast<const ASTLiteral *>(node.get()))
		return node;

	return std::make_shared<ASTLiteral>(node->range,
		evaluateConstantExpression(node, context));
}


ASTPtr evaluateConstantExpressionOrIdentidierAsLiteral(ASTPtr & node, const Context & context)
{
	if (const ASTIdentifier * id = typeid_cast<const ASTIdentifier *>(node.get()))
		return std::make_shared<ASTLiteral>(node->range, Field(id->name));

	return evaluateConstantExpressionAsLiteral(node, context);
}

}
