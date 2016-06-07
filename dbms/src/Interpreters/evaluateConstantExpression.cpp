#include <DB/Core/Block.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Parsers/IAST.h>
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

/** Выполнить константное выражение (для элемента множества в IN). Весьма неоптимально. */
Field evaluateConstantExpression(ASTPtr & node, const Context & context)
{
	ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(
		node, context, nullptr, NamesAndTypesList{{ "_dummy", new DataTypeUInt8 }}).getConstActions();

	/// В блоке должен быть хотя бы один столбец, чтобы у него было известно число строк.
	Block block_with_constants{{ new ColumnConstUInt8(1, 0), new DataTypeUInt8, "_dummy" }};

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

}
