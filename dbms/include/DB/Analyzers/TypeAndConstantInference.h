#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/DataTypes/IDataType.h>
#include <unordered_map>


namespace DB
{

class Context;
class WriteBuffer;
class CollectAliases;
class AnalyzeColumns;


/** For every expression, deduce its type,
  *  and if it is a constant expression, calculate its value.
  *
  * Types and constants inference goes together,
  *  because sometimes resulting type of a function depend on value of constant expression.
  * Notable examples: tupleElement(tuple, N) and toFixedString(s, N) functions.
  */
struct TypeAndConstantInference
{
	void process(ASTPtr & ast, Context & context, CollectAliases & aliases, const AnalyzeColumns & columns);

	struct ExpressionInfo
	{
		ASTPtr node;
		DataTypePtr data_type;
		bool is_constant_expression = false;
		Field value;	/// Has meaning if is_constant_expression == true.
	};

	/// Key is getColumnName of AST node.
	using Info = std::unordered_map<String, ExpressionInfo>;
	Info info;

	/// Debug output
	void dump(WriteBuffer & out) const;
};

}
