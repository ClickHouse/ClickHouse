#pragma once

#include <DB/Columns/IColumnDummy.h>
#include <DB/Interpreters/Expression.h>


namespace DB
{
	
/** Столбец, содержащий лямбда-выражение.
  * Ведёт себя как столбец-константа. Содержит выражение, но не входные или выходные данные.
  */
class ColumnExpression : public IColumnDummy
{
public:
	ColumnExpression(size_t s_, ExpressionPtr expression_, const NamesAndTypes & arguments_, DataTypePtr return_type_)
		: IColumnDummy(s_), expression(expression_), arguments(arguments_), return_type(return_type_) {}
	
	std::string getName() const { return "ColumnExpression"; }
	ColumnPtr cloneDummy(size_t s_) const { return new ColumnExpression(s_, expression, arguments, return_type); }
	
	ExpressionPtr & getExpression() { return expression; }
	const NamesAndTypes & getArguments() const { return arguments; }
	const DataTypePtr & getReturnType() const { return return_type; }
	
private:
	ExpressionPtr expression;
	NamesAndTypes arguments;
	DataTypePtr return_type;
};
	
}
