#pragma once

#include <DB/Core/NamesAndTypes.h>
#include <DB/Columns/IColumnDummy.h>


namespace DB
{

class ExpressionActions;

/** Столбец, содержащий лямбда-выражение.
  * Ведёт себя как столбец-константа. Содержит выражение, но не входные или выходные данные.
  */
class ColumnExpression final : public IColumnDummy
{
private:
	using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
	ColumnExpression(size_t s_, ExpressionActionsPtr expression_, const NamesAndTypes & arguments_, DataTypePtr return_type_, String return_name_);
	ColumnExpression(size_t s_, ExpressionActionsPtr expression_, const NamesAndTypesList & arguments_, DataTypePtr return_type_, String return_name_);

	std::string getName() const override;
	ColumnPtr cloneDummy(size_t s_) const override;

	const ExpressionActionsPtr & getExpression() const;
	const DataTypePtr & getReturnType() const;
	const std::string & getReturnName() const;
	const NamesAndTypes & getArguments() const;
	Names getArgumentNames() const;

private:
	ExpressionActionsPtr expression;
	NamesAndTypes arguments;
	DataTypePtr return_type;
	std::string return_name;
};

}
