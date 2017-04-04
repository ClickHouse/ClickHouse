#pragma once

#include <Core/NamesAndTypes.h>
#include <Columns/IColumnDummy.h>


namespace DB
{

class ExpressionActions;

/** A column containing a lambda expression.
  * Behaves like a constant-column. Contains an expression, but not input or output data.
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
