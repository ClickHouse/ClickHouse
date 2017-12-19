#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnExpression.h>


namespace DB
{

ColumnExpression::ColumnExpression(
    size_t s_, const ExpressionActionsPtr & expression_, const NamesAndTypes & arguments_, const DataTypePtr & return_type_, const String & return_name_)
    : expression(expression_), arguments(arguments_), return_type(return_type_), return_name(return_name_)
{
    s = s_;
}

ColumnExpression::ColumnExpression(
    size_t s_, const ExpressionActionsPtr & expression_, const NamesAndTypesList & arguments_, const DataTypePtr & return_type_, const String & return_name_)
    : expression(expression_), arguments(arguments_.begin(), arguments_.end()), return_type(return_type_), return_name(return_name_)
{
    s = s_;
}

MutableColumnPtr ColumnExpression::cloneDummy(size_t s_) const
{
    return ColumnExpression::create(s_, expression, arguments, return_type, return_name);
}

const ExpressionActionsPtr & ColumnExpression::getExpression() const { return expression; }
const DataTypePtr & ColumnExpression::getReturnType() const { return return_type; }
const std::string & ColumnExpression::getReturnName() const { return return_name; }
const NamesAndTypes & ColumnExpression::getArguments() const { return arguments; }

Names ColumnExpression::getArgumentNames() const
{
    Names res(arguments.size());
    for (size_t i = 0; i < arguments.size(); ++i)
        res[i] = arguments[i].name;
    return res;
}

}
