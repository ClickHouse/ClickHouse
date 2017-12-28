#pragma once

#include <Core/NamesAndTypes.h>
#include <Columns/IColumnDummy.h>


namespace DB
{

class ExpressionActions;

/** A column containing a lambda expression.
  * Behaves like a constant-column. Contains an expression, but not input or output data.
  */
class ColumnExpression final : public COWPtrHelper<IColumnDummy, ColumnExpression>
{
private:
    friend class COWPtrHelper<IColumnDummy, ColumnExpression>;

    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

    ColumnExpression(size_t s_, const ExpressionActionsPtr & expression_, const NamesAndTypesList & arguments_, const DataTypePtr & return_type_, const String & return_name_);
    ColumnExpression(const ColumnExpression &) = default;

public:
    const char * getFamilyName() const override { return "Expression"; }
    MutableColumnPtr cloneDummy(size_t s_) const override;

    const ExpressionActionsPtr & getExpression() const;
    const DataTypePtr & getReturnType() const;
    const std::string & getReturnName() const;
    const NamesAndTypesList & getArguments() const;

private:
    ExpressionActionsPtr expression;
    NamesAndTypesList arguments;
    DataTypePtr return_type;
    std::string return_name;
};

}
