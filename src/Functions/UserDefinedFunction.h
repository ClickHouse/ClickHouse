#pragma once

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

class UserDefinedFunction;
using UserDefinedFunctionPtr = std::shared_ptr<UserDefinedFunction>;

class UserDefinedFunction : public IFunction
{
public:
    explicit UserDefinedFunction(ContextPtr context_);
    static UserDefinedFunctionPtr create(ContextPtr context);

    String getName() const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    void setName(const String & name_);
    void setFunctionCore(ASTPtr function_core_);

private:
    Block executeCore(const ColumnsWithTypeAndName & arguments) const;

private:
    String name;
    ASTPtr function_core;
    ContextPtr context;
};

}
