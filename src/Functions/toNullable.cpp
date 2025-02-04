#include <Functions/toNullable.h>

#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <memory>

namespace DB
{

FunctionPtr FunctionToNullable::create(ContextPtr)
{
    return std::make_shared<FunctionToNullable>();
}

std::string FunctionToNullable::getName() const { return name; }

size_t FunctionToNullable::getNumberOfArguments() const { return 1; }

bool FunctionToNullable::useDefaultImplementationForNulls() const { return false; }

bool FunctionToNullable::useDefaultImplementationForNothing() const { return false; }

bool FunctionToNullable::useDefaultImplementationForConstants() const { return true; }

bool FunctionToNullable::isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const { return false; }

DataTypePtr FunctionToNullable::getReturnTypeImpl(const DataTypes & arguments) const { return makeNullable(arguments[0]); }

ColumnPtr FunctionToNullable::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const
{
    return makeNullable(arguments[0].column);
}

REGISTER_FUNCTION(ToNullable)
{
    factory.registerFunction<FunctionToNullable>();
}

}
