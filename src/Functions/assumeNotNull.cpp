#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{
namespace
{

/// Implements the function assumeNotNull which takes 1 argument and works as follows:
/// - if the argument is a nullable column, return its embedded column;
/// - otherwise return the original argument.
/// NOTE: assumeNotNull may not be called with the NULL value.
class FunctionAssumeNotNull : public IFunction
{
public:
    static constexpr auto name = "assumeNotNull";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionAssumeNotNull>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return removeNullable(arguments[0]);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnPtr & col = arguments[0].column;

        if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(*col))
            return nullable_col->getNestedColumnPtr();
        else
            return col;
    }
};

}

void registerFunctionAssumeNotNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAssumeNotNull>();
}

}
