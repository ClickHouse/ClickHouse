#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/ArrayJoinAction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_IS_SPECIAL;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** arrayJoin(arr) - a special function - it can not be executed directly;
  *                     is used only to get the result type of the corresponding expression.
  */
class FunctionArrayJoin : public IFunction
{
public:
    static constexpr auto name = "arrayJoin";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionArrayJoin>();
    }


    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    /** It could return many different values for single argument. */
    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & arr = getArrayJoinDataType(arguments[0]);
        if (!arr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be Array or Map", getName());
        return arr->getNestedType();

    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        throw Exception(ErrorCodes::FUNCTION_IS_SPECIAL, "Function {} must not be executed directly.", getName());
    }

    /// Because of function cannot be executed directly.
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
};


REGISTER_FUNCTION(ArrayJoin)
{
    factory.registerFunction<FunctionArrayJoin>();
}

}
