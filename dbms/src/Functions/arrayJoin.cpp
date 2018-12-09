#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>


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
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionArrayJoin>();
    }


    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    /** It could return many different values for single argument. */
    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    String getSignature() const override { return "f(Array(T)) -> T"; }

    void executeImpl(Block &, const ColumnNumbers &, size_t, size_t /*input_rows_count*/) override
    {
        throw Exception("Function " + getName() + " must not be executed directly.", ErrorCodes::FUNCTION_IS_SPECIAL);
    }

    /// Because of function cannot be executed directly.
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
};


void registerFunctionArrayJoin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayJoin>();
}

}
