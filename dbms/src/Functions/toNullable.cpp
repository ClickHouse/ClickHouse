#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Core/ColumnNumbers.h>


namespace DB
{

/// If value is not Nullable or NULL, wraps it to Nullable.
class FunctionToNullable : public IFunction
{
public:
    static constexpr auto name = "toNullable";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionToNullable>();
    }

    std::string getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    String getSignature() const override { return "f(T) -> Nullable(T)"; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        block.getByPosition(result).column = makeNullable(block.getByPosition(arguments[0]).column);
    }
};


void registerFunctionToNullable(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToNullable>();
}

}
