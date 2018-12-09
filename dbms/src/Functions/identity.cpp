#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionIdentity : public IFunction
{
public:
    static constexpr auto name = "identity";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIdentity>();
    }

    String getName() const override
    {
        return name;
    }

    String getSignature() const override { return "f(T) -> T"; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
    }
};


void registerFunctionIdentity(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIdentity>();
}

}
