#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

//#if USE_EMBEDDED_COMPILER
#include <llvm/IR/Constants.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Type.h>
//#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionSomething : public IFunction
{
public:
    static constexpr auto name = "something";

//#if USE_EMBEDDED_COMPILER
    llvm::Value * compile(llvm::IRBuilderBase & builder, const DataTypes &, const ValuePlaceholders &) const override
    {
        // if (types.size() != 2 || types[0] != DataTypeFloat64 || types[1] != DataTypeFloat64)
        //     throw Exception("invalid arguments for " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        // return static_cast<llvm::IRBuilder<>>(builder).CreateFAdd(values[0], values[1], "add");
        return llvm::ConstantFP::get(builder.getDoubleTy(), 12345.0);
    }
//#endif

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionSomething>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeFloat64>(); }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        throw Exception("should've used the jitted version", ErrorCodes::NOT_IMPLEMENTED);
    }
};


void registerFunctionsLLVMTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSomething>();
}

}
