#include <Common/config.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#if USE_EMBEDDED_COMPILER
#include <llvm/IR/Constants.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Type.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionSomething : public IFunction
{
public:
    static constexpr auto name = "something";

#if USE_EMBEDDED_COMPILER
    bool isCompilable(const DataTypes & types) const override
    {
        return types.size() == 2 && types[0]->equals(*types[1]);
    }

    llvm::Value * compile(llvm::IRBuilderBase & builder, const DataTypes & types, const ValuePlaceholders & values) const override
    {
        if (types[0]->equals(DataTypeFloat32{}) || types[0]->equals(DataTypeFloat64{}))
            return static_cast<llvm::IRBuilder<>&>(builder).CreateFAdd(values[0](), values[1]());
        return static_cast<llvm::IRBuilder<>&>(builder).CreateAdd(values[0](), values[1]());
    }
#endif

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionSomething>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override { return types[0]; }

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
