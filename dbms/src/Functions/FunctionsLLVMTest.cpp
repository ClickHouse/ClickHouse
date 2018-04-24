#include <Columns/ColumnVector.h>
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
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionSomething : public IFunction
{
public:
    static constexpr auto name = "something";

//#if USE_EMBEDDED_COMPILER
    bool isCompilable(const DataTypes & types) const override
    {
        return types.size() == 2 && types[0]->equals(*types[1]);
    }

    llvm::Value * compile(llvm::IRBuilderBase & builder, const DataTypes & types, const ValuePlaceholders & values) const override
    {
        if (types[0]->equals(DataTypeFloat32{}) || types[0]->equals(DataTypeFloat64{}))
            return static_cast<llvm::IRBuilder<>&>(builder).CreateFAdd(values[0], values[1]);
        return static_cast<llvm::IRBuilder<>&>(builder).CreateAdd(values[0], values[1]);
    }

    IColumn::Ptr createResultColumn(const DataTypes & types, size_t size) const
    {
        if (types[0]->equals(DataTypeInt8{}))
            return ColumnVector<Int8>::create(size);
        if (types[0]->equals(DataTypeInt16{}))
            return ColumnVector<Int16>::create(size);
        if (types[0]->equals(DataTypeInt32{}))
            return ColumnVector<Int32>::create(size);
        if (types[0]->equals(DataTypeInt64{}))
            return ColumnVector<Int64>::create(size);
        if (types[0]->equals(DataTypeUInt8{}))
            return ColumnVector<UInt8>::create(size);
        if (types[0]->equals(DataTypeUInt16{}))
            return ColumnVector<UInt16>::create(size);
        if (types[0]->equals(DataTypeUInt32{}))
            return ColumnVector<UInt32>::create(size);
        if (types[0]->equals(DataTypeUInt64{}))
            return ColumnVector<UInt64>::create(size);
        if (types[0]->equals(DataTypeFloat32{}))
            return ColumnVector<Float32>::create(size);
        if (types[0]->equals(DataTypeFloat64{}))
            return ColumnVector<Float64>::create(size);
        throw Exception("invalid input type", ErrorCodes::LOGICAL_ERROR);
    }
//#endif

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
