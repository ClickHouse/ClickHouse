#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Field.h>


namespace DB {

class FunctionHello : public IFunction {
public:
    static constexpr auto name = "helloWorld";
    static FunctionPtr create(ContextPtr) {return std::make_shared<FunctionHello>(); }
    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeString>(); }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, "Hello World!");
    }
};

void registerFunctionHelloWorld(FunctionFactory & factory) {
    factory.registerFunction<FunctionHello>();   
}

}
