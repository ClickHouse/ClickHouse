#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace
{

class FunctionEmptyArray : public IFunction
{
private:
    String element_type;

public:
    static String getNameImpl(const String & element_type) { return "emptyArray" + element_type; }

    explicit FunctionEmptyArray(const String & element_type_) : element_type(element_type_) {}

private:
    String getName() const override
    {
        return getNameImpl(element_type);
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(element_type));
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnArray::create(
            DataTypeFactory::instance().get(element_type)->createColumn(),
            ColumnArray::ColumnOffsets::create(input_rows_count, 0));
    }
};

void registerFunction(FunctionFactory & factory, const String & element_type)
{
    factory.registerFunction(FunctionEmptyArray::getNameImpl(element_type),
        [element_type](ContextPtr){ return std::make_shared<FunctionEmptyArray>(element_type); });
}

}

REGISTER_FUNCTION(EmptyArray)
{
    const std::vector<std::string> types = {
        "UInt8", "UInt16", "UInt32", "UInt64",
        "Int8", "Int16", "Int32", "Int64",
        "Float32", "Float64", "Date", "DateTime", "String"
    };

    auto createDocumentation = [](const std::string& type_name) {
        FunctionDocumentation::Description description = "Returns an empty " + type_name + " array";
        FunctionDocumentation::Syntax syntax = "empty" + type_name + "()";
        FunctionDocumentation::ReturnedValue returned_value = "An empty array of type " + type_name;

        FunctionDocumentation::Example example = {
            "Create an empty " + type_name + " array",
            "SELECT empty" + type_name + "()",
            "[]"
        };

        FunctionDocumentation::Category categories = {"array"};

        return FunctionDocumentation{
            description, syntax, {}, returned_value, {example}, categories
        };
    };

    // Register all functions
    for (const auto& type : types) {
        registerFunction(factory, type, createDocumentation(type));
    }
}

}
