#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>
#include <Columns/ColumnConst.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Returns global default value for type name (example: 0 for numeric types, '' for String).
class FunctionDefaultValueOfTypeName : public IFunction
{
public:
    static constexpr auto name = "defaultValueOfTypeName";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionDefaultValueOfTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const ColumnConst * col_type_const = typeid_cast<const ColumnConst *>(arguments.front().column.get());
        if (!col_type_const || !isString(arguments.front().type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be a constant string describing type.",
                getName());

        return DataTypeFactory::instance().get(col_type_const->getValue<String>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType & type = *result_type;
        return type.createColumnConst(input_rows_count, type.getDefault());
    }
};

}

REGISTER_FUNCTION(DefaultValueOfTypeName)
{
    FunctionDocumentation::Description description_defaultValueOfTypeName = R"(
Returns the default value for the given type name.
    )";
    FunctionDocumentation::Syntax syntax_defaultValueOfTypeName = "defaultValueOfTypeName(type)";
    FunctionDocumentation::Arguments arguments_defaultValueOfTypeName = {
        {"type", "A string representing a type name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_defaultValueOfTypeName = {"Returns the default value for the given type name: `0` for numbers, an empty string for strings, or `NULL` for Nullable", {"UInt8", "String", "NULL"}};
    FunctionDocumentation::Examples examples_defaultValueOfTypeName = {
    {
        "Usage example",
        R"(
SELECT defaultValueOfTypeName('Int8');
        )",
        R"(
┌─defaultValueOfTypeName('Int8')─┐
│                              0 │
└────────────────────────────────┘
        )"
    },
    {
        "Nullable example",
        R"(
SELECT defaultValueOfTypeName('Nullable(Int8)');
        )",
        R"(
┌─defaultValueOfTypeName('Nullable(Int8)')─┐
│                                     ᴺᵁᴸᴸ │
└──────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_defaultValueOfTypeName = {1, 1};
    FunctionDocumentation::Category category_defaultValueOfTypeName = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_defaultValueOfTypeName = {description_defaultValueOfTypeName, syntax_defaultValueOfTypeName, arguments_defaultValueOfTypeName, returned_value_defaultValueOfTypeName, examples_defaultValueOfTypeName, introduced_in_defaultValueOfTypeName, category_defaultValueOfTypeName};

    factory.registerFunction<FunctionDefaultValueOfTypeName>(documentation_defaultValueOfTypeName);
}

}
