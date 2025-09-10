#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/// Dump the structure of type and column.
class FunctionDumpColumnStructure : public IFunction
{
public:
    static constexpr auto name = "dumpColumnStructure";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionDumpColumnStructure>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & elem = arguments[0];

        /// Note that the result is not a constant, because it contains columns size.

        return DataTypeString().createColumnConst(input_rows_count,
                elem.type->getName() + ", " + elem.column->dumpStructure())->convertToFullColumnIfConst();
    }
};

}

REGISTER_FUNCTION(DumpColumnStructure)
{
    FunctionDocumentation::Description description_dumpColumnStructure = R"(
Outputs a detailed description of the internal structure of a column and its data type.
)";
    FunctionDocumentation::Syntax syntax_dumpColumnStructure = "dumpColumnStructure(x)";
    FunctionDocumentation::Arguments arguments_dumpColumnStructure = {
        {"x", "Value for which to get the description of.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_dumpColumnStructure = {"Returns a description of the column structure used for representing the value.", {"String"}};
    FunctionDocumentation::Examples examples_dumpColumnStructure = {
    {
        "Usage example",
        R"(
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'));
        )",
        R"(
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_dumpColumnStructure = {1, 1};
    FunctionDocumentation::Category category_dumpColumnStructure = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_dumpColumnStructure = {description_dumpColumnStructure, syntax_dumpColumnStructure, arguments_dumpColumnStructure, returned_value_dumpColumnStructure, examples_dumpColumnStructure, introduced_in_dumpColumnStructure, category_dumpColumnStructure};

    factory.registerFunction<FunctionDumpColumnStructure>(documentation_dumpColumnStructure);
}

}
