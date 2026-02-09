#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionLowCardinalityKeys: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityKeys";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionLowCardinalityKeys>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type = typeid_cast<const DataTypeLowCardinality *>(arguments[0].get());
        if (!type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First first argument of function lowCardinalityKeys must be ColumnLowCardinality, "
                            "but got {}", arguments[0]->getName());

        return type->getDictionaryType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & arg = arguments[0];
        const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(arg.column.get());
        /// In the case of a shared dictionary, we want only dictionary keys that correspond to this block:
        return low_cardinality_column->cutAndCompact(0, input_rows_count)->getDictionary().getNestedColumn()->cloneResized(arg.column->size());
    }
};

}

REGISTER_FUNCTION(LowCardinalityKeys)
{
    FunctionDocumentation::Description description = R"(
Returns the dictionary values of a [LowCardinality](../data-types/lowcardinality.md) column.
If the block is smaller or larger than the dictionary size, the result will be truncated or extended with default values.
Since LowCardinality have per-part dictionaries, this function may return different dictionary values in different parts.
    )";
    FunctionDocumentation::Syntax syntax = "lowCardinalityKeys(col)";
    FunctionDocumentation::Arguments arguments = {
        {"col", "A low cardinality column.", {"LowCardinality"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the dictionary keys.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "lowCardinalityKeys",
        R"(
DROP TABLE IF EXISTS test;
CREATE TABLE test (s LowCardinality(String)) ENGINE = Memory;

-- create two parts:

INSERT INTO test VALUES ('ab'), ('cd'), ('ab'), ('ab'), ('df');
INSERT INTO test VALUES ('ef'), ('cd'), ('ab'), ('cd'), ('ef');

SELECT s, lowCardinalityKeys(s) FROM test;
        )",
        R"(
┌─s──┬─lowCardinalityKeys(s)─┐
│ ef │                       │
│ cd │ ef                    │
│ ab │ cd                    │
│ cd │ ab                    │
│ ef │                       │
└────┴───────────────────────┘
┌─s──┬─lowCardinalityKeys(s)─┐
│ ab │                       │
│ cd │ ab                    │
│ ab │ cd                    │
│ ab │ df                    │
│ df │                       │
└────┴───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLowCardinalityKeys>(documentation);
}

}
