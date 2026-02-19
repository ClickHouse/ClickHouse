#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnsNumber.h>
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

class FunctionLowCardinalityIndices: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityIndices";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionLowCardinalityIndices>(); }

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
                            "First first argument of function lowCardinalityIndexes must be ColumnLowCardinality, "
                            "but got {}", arguments[0]->getName());

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & arg = arguments[0];
        auto indexes_col = typeid_cast<const ColumnLowCardinality *>(arg.column.get())->getIndexesPtr();
        auto new_indexes_col = ColumnUInt64::create(indexes_col->size());
        auto & data = new_indexes_col->getData();
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = indexes_col->getUInt(i);

        return new_indexes_col;
    }
};

}

REGISTER_FUNCTION(LowCardinalityIndices)
{
    FunctionDocumentation::Description description = R"(
Returns the position of a value in the dictionary of a [LowCardinality](../data-types/lowcardinality.md) column. Positions start at 1. Since LowCardinality have per-part dictionaries, this function may return different positions for the same value in different parts.
    )";
    FunctionDocumentation::Syntax syntax = "lowCardinalityIndices(col)";
    FunctionDocumentation::Arguments arguments = {
        {"col", "A low cardinality column.", {"LowCardinality"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The position of the value in the dictionary of the current part.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage examples",
        R"(
DROP TABLE IF EXISTS test;
CREATE TABLE test (s LowCardinality(String)) ENGINE = Memory;

-- create two parts:

INSERT INTO test VALUES ('ab'), ('cd'), ('ab'), ('ab'), ('df');
INSERT INTO test VALUES ('ef'), ('cd'), ('ab'), ('cd'), ('ef');

SELECT s, lowCardinalityIndices(s) FROM test;
        )",
        R"(
┌─s──┬─lowCardinalityIndices(s)─┐
│ ab │                        1 │
│ cd │                        2 │
│ ab │                        1 │
│ ab │                        1 │
│ df │                        3 │
└────┴──────────────────────────┘
┌─s──┬─lowCardinalityIndices(s)─┐
│ ef │                        1 │
│ cd │                        2 │
│ ab │                        3 │
│ cd │                        2 │
│ ef │                        1 │
└────┴──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLowCardinalityIndices>(documentation);
}

}
