#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/flattenTuple.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionFlattenTuple : public IFunction
{
public:
    static constexpr auto name = "flattenTuple";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFlattenTuple>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & type = arguments[0];
        const auto * type_tuple = checkAndGetDataType<DataTypeTuple>(removeNullable(type).get());
        if (!type_tuple)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function '{}' must be Tuple or Nullable(Tuple). Got '{}'",
                getName(),
                type->getName());

        if (type_tuple->getElements().empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Tuple cannot be empty for function '{}'", getName());

        if (!type_tuple || !type_tuple->hasExplicitNames())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Tuple argument for function '{}' must be named. Got '{}'",
                getName(), type->getName());

        return flattenTuple(type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto column = arguments.at(0).column;
        if (!checkAndGetColumn<ColumnTuple>(removeNullable(column).get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}. Expected ColumnTuple or Nullable(ColumnTuple)",
                column->getName(), getName());

        return flattenTuple(column);
    }
};

}

REGISTER_FUNCTION(FlattenTuple)
{
    FunctionDocumentation::Description description = R"(
Flattens a named and nested tuple.
The elements of the returned tuple are the paths of the input tuple.
)";
    FunctionDocumentation::Syntax syntax = "flattenTuple(input)";
    FunctionDocumentation::Arguments arguments = {
        {"input", "Named and nested tuple to flatten.", {"Tuple(n1 T1[, n2 T2, ... ])"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an output tuple whose elements are paths from the original input.", {"Tuple(T)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE tab(t Tuple(a UInt32, b Tuple(c String, d UInt32))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ((3, ('c', 4)));

SELECT flattenTuple(t) FROM tab;
        )",
        R"(
┌─flattenTuple(t)┐
│ (3, 'c', 4)    │
└────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFlattenTuple>(documentation);
}

}
