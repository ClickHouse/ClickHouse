#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/ObjectUtils.h>
#include <Columns/ColumnTuple.h>

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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & type = arguments[0];
        const auto * type_tuple = checkAndGetDataType<DataTypeTuple>(type.get());
        if (!type_tuple || !type_tuple->hasExplicitNames())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function '{}' must be Named Tuple. Got '{}'",
                getName(), type->getName());

        auto [paths, types] = flattenTuple(type);
        Names names;
        names.reserve(paths.size());
        for (const auto & path : paths)
            names.push_back(path.getPath());

        return std::make_shared<DataTypeTuple>(types, names);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto column = arguments.at(0).column;
        if (!checkAndGetColumn<ColumnTuple>(column.get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}. Expected ColumnTuple",
                column->getName(), getName());

        return flattenTuple(column);
    }
};

}

REGISTER_FUNCTION(FlattenTuple)
{
    FunctionDocumentation::Description description = R"(
Returns a flattened `output` tuple from a nested and named `input` tuple.
Elements of the `output` tuple are the paths from the original `input` tuple.

For instance: `Tuple(a Int, Tuple(b Int, c Int)) -> Tuple(a Int, b Int, c Int)`.

`flattenTuple` can be used to select all paths from type `Object` as separate columns.
)";
    FunctionDocumentation::Syntax syntax = "flattenTuple(input)";
    FunctionDocumentation::Arguments arguments = {
        {"input", "Nested named tuple to flatten.", {"Tuple(n1 T1[, n2 T2, ... ])"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an output tuple whose elements are paths from the original input.", {"Tuple"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE t_flatten_tuple(t Tuple(t1 Nested(a UInt32, s String), b UInt32, t2 Tuple(k String, v UInt32))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_flatten_tuple VALUES (([(1, 'a'), (2, 'b')], 3, ('c', 4)));
SELECT flattenTuple(t) FROM t_flatten_tuple;
        )",
        R"(
┌─flattenTuple(t)─────────────────┐
│ ([1, 2], ['a', 'b'], 3, 'c', 4) │
└─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Tuple;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFlattenTuple>(documentation);
}

}
