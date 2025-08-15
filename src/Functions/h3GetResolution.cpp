#include "config.h"

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionH3GetResolution : public IFunction
{
public:
    static constexpr auto name = "h3GetResolution";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetResolution>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * column = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data = column->getData();

        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 hindex = data[row];

            UInt8 res = getResolution(hindex);

            dst_data[row] = res;
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(H3GetResolution)
{
    FunctionDocumentation::Description description = R"(
Defines the resolution of the given [H3](https://h3geo.org/docs/core-library/h3Indexing/) index.
    )";
    FunctionDocumentation::Syntax syntax = "h3GetResolution(h3index)";
    FunctionDocumentation::Arguments arguments = {
        {"h3index", "Hexagon index number.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the resolution of the index with range `[0, 15]` if the index is valid, otherwise returns a random value.",
        {"UInt8"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get resolution of H3 index",
            "SELECT h3GetResolution(639821929606596015) AS resolution",
            R"(
┌─resolution─┐
│         14 │
└────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3GetResolution>(documentation);
}

}

#endif
