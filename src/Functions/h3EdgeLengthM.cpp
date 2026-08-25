#include "config.h"

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

// Average metric edge length of H3 hexagon. The edge length `e` for given resolution `res` can
// be used for converting metric search radius `radius` to hexagon search ring size `k` that is
// used by `H3kRing` function. For small enough search area simple flat approximation can be used,
// i.e. the smallest `k` that satisfies relation `3 k^2 - 3 k + 1 >= (radius / e)^2` should be
// chosen
class FunctionH3EdgeLengthM : public IFunction
{
public:
    static constexpr auto name = "h3EdgeLengthM";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3EdgeLengthM>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt8",
                arg->getName(), 1, getName());

        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * column = checkAndGetColumn<ColumnUInt8>(non_const_arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt8",
                arguments[0].column->getName(),
                1,
                getName());

        const auto & data = column->getData();

        auto dst = ColumnVector<Float64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt8 resolution = data[row];
            if (resolution > MAX_H3_RES)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                    toString(resolution), getName(), MAX_H3_RES);

            Float64 res = getHexagonEdgeLengthAvgM(resolution);

            dst_data[row] = res;
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(H3EdgeLengthM)
{
    FunctionDocumentation::Description description = R"(
Calculates the average length of an [H3](https://h3geo.org/docs/core-library/h3Indexing/) hexagon edge in meters.
    )";
    FunctionDocumentation::Syntax syntax = "h3EdgeLengthM(resolution)";
    FunctionDocumentation::Arguments arguments = {
        {"resolution", "Index resolution. Range: `[0, 15]`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the average edge length of an [H3](#h3-index) hexagon in meters.",
        {"Float64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get edge length for maximum resolution",
            "SELECT h3EdgeLengthM(15) AS edgeLengthM",
            R"(
┌─edgeLengthM─┐
│ 0.509713273 │
└─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3EdgeLengthM>(documentation);
}

}

#endif
