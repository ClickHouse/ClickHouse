#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
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
    class FunctionH3ToCenterChild : public IFunction
    {
    public:
        static constexpr auto name = "h3ToCenterChild";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToCenterChild>(); }

        std::string getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 2; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        arg = arguments[1].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt8",
                arg->getName(), 2, getName());

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_hindex = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_hindex)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_hindex = col_hindex->getData();

        const auto * col_resolution = checkAndGetColumn<ColumnUInt8>(non_const_arguments[1].column.get());
        if (!col_resolution)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt8.",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_resolution = col_resolution->getData();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (data_resolution[row] > MAX_H3_RES)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                    toString(data_resolution[row]),
                    getName(),
                    toString(MAX_H3_RES));

            UInt64 res = cellToCenterChild(data_hindex[row], data_resolution[row]);

            dst_data[row] = res;
        }
        return dst;
    }
};

}

REGISTER_FUNCTION(H3ToCenterChild)
{
    FunctionDocumentation::Description description = R"(
Returns the center child (finer) [H3](#h3-index) index contained by the given H3 index at the given resolution.

This function finds the center child of an H3 index at a specified finer resolution. The resolution must be greater than the resolution of the input index.
    )";
    FunctionDocumentation::Syntax syntax = "h3ToCenterChild(index, resolution)";
    FunctionDocumentation::Arguments arguments = {
        {"index", "Parent H3 index.", {"UInt64"}},
        {"resolution", "Resolution of the center child with range `[0, 15]`.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the H3 index of the center child at the specified resolution.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get center child at resolution 1",
            "SELECT h3ToCenterChild(577023702256844799, 1) AS centerToChild",
            R"(
┌──────centerToChild─┐
│ 581496515558637567 │
└────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3ToCenterChild>(documentation);
}

}

#endif
