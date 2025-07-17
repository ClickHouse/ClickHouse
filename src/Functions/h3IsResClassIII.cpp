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

class FunctionH3IsResClassIII : public IFunction
{
public:
    static constexpr auto name = "h3IsResClassIII";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3IsResClassIII>(); }

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
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data = column->getData();

        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            UInt8 res = isResClassIII(data[row]);
            dst_data[row] = res;
        }
        return dst;
    }
};

}

REGISTER_FUNCTION(H3IsResClassIII)
{
    FunctionDocumentation::Description description = R"(
Returns whether an [H3](#h3-index) index has a resolution with Class III orientation.

Class III resolutions have an odd-numbered resolution, while Class II resolutions are even-numbered.
    )";
    FunctionDocumentation::Syntax syntax = "h3IsResClassIII(index)";
    FunctionDocumentation::Arguments arguments = {
        {"index", "Hexagon index number.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns `1` if the index has a Class III resolution (odd-numbered), `0` otherwise.",
        {"UInt8"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Check if H3 index has Class III resolution",
            "SELECT h3IsResClassIII(617420388352917503) AS res",
            R"(
┌─res─┐
│   1 │
└─────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3IsResClassIII>(documentation);
}

}

#endif
