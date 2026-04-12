#include <Functions/h3Common.h>

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <base/range.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionH3GetBaseCell : public IFunction
{
public:
    static constexpr auto name = "h3GetBaseCell";

    H3Validator validator;

    explicit FunctionH3GetBaseCell(const ContextPtr & context) : validator(context) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionH3GetBaseCell>(context); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"index", &isUInt64, nullptr, "UInt64"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args);

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

            UInt8 res = 0;
            if (validator.validateCell(hindex))
                res = static_cast<UInt8>(getBaseCellNumber(hindex));

            dst_data[row] = res;
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(H3GetBaseCell)
{
    FunctionDocumentation::Description description = R"(
Returns the base cell number of the [H3](#h3-index) index.
    )";
    FunctionDocumentation::Syntax syntax = "h3GetBaseCell(index)";
    FunctionDocumentation::Arguments arguments = {
        {"index", "Hexagon index number.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the hexagon base cell number. Throws an exception if the input is not a valid H3 cell (controlled by the `functions_h3_default_if_invalid` setting).",
        {"UInt8"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get base cell number",
            "SELECT h3GetBaseCell(612916788725809151) AS basecell",
            R"(
┌─basecell─┐
│       12 │
└──────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3GetBaseCell>(documentation);
}

}

#endif
