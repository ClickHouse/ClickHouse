#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/ColorConversion.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ITupleFunction.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Function that converts color from OKLAB perceptual color space to sRGB color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
namespace
{
class FunctionColorOKLABToSRGB : public ITupleFunction
{
public:
    static constexpr auto name = "colorOKLABToSRGB";

    explicit FunctionColorOKLABToSRGB(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorOKLABToSRGB>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 or 2 arguments, {} provided",
                getName(), arguments.size());

        const auto * first_arg = arguments[0].get();

        /// We require the first argument to be a Tuple rather than an Array because the three OKLAB channels are conceptually
        /// independent and users often supply them in different numeric types e.g. (0.75 Float64, 0.12 Float32, 45 UInt8)
        if (!isTuple(first_arg))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a tuple",
                getName());

        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(first_arg);
        const auto & tuple_inner_types  = tuple_type->getElements();

        if (tuple_inner_types.size() != ColorConversion::channels)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a tuple of size {}, a tuple of size {} was provided",
                getName(), ColorConversion::channels, tuple_inner_types.size());

        for (const auto & tuple_inner_type : tuple_inner_types)
        {
            if (!isNumber(tuple_inner_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Tuple elements of the first argument of function {} must be numbers",
                    getName());
        }

        if (arguments.size() == 2 && !isNumber(arguments[1].get()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument of function {} must be a number",
                    getName());

        auto float64_type = std::make_shared<DataTypeFloat64>();
        return std::make_shared<DataTypeTuple>(DataTypes(ColorConversion::channels, float64_type));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto float64_type = std::make_shared<DataTypeFloat64>();
        auto tuple_f64_type = std::make_shared<DataTypeTuple>(DataTypes(ColorConversion::channels, float64_type));

        auto tuple_f64_arg = castColumn(arguments[0], tuple_f64_type);
        auto rgb_cols = getTupleElements(*tuple_f64_arg);

        ColumnPtr gamma;
        if (arguments.size() == 2)
            gamma = castColumn(arguments[1], float64_type)->convertToFullColumnIfConst();

        ColumnPtr lightness_column = rgb_cols[0]->convertToFullColumnIfConst();
        ColumnPtr a_column = rgb_cols[1]->convertToFullColumnIfConst();
        ColumnPtr b_column = rgb_cols[2]->convertToFullColumnIfConst();

        const auto & lightness_data = assert_cast<const ColumnFloat64 &>(*lightness_column).getData();
        const auto & a_data = assert_cast<const ColumnFloat64 &>(*a_column).getData();
        const auto & b_data = assert_cast<const ColumnFloat64 &>(*b_column).getData();
        const auto * gamma_data = gamma ? &assert_cast<const ColumnFloat64 &>(*gamma).getData() : nullptr;

        auto col_red = ColumnFloat64::create();
        auto col_green = ColumnFloat64::create();
        auto col_blue = ColumnFloat64::create();

        auto & red_data = col_red->getData();
        auto & green_data = col_green->getData();
        auto & blue_data = col_blue->getData();

        red_data.reserve(input_rows_count);
        green_data.reserve(input_rows_count);
        blue_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            ColorConversion::Color lab_data{lightness_data[row], a_data[row], b_data[row]};
            Float64 gamma_cur = gamma_data ? (*gamma_data)[row] : ColorConversion::default_gamma;
            ColorConversion::Color res = convertOklabToSrgb(lab_data, gamma_cur);
            red_data.push_back(res[0]);
            green_data.push_back(res[1]);
            blue_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_red), std::move(col_green), std::move(col_blue)}));
    }

private:
    /// OKLAB -> sRGB. Follows the step-by-step pipeline described in Ottosson’s article. See ColorConversion.h
    ColorConversion::Color convertOklabToSrgb(const ColorConversion::Color & oklab, Float64 gamma) const
    {

        ColorConversion::Color lms{};
        for (size_t i = 0; i < ColorConversion::channels; ++i)
        {
            for (size_t channel = 0; channel < ColorConversion::channels; ++channel)
                lms[i] = std::fma(oklab[channel], ColorConversion::oklab_to_lms_base[(3 * i) + channel], lms[i]);
            lms[i] = lms[i] * lms[i] * lms[i];
        }

        ColorConversion::Color rgb{};
        for (size_t i = 0; i < ColorConversion::channels; ++i)
        {
            for (size_t channel = 0; channel < ColorConversion::channels; ++channel)
                rgb[i] = std::fma(lms[channel], ColorConversion::lms_to_linear_base[(3 * i) + channel], rgb[i]);
        }

        if (gamma == 0)
            gamma = ColorConversion::gamma_fallback;

        Float64 power = 1 / gamma;
        for (size_t i = 0; i < ColorConversion::channels; ++i)
        {
            rgb[i] = std::clamp(rgb[i], 0.0, 1.0);
            rgb[i] = std::pow(rgb[i], power) * 255.0;
        }

        return rgb;
    }
};
}

REGISTER_FUNCTION(FunctionColorOKLABToSRGB)
{
    FunctionDocumentation::Description description = R"(
Converts a color from the OKLab perceptual color space to the sRGB color space.

The input color is specified in the OKLab color space. If the input values are outside
the typical OKLab ranges, the result is implementation-defined.

OKLab uses three components:
- L: perceptual lightness (typically in the range [0..1])
- a: green–red opponent axis
- b: blue–yellow opponent axis

The a and b components are not bounded. OKLab is designed to be perceptually uniform
while remaining inexpensive to compute.

The conversion is intended to be the inverse of colorSRGBToOKLAB and consists of
the following stages:
1) Conversion from OKLab to linear sRGB.
2) Conversion from linear sRGB to gamma-encoded sRGB.

The optional gamma argument specifies the exponent used when converting from linear
sRGB to gamma-encoded RGB values. If not specified, a default gamma value is used
for consistency with colorSRGBToOKLAB.

For more information about the OKLab color space and its relationship to sRGB, see https://oklch.com/.
    )";
    FunctionDocumentation::Syntax syntax = "colorOKLABToSRGB(tuple [, gamma])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "A tuple of three numeric values `L`, `a`, `b`, where `L` is in the range `[0...1]`.", {"Tuple(Float64, Float64, Float64)"}},
        {"gamma", "Optional. The exponent that is used to transform linear sRGB back to sRGB by applying `(x ^ (1 / gamma)) * 255` for each channel `x`. Defaults to `2.2`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (R, G, B) representing sRGB color values.", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Convert OKLAB to sRGB",
        R"(
SELECT colorOKLABToSRGB((0.4466, 0.0991, .44), 2.2) AS rgb
WITH colorOKLABToSRGB((0.7, 0.1, .54)) as t SELECT tuple(toUInt8(t.1), toUInt8(t.2), toUInt8(t.3)) AS RGB
        )",
        R"(
┌─rgb──────────────────────────────────────────────────────┐
│ (127.03349738778945,66.06672044472008,37.11802592155851) │
└──────────────────────────────────────────────────────────┘
┌─RGB──────────┐
│ (205,139,97) │
└──────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionColorOKLABToSRGB>(documentation);
}
}
