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

/** Function that converts color from OKLCH perceptual color space to sRGB color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
namespace
{
class FunctionColorOKLCHToSRGB : public ITupleFunction
{
public:
    static constexpr auto name = "colorOKLCHToSRGB";

    explicit FunctionColorOKLCHToSRGB(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorOKLCHToSRGB>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
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

        /// We require the first argument to be a Tuple rather than an Array because the three OKLCH channels are conceptually
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
        ColumnPtr chroma_column = rgb_cols[1]->convertToFullColumnIfConst();
        ColumnPtr hue_column = rgb_cols[2]->convertToFullColumnIfConst();

        const auto & lightness_data = assert_cast<const ColumnFloat64 &>(*lightness_column).getData();
        const auto & chroma_data = assert_cast<const ColumnFloat64 &>(*chroma_column).getData();
        const auto & hue_data = assert_cast<const ColumnFloat64 &>(*hue_column).getData();
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
            ColorConversion::Color lch_data{lightness_data[row], chroma_data[row], hue_data[row]};
            Float64 gamma_cur = gamma_data ? (*gamma_data)[row] : ColorConversion::default_gamma;
            ColorConversion::Color res = convertOklchToSrgb(lch_data, gamma_cur);
            red_data.push_back(res[0]);
            green_data.push_back(res[1]);
            blue_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_red), std::move(col_green), std::move(col_blue)}));
    }

private:
    /// OKLCH -> sRGB. Follows the step-by-step pipeline described in Ottosson’s article. See ColorConversion.h
    ColorConversion::Color convertOklchToSrgb(const ColorConversion::Color & oklch, Float64 gamma) const
    {
        Float64 chroma = oklch[1];
        Float64 hue_rad = oklch[2] * ColorConversion::deg2rad;

        ColorConversion::Color oklab = oklch;

        oklab[1] = chroma * std::cos(hue_rad);
        oklab[2] = chroma * std::sin(hue_rad);

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

REGISTER_FUNCTION(FunctionColorOKLCHToSRGB)
{
    FunctionDocumentation::Description description = R"(
Converts a colour from the **OKLCH** perceptual colour space to the familiar **sRGB** colour space.

If `L` is outside the range `[0...1]`, `C` is negative, or `H` is outside the range `[0...360]`, the result is implementation-defined.

:::note
**OKLCH** is a cylindrical version of the OKLab colour space.
It's three coordinates are `L` (the lightness in the range `[0...1]`), `C` (chroma `>= 0`) and `H` (hue in degrees  from `[0...360]`)**.
OKLab/OKLCH is designed to be perceptually uniform while remaining cheap to compute.
:::

The conversion is the inverse of [`colorSRGBToOKLCH`](#colorSRGBToOKLCH):

1) OKLCH to OKLab.
2) OKLab to Linear sRGB
3) Linear sRGB to sRGB

The second argument gamma is used at the last stage.

For references of colors in OKLCH space, and how they correspond to sRGB colors please see [https://oklch.com/](https://oklch.com/).
    )";
    FunctionDocumentation::Syntax syntax = "colorOKLCHToSRGB(tuple [, gamma])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "A tuple of three numeric values `L`, `C`, `H`, where `L` is in the range `[0...1]`, `C >= 0` and `H` is in the range `[0...360]`.", {"Tuple(Float64, Float64, Float64)"}},
        {"gamma", "Optional. The exponent that is used to transform linear sRGB back to sRGB by applying `(x ^ (1 / gamma)) * 255` for each channel `x`. Defaults to `2.2`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (R, G, B) representing sRGB color values.", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Convert OKLCH to sRGB",
        R"(
SELECT colorOKLCHToSRGB((0.4466, 0.0991, 45.44), 2.2) AS rgb
WITH colorOKLCHToSRGB((0.7, 0.1, 54)) as t SELECT tuple(toUInt8(t.1), toUInt8(t.2), toUInt8(t.3)) AS RGB
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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionColorOKLCHToSRGB>(documentation);
}
}
