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

/** Function that converts color from sRGB color space to perceptual OKLCH color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */

namespace
{
class FunctionColorSRGBToOKLAB : public ITupleFunction
{
public:
    static constexpr auto name = "colorSRGBToOKLAB";

    explicit FunctionColorSRGBToOKLAB(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorSRGBToOKLAB>(context_); }

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

        /// We require the first argument to be a Tuple rather than an Array to give the user more flexibility
        /// which types they use for input, e.g. (32.7554 Float64, 49 UInt8, 132 UInt8)
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
                    "Tuple elements of first argument of function {} must be numbers",
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
        auto tuple_f64_ptr = std::make_shared<DataTypeTuple>(DataTypes(ColorConversion::channels, float64_type));

        auto tuple_f64_arg = castColumn(arguments[0], tuple_f64_ptr);
        auto rgb_cols = getTupleElements(*tuple_f64_arg);

        ColumnPtr gamma;
        if (arguments.size() == 2)
            gamma = castColumn(arguments[1], float64_type)->convertToFullColumnIfConst();

        ColumnPtr red_column = rgb_cols[0]->convertToFullColumnIfConst();
        ColumnPtr green_column = rgb_cols[1]->convertToFullColumnIfConst();
        ColumnPtr blue_column = rgb_cols[2]->convertToFullColumnIfConst();

        const auto & red_data = assert_cast<const ColumnFloat64 &>(*red_column).getData();
        const auto & green_data = assert_cast<const ColumnFloat64 &>(*green_column).getData();
        const auto & blue_data = assert_cast<const ColumnFloat64 &>(*blue_column).getData();
        const auto * gamma_data = gamma ? &assert_cast<const ColumnFloat64 &>(*gamma).getData() : nullptr;

        auto col_lightness = ColumnFloat64::create();
        auto col_a = ColumnFloat64::create();
        auto col_b = ColumnFloat64::create();

        auto & lightness_data = col_lightness->getData();
        auto & a_data = col_a->getData();
        auto & b_data = col_b->getData();

        lightness_data.reserve(input_rows_count);
        a_data.reserve(input_rows_count);
        b_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            ColorConversion::Color rgb_data{red_data[row], green_data[row], blue_data[row]};
            Float64 gamma_cur = gamma_data ? (*gamma_data)[row] : ColorConversion::default_gamma;
            ColorConversion::Color res = convertSrgbToOklab(rgb_data, gamma_cur);
            lightness_data.push_back(res[0]);
            a_data.push_back(res[1]);
            b_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_lightness), std::move(col_a), std::move(col_b)}));
    }

private:
    /// sRGB -> OKLAB. Follows the step-by-step pipeline described in Ottosson’s article, see ColorConversion.h
    ColorConversion::Color convertSrgbToOklab(const ColorConversion::Color & rgb, Float64 gamma) const
    {
        ColorConversion::Color rgb_lin;
        for (size_t i = 0; i < ColorConversion::channels; ++i)
            rgb_lin[i] = std::pow(rgb[i] / 255.0, gamma);

        ColorConversion::Color lms{};
        for (size_t i = 0; i < ColorConversion::channels; ++i)
        {
            for (size_t channel = 0; channel < ColorConversion::channels; ++channel)
                lms[i] = std::fma(rgb_lin[channel], ColorConversion::linear_to_lms_base[(3 * i) + channel], lms[i]);
            lms[i] = std::cbrt(lms[i]);
        }

        ColorConversion::Color oklab{};
        for (size_t i = 0; i < ColorConversion::channels; ++i)
        {
            for (size_t channel = 0; channel < ColorConversion::channels; ++channel)
                oklab[i] = std::fma(lms[channel], ColorConversion::lms_to_oklab_base[(3 * i) + channel], oklab[i]);
        }

        return oklab;
    }

};

}

REGISTER_FUNCTION(ColorSRGBToOKLAB)
{
    FunctionDocumentation::Description description = R"(
Converts a colour encoded in the **sRGB** colour space to the perceptually uniform **OKLAB** colour space.

If any input channel is outside `[0...255]` or the gamma value is non-positive, the behaviour is implementation-defined.

:::note
**OKLAB** is a perceptually uniform color space.
Its three coordinates are `L` (the lightness in the range `[0...1]`), `a (Green–Red axis)` and `b (Blue–Yellow axis)`.
OKLab is designed to be perceptually uniform while remaining cheap to compute.
:::

The conversion consists of three stages:
1) sRGB to Linear sRGB
2) Linear sRGB to OKLab
)";
    FunctionDocumentation::Syntax syntax = "colorSRGBToOKLAB(tuple[, gamma])";
    FunctionDocumentation::Arguments arguments = {
        {"tuple", "Tuple of three values R, G, B in the range `[0...255]`.", {"Tuple(UInt8, UInt8, UInt8)"}},
        {"gamma", "Optional. Exponent that is used to linearize sRGB by applying `(x / 255)^gamma` to each channel `x`. Defaults to `2.2`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple (L, a, b) representing the OKLAB color space values.", {"Tuple(Float64, Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Convert sRGB to OKLAB",
        R"(
SELECT colorSRGBToOKLAB((128, 64, 32), 2.2) AS lab
        )",
        R"(
┌─lab─────────────────────────────────────────────────────────┐
│ (0.4436238384931984,0.10442699545678624,45.907345481930236) │
└─────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionColorSRGBToOKLAB>(documentation);
}

}
