#pragma once

#include <base/types.h>
#include <array>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
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

namespace ColorConversion
{
    /// Conversion constants for conversion between OKLCH and sRGB
    ///
    /// Source: Björn Ottosson, “OkLab – A perceptual colour space”, https://bottosson.github.io/posts/oklab/

    constexpr Float64 epsilon = 1e-6;           /// epsilon used when Chroma is approximately 0; below this value we set Chroma = Hue = 0.
    constexpr Float64 rad2deg = 57.2957795131;  /// 180 / pi (radians -> degrees)
    constexpr Float64 deg2rad = 0.01745329251;  /// pi / 180 (degrees -> radians)
    constexpr Float64 gamma_fallback = 1e-6;    /// substituted when the caller passes gamma = 0 so that 1 / gamma in gamma transfer function stays finite
    constexpr Float64 default_gamma = 2.2;      /// gamma defaults at 2.2 if not provided
    constexpr size_t channels = 3;              /// number of colour channels (always 3).

    using Color = std::array<Float64, channels>;
    using Mat3x3 = std::array<Float64, 9>;

    /// These 3 x 3 matrices are copied verbatim from the reference code from the source.
    ///
    ///  The forward pipeline implemented in srgbToOklchBase() and its inverse oklchToSrgbBase().
    ///     sRGB - linear-RGB - LMS (cube) - OkLab - OKLCH
    ///
    /// Each matrix converts from the space in its name to the next stage in the pipeline:

    /// linear-RGB -> LMS
    constexpr Mat3x3 linear_to_lms_base = {0.4122214708, 0.5363325363, 0.0514459929,
                                           0.2119034982, 0.6806995451, 0.1073969566,
                                           0.0883024619, 0.2817188376, 0.6299787005};
    /// LMS -> OKLab
    constexpr Mat3x3 lms_to_oklab_base = {0.2104542553,  0.7936177850, -0.0040720468,
                                          1.9779984951, -2.4285922050,  0.45059370996,
                                          0.0259040371,  0.7827717662, -0.8086757660};
    /// OKLab -> LMS
    constexpr Mat3x3 oklab_to_lms_base = {1,  0.3963377774,  0.2158037573,
                                          1, -0.1055613458, -0.0638541728,
                                          1, -0.0894841775, -1.2914855480};
    /// LMS -> linear-RGB
    constexpr Mat3x3 lms_to_linear_base = {4.0767416621, -3.3077115913,  0.2309699292,
                                           -1.2684380046,  2.6097574011, -0.3413193965,
                                           -0.0041960863, -0.7034186147,  1.7076147010};

    inline Color srgbToOklab(const Color & rgb, Float64 gamma)
    {
        /// Step 1: sRGB to Linear sRGB (gamma correction)
        Color rgb_lin;
        for (size_t i = 0; i < channels; ++i)
            rgb_lin[i] = std::pow(rgb[i] / 255.0, gamma);

        /// Step 2: Linear sRGB to LMS (cone response)
        Color lms{};
        for (size_t i = 0; i < channels; ++i)
        {
            for (size_t channel = 0; channel < channels; ++channel)
                lms[i] = std::fma(rgb_lin[channel], linear_to_lms_base[(3 * i) + channel], lms[i]);
            lms[i] = std::cbrt(lms[i]);
        }

        /// Step 3: LMS to OKLab
        Color oklab{};
        for (size_t i = 0; i < channels; ++i)
        {
            for (size_t channel = 0; channel < channels; ++channel)
                oklab[i] = std::fma(lms[channel], lms_to_oklab_base[(3 * i) + channel], oklab[i]);
        }

        return oklab;
    }

        /// Common helper: OKLab -> sRGB conversion
    /// This is shared by both OKLAB and OKLCH conversions to sRGB
    inline Color oklabToSrgb(const Color & oklab, Float64 gamma)
    {
        /// Step 1: OKLab to LMS (cone response)
        Color lms{};
        for (size_t i = 0; i < channels; ++i)
        {
            for (size_t channel = 0; channel < channels; ++channel)
                lms[i] = std::fma(oklab[channel], oklab_to_lms_base[(3 * i) + channel], lms[i]);
            lms[i] = lms[i] * lms[i] * lms[i];
        }

        /// Step 2: LMS to Linear sRGB
        Color rgb{};
        for (size_t i = 0; i < channels; ++i)
        {
            for (size_t channel = 0; channel < channels; ++channel)
                rgb[i] = std::fma(lms[channel], lms_to_linear_base[(3 * i) + channel], rgb[i]);
        }

        /// Step 3: Linear sRGB to sRGB (gamma correction)
        if (gamma == 0)
            gamma = gamma_fallback;

        Float64 power = 1 / gamma;
        for (size_t i = 0; i < channels; ++i)
        {
            rgb[i] = std::clamp(rgb[i], 0.0, 1.0);
            rgb[i] = std::pow(rgb[i], power) * 255.0;
        }

        return rgb;
    }
}

/** Base class for functions that convert color from sRGB color space to perceptual color spaces.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
template <typename Derived>
class ColorConversionFromSRGBBase : public ITupleFunction
{
public:

    explicit ColorConversionFromSRGBBase(ContextPtr context_) : ITupleFunction(context_) {}

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

        auto col_channel0 = ColumnFloat64::create();
        auto col_channel1 = ColumnFloat64::create();
        auto col_channel2 = ColumnFloat64::create();

        auto & channel0_data = col_channel0->getData();
        auto & channel1_data = col_channel1->getData();
        auto & channel2_data = col_channel2->getData();

        channel0_data.reserve(input_rows_count);
        channel1_data.reserve(input_rows_count);
        channel2_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            ColorConversion::Color rgb_data{red_data[row], green_data[row], blue_data[row]};
            Float64 gamma_cur = gamma_data ? (*gamma_data)[row] : ColorConversion::default_gamma;

            ColorConversion::Color res = static_cast<const Derived *>(this)->convertFromSrgb(rgb_data, gamma_cur);

            channel0_data.push_back(res[0]);
            channel1_data.push_back(res[1]);
            channel2_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_channel0), std::move(col_channel1), std::move(col_channel2)}));
    }
};

/** Base class for functions that convert color from perceptual color spaces to sRGB color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */
template <typename Derived>
class ColorConversionToSRGBBase : public ITupleFunction
{
public:
    explicit ColorConversionToSRGBBase(ContextPtr context_) : ITupleFunction(context_) {}

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
        auto tuple_f64_type = std::make_shared<DataTypeTuple>(DataTypes(ColorConversion::channels, float64_type));

        auto tuple_f64_arg = castColumn(arguments[0], tuple_f64_type);
        auto color_cols = getTupleElements(*tuple_f64_arg);

        ColumnPtr gamma;
        if (arguments.size() == 2)
            gamma = castColumn(arguments[1], float64_type)->convertToFullColumnIfConst();

        ColumnPtr channel0_column = color_cols[0]->convertToFullColumnIfConst();
        ColumnPtr channel1_column = color_cols[1]->convertToFullColumnIfConst();
        ColumnPtr channel2_column = color_cols[2]->convertToFullColumnIfConst();

        const auto & channel0_data = assert_cast<const ColumnFloat64 &>(*channel0_column).getData();
        const auto & channel1_data = assert_cast<const ColumnFloat64 &>(*channel1_column).getData();
        const auto & channel2_data = assert_cast<const ColumnFloat64 &>(*channel2_column).getData();
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
            ColorConversion::Color input_color{channel0_data[row], channel1_data[row], channel2_data[row]};
            Float64 gamma_cur = gamma_data ? (*gamma_data)[row] : ColorConversion::default_gamma;

            ColorConversion::Color res = static_cast<const Derived *>(this)->convertToSrgb(input_color, gamma_cur);

            red_data.push_back(res[0]);
            green_data.push_back(res[1]);
            blue_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_red), std::move(col_green), std::move(col_blue)}));
    }
};

}
