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
class FunctionColorSRGBToOKLCH : public ITupleFunction
{
public:
    static constexpr auto name = "colorSRGBToOKLCH";

    explicit FunctionColorSRGBToOKLCH(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorSRGBToOKLCH>(context_); }

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

        /// We require the first argument to be a Tuple rather than an Array to give the user more flexibility
        /// which types they use for input, e.g. (32.7554 Float64, 49 UInt8, 132 UInt8)
        if (!isTuple(first_arg))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a tuple",
                getName());

        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(first_arg);
        const auto & tuple_inner_types  = tuple_type->getElements();

        if (tuple_inner_types.size() != channels)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a tuple of size {}, a tuple of size {} was provided",
                getName(), channels, tuple_inner_types.size());

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
        return std::make_shared<DataTypeTuple>(DataTypes(channels, float64_type));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto float64_type = std::make_shared<DataTypeFloat64>();
        auto tuple_f64_ptr = std::make_shared<DataTypeTuple>(DataTypes(channels, float64_type));

        auto tuple_f64_arg = castColumn(arguments[0], tuple_f64_ptr);
        auto rgb_cols = getTupleElements(*tuple_f64_arg);

        ColumnPtr gamma;
        if (arguments.size() == 2)
            gamma = castColumn(arguments[1], float64_type);

        const auto & red_data = assert_cast<const ColumnFloat64 &>(*rgb_cols[0]).getData();
        const auto & green_data = assert_cast<const ColumnFloat64 &>(*rgb_cols[1]).getData();
        const auto & blue_data = assert_cast<const ColumnFloat64 &>(*rgb_cols[2]).getData();
        const auto * gamma_data = gamma ? &assert_cast<const ColumnFloat64 &>(*gamma).getData() : nullptr;

        auto col_lightness = ColumnFloat64::create();
        auto col_chroma = ColumnFloat64::create();
        auto col_hue = ColumnFloat64::create();

        auto & lightness_data = col_lightness->getData();
        auto & chroma_data = col_chroma->getData();
        auto & hue_data = col_hue->getData();

        lightness_data.reserve(input_rows_count);
        chroma_data.reserve(input_rows_count);
        hue_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            Color rgb_data{red_data[row], green_data[row], blue_data[row]};
            Float64 gamma_cur = gamma_data ? (*gamma_data)[row] : default_gamma;
            Color res = convertSrgbToOklch(rgb_data, gamma_cur);
            lightness_data.push_back(res[0]);
            chroma_data.push_back(res[1]);
            hue_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_lightness), std::move(col_chroma), std::move(col_hue)}));
    }

private:
    /// sRGB -> OKLCH. Follows the step-by-step pipeline described in Ottosson’s article, see ColorConversion.h
    Color convertSrgbToOklch(const Color & rgb, Float64 gamma) const
    {
        Color rgb_lin;
        for (size_t i = 0; i < channels; ++i)
            rgb_lin[i] = std::pow(rgb[i] / 255.0, gamma);

        Color lms{};
        for (size_t i = 0; i < channels; ++i)
        {
            for (size_t channel = 0; channel < channels; ++channel)
                lms[i] = std::fma(rgb_lin[channel], linear_to_lms_base[(3 * i) + channel], lms[i]);
            lms[i] = std::cbrt(lms[i]);
        }

        Color oklab{};
        for (size_t i = 0; i < channels; ++i)
        {
            for (size_t channel = 0; channel < channels; ++channel)
                oklab[i] = std::fma(lms[channel], lms_to_oklab_base[(3 * i) + channel], oklab[i]);
        }

        Color oklch = oklab;

        Float64 a = oklab[1];
        Float64 b = oklab[2];

        oklch[1] = std::sqrt(a * a + b * b);
        if (oklch[1] >= epsilon)
        {
            Float64 hue_degrees = std::atan2(b, a) * rad2deg;
            oklch[2]  = std::fmod(hue_degrees + 360.0, 360.0);
        }
        else
        {
            oklch[1] = 0;
            oklch[2] = 0;
        }

        return oklch;
    }

};

}

REGISTER_FUNCTION(ColorSRGBToOKLCH)
{
    const FunctionDocumentation description = {
        .description=R"(Converts converts color from sRGB color space to perceptual OKLCH color space.
Takes an optional parameter gamma, that is defaulted at 2.2 in case it is not provided. Dual of colorOKLCHToSRGB)",
        .arguments={
            {"rgb_tuple", "A 3-element tuple of numeric values (e.g. integers in rage [0...255])"},
            {"gamma", "Optional gamma exponent to linearize sRGB before conversion. Defaults to 2.2."},
        },
        .returned_value{"Returns a 3-element tuple of OKLCH values", {"Tuple(Float64, Float64, Float64)"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Other,
    };
    factory.registerFunction<FunctionColorSRGBToOKLCH>(description);
}

}
