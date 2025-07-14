#include <Core/Field.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ITupleFunction.h>
#include <Interpreters/castColumn.h>
#include <Functions/ColorConversion.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Function that converts color from OKLCH perceptual color space
  * to sRGB color space.
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

    String  getName() const override { return name; }
    bool    isVariadic() const override { return true; }
    size_t  getNumberOfArguments() const override { return 0; }
    bool    useDefaultImplementationForConstants() const override { return true; }
    bool    isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 or 2 arguments, {} provided.",
                getName(), arguments.size());

        const auto * first_arg = arguments[0].get();

        /// We require the first argument to be a Tuple rather than an Array because
        ///  the three OKLCH channels are conceptually independent and users often
        ///  supply them in different numeric types:
        ///  e.g. (0.75 Float64, 0.12 Float32, 45 UInt8)
        if (!isTuple(first_arg))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a tuple.",
                getName());

        const auto * type = checkAndGetDataType<DataTypeTuple>(first_arg);

        const auto & tuple_elems  = type->getElements();

        if (tuple_elems.size() != channels_num)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument in function {} must be a tuple of size {}, a tuple of size {} provided.",
                getName(), channels_num, tuple_elems.size());

        for (const auto & elem : tuple_elems)
        {
            if (!isNumber(elem))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "In function {} tuple elements must be numbers.",
                    getName());
        }

        if (arguments.size() == 2 && !isNumber(arguments[1].get()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "In function {} second argument must be a number.",
                    getName());

        const auto float_64_ptr = std::make_shared<DataTypeFloat64>();

        return std::make_shared<DataTypeTuple>(DataTypes(channels_num, float_64_ptr));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto float_64_ptr = std::make_shared<DataTypeFloat64>();
        const auto tuple_f64_ptr = std::make_shared<DataTypeTuple>(DataTypes(channels_num, float_64_ptr));

        const auto   tuple_f64_arg = castColumn(arguments[0], tuple_f64_ptr);
        const auto   rgb_cols = getTupleElements(*tuple_f64_arg);

        ColumnPtr gamma;
        if (arguments.size() == 2)
            gamma = castColumn(arguments[1], float_64_ptr);

        const auto & lightness_data = assert_cast<const ColumnFloat64 &>(*rgb_cols[0]).getData();
        const auto & chroma_data = assert_cast<const ColumnFloat64 &>(*rgb_cols[1]).getData();
        const auto & hue_data = assert_cast<const ColumnFloat64 &>(*rgb_cols[2]).getData();
        const auto * gamma_data = gamma ? &assert_cast<const ColumnFloat64 &>(*gamma).getData() : nullptr;

        auto col_r = ColumnFloat64::create();
        auto col_g = ColumnFloat64::create();
        auto col_b = ColumnFloat64::create();

        auto & r_data = col_r->getData();
        auto & g_data = col_g->getData();
        auto & b_data = col_b->getData();
        r_data.reserve(input_rows_count);
        g_data.reserve(input_rows_count);
        b_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            Color lch_data{lightness_data[row],
                            chroma_data[row],
                            hue_data[row]};
            Float64 gamma_cur = gamma_data ? (*gamma_data)[row] : default_gamma;
            Color res = convertOklchToSrgb(lch_data, gamma_cur);
            r_data.push_back(res[0]);
            g_data.push_back(res[1]);
            b_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_r), std::move(col_g), std::move(col_b)}));
    }

private:
    /// OKLCH -> sRGB.
    /// Follows the step-by-step pipeline described in Ottosson’s article
    /// (see constants block above for reference and matrices).
    Color convertOklchToSrgb(const Color & OKLCH, Float64 gamma) const
    {
        Float64 chroma = OKLCH[1];
        Float64 hue_rad = OKLCH[2] * deg2rad;

        Color oklab = OKLCH;

        oklab[1] = chroma * std::cos(hue_rad);
        oklab[2] = chroma * std::sin(hue_rad);

        Color lms{};
        for (size_t i = 0; i < channels_num; ++i)
        {
            for (size_t channel = 0; channel < channels_num; ++channel)
            {
                lms[i] = std::fma(oklab[channel], oklab_to_lms_base[(3 * i) + channel], lms[i]);
            }
            lms[i] = lms[i] * lms[i] * lms[i];
        }

        Color rgb{};
        for (size_t i = 0; i < channels_num; ++i)
        {
            for (size_t channel = 0; channel < channels_num; ++channel)
            {
                rgb[i] = std::fma(lms[channel], lms_to_linear_base[(3 * i) + channel], rgb[i]);
            }
        }

        if (gamma == 0)
            gamma = gamma_fallback;

        Float64 power = 1 / gamma;
        for (size_t i = 0; i < channels_num; ++i)
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
    const FunctionDocumentation description = {
        .description=R"(Converts color from OKLCH perceptual color space to sRGB color space.
Takes an optional parameter gamma, that is defaulted at 2.2 in case it is not provided. Dual of colorSRGBToOKLCH)",
        .arguments={
            {"OKLCH_tuple", R"(A 3-element tuple of numeric values representing OKLCH coordinates: L (lightness in [0...1]),
C (chroma >= 0), and H (hue in degrees [0...360]))"},
            {"gamma", "Optional gamma exponent for sRGB transfer function. Defaults to 2.2 if omitted."},
        },
        .returned_value{"Returns a 3-element tuple of sRGB values", {"Tuple(Float64, Float64, Float64)"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Other
    };
    factory.registerFunction<FunctionColorOKLCHToSRGB>(description);
}
}
