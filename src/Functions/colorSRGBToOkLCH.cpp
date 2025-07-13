#include <Core/Field.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
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

/** Function that converts color from sRGB color space
  * to perceptual OkLCH color space.
  * Returns a tuple of type Tuple(Float64, Float64, Float64).
  */

namespace
{
class FunctionColorSRGBToOkLCH : public ITupleFunction
{
private:

    static constexpr Float64 eps          = 1e-6;
    static constexpr Float64 rad2deg      = 57.2957795131;
    static constexpr size_t  channels_num = 3;

    using Colors = std::array<Float64, channels_num>;
    using Mat3x3 = std::array<Float64, 9>;

    static constexpr Mat3x3 rgb_to_lms = {0.4122214708, 0.5363325363, 0.0514459929,
                                          0.2119034982, 0.6806995451, 0.1073969566,
                                          0.0883024619, 0.2817188376, 0.6299787005};

    static constexpr Mat3x3 lms_to_oklab_base = {0.2104542553,  0.7936177850, -0.0040720468,
                                                 1.9779984951, -2.4285922050,  0.45059370996,
                                                 0.0259040371,  0.7827717662, -0.8086757660};

public:

    static constexpr auto name = "colorSRGBToOkLCH";

    explicit FunctionColorSRGBToOkLCH(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionColorSRGBToOkLCH>(context_); }

    String  getName() const override { return name; }
    bool    isVariadic() const override { return true; }
    size_t  getNumberOfArguments() const override { return 0; }
    bool    useDefaultImplementationForConstants() const override { return true; }
    bool    isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || 2 < arguments.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 or 2 arguments, {} provided.",
                getName(), arguments.size());

        const auto * first_arg = arguments[0].get();

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
        const auto float_64_ptr  = std::make_shared<DataTypeFloat64>();
        const auto tuple_f64_ptr = std::make_shared<DataTypeTuple>(DataTypes(channels_num, float_64_ptr));

        const auto   tuple_f64_arg    = castColumn(arguments[0], tuple_f64_ptr);
        const auto   rgb_cols         = getTupleElements(*tuple_f64_arg);

        ColumnPtr gamma;
        if (arguments.size() == 2)
        {
            gamma = castColumn(arguments[1], float_64_ptr);
        }
        else
        {
            gamma = ColumnFloat64::create(input_rows_count, 2.2);
        }

        const auto & r_data     = assert_cast<const ColumnFloat64 &>(*rgb_cols[0]).getData();
        const auto & g_data     = assert_cast<const ColumnFloat64 &>(*rgb_cols[1]).getData();
        const auto & b_data     = assert_cast<const ColumnFloat64 &>(*rgb_cols[2]).getData();
        const auto & gamma_data = assert_cast<const ColumnFloat64 &>(*gamma).getData();

        auto col_l = ColumnFloat64::create();
        auto col_c = ColumnFloat64::create();
        auto col_h = ColumnFloat64::create();

        auto & l_data = col_l->getData();
        auto & c_data = col_c->getData();
        auto & h_data = col_h->getData();
        l_data.reserve(input_rows_count);
        c_data.reserve(input_rows_count);
        h_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            Colors rgb_data{r_data[row],
                            g_data[row],
                            b_data[row]};
            Colors res = srgbToOklchBase(rgb_data, gamma_data[row]);
            l_data.push_back(res[0]);
            c_data.push_back(res[1]);
            h_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_l), std::move(col_c), std::move(col_h)}));
    }

private:

    Colors srgbToOklchBase(const Colors & rgb, Float64 gamma) const
    {
        Colors rgb_lin;
        for (size_t i = 0; i < channels_num; ++i)
        {
            rgb_lin[i] = std::pow(rgb[i] / 255.0, gamma);
        }

        Colors lms{};
        for (size_t i = 0; i < channels_num; ++i)
        {
            for (size_t channel = 0; channel < channels_num; ++channel)
            {
                lms[i] = std::fma(rgb_lin[channel], rgb_to_lms[(3 * i) + channel], lms[i]);
            }
            lms[i] = std::cbrt(lms[i]);
        }

        Colors oklab{};
        for (size_t i = 0; i < channels_num; ++i)
        {
            for (size_t channel = 0; channel < channels_num; ++channel)
            {
                oklab[i] = std::fma(lms[channel], lms_to_oklab_base[(3 * i) + channel], oklab[i]);
            }
        }

        Colors oklch = oklab;

        Float64 a = oklab[1];
        Float64 b = oklab[2];

        oklch[1] = std::sqrt(a * a + b * b);
        if (oklch[1] >= eps)
        {
            Float64 degrees = std::atan2(b, a) * rad2deg;
            oklch[2]  = std::fmod(degrees + 360.0, 360.0);
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

REGISTER_FUNCTION(ColorSRGBToOkLCH)
{
    const FunctionDocumentation description = {
        .description=R"(Converts converts color from sRGB color space to perceptual OkLCH color space.
Takes an optional parameter gamma, that is defaulted at 2.2 in case it is not provided. Dual of colorOkLCHToSRGB)",
        .arguments={
            {"rgb_tuple", "A 3-element tuple of numeric values (e.g. integers in rage [0...255])"},
            {"gamma", "Optional gamma exponent to linearize sRGB before conversion. Defaults to 2.2."},
        },
        .returned_value{"Returns a 3-element tuple of OkLCH values", {"Tuple(Float64, Float64, Float64)"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Other,
    };
    factory.registerFunction<FunctionColorSRGBToOkLCH>(description);
}

}
