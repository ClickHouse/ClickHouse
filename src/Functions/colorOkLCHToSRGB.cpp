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

namespace 
{
class FunctionOkLCHToSRGB : public ITupleFunction
{
private:

    static constexpr Float64 eps          = 1e-6;
    static constexpr Float64 deg2rad      = 0.01745329251;
    static constexpr size_t  channels_num = 3;

    using Colors = std::array<Float64, channels_num>;
    using Mat3x3 = std::array<Float64, 9>;


    static constexpr Mat3x3 oklab_to_lms = {1,  0.3963377774,  0.2158037573,
                                            1, -0.1055613458, -0.0638541728,
                                            1, -0.0894841775, -1.2914855480};
    
    static constexpr Mat3x3 lms_to_linear_rgb_base = {4.0767416621, -3.3077115913,  0.2309699292,
                                                     -1.2684380046,  2.6097574011, -0.3413193965,
                                                     -0.0041960863, -0.7034186147,  1.7076147010};

public:

    static constexpr auto name = "colorOkLCHToSRGB";

    explicit FunctionOkLCHToSRGB(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionOkLCHToSRGB>(context_); }

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
                "First arguement for function {} must be a tuple.",
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
                    "In fuction {} tuple elements must be numbers.",
                    getName());
        }
        
        if (arguments.size() == 2 && !isNumber(arguments[1].get()) ) 
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

        const auto & l_data     = assert_cast<const ColumnFloat64 &>(*rgb_cols[0]).getData();
        const auto & c_data     = assert_cast<const ColumnFloat64 &>(*rgb_cols[1]).getData();
        const auto & h_data     = assert_cast<const ColumnFloat64 &>(*rgb_cols[2]).getData();
        const auto & gamma_data = assert_cast<const ColumnFloat64 &>(*gamma).getData();

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
            Colors lch_data{l_data[row], 
                            c_data[row],
                            h_data[row]};
            Colors res = oklch_to_srgb_base(lch_data, gamma_data[row]);
            r_data.push_back(res[0]);
            g_data.push_back(res[1]);
            b_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_r), std::move(col_g), std::move(col_b)}));
    }

private:

    Colors oklch_to_srgb_base(const Colors & oklch, Float64 gamma) const
    {
        Float64 c = oklch[1];
        Float64 h = oklch[2] * deg2rad; 

        Colors oklab = oklch;

        oklab[1] = c * std::cos(h);
        oklab[2] = c * std::sin(h);

        Colors lms{};
        for (size_t i = 0; i < channels_num; ++i) 
        {
            for (size_t channel = 0; channel < channels_num; ++channel) 
            {
                lms[i] += oklab[channel] * oklab_to_lms[(3 * i) + channel];
            } 
            lms[i] = std::pow(lms[i], 3);  
        }
 
        Colors rgb{};
        for (size_t i = 0; i < channels_num; ++i) 
        {
            for (size_t channel = 0; channel < channels_num; ++channel) 
            {
                rgb[i] += lms[channel] * lms_to_linear_rgb_base[(3 * i) + channel];
            }
        }

        if (gamma == 0) 
        {
            gamma = eps;
        }

        for (size_t i = 0; i < channels_num; ++i) 
        {
            Float64 power = 1 / gamma;
            rgb[i] = std::pow(rgb[i], power) * 255.0;     
        }

        return rgb;
    }

};

}

REGISTER_FUNCTION(FunctionOkLCHToSRGB)
{
    const FunctionDocumentation description = {
        .description=R"(Convers OkLCH color into sRGB color space. Takes an optional parameter
gamma, that is defaulted at 2.2 in case not provided. Dual of colorSRGBToOkLCH)",
        .arguments={
            {"oklch_tuple", "A 3-element tuple of numeric values representing OKLCH coordinates: L (lightness in [0...1]), C (chroma >= 0), and H (hue in degrees [0...360])"},
            {"gamma", "Optional gamma exponent for sRGB transfer function. Defaults to 2.2 if ommited."},
        },
        .returned_value{"Returns a 3-element tuple of sRGB values", {"Tuple(Float64, Float64, Float64)"}},
        .category = FunctionDocumentation::Category::Other
    };
    factory.registerFunction<FunctionOkLCHToSRGB>(description);
}

}
