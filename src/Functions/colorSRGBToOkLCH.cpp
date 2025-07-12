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
class FunctionColorSRGBToOkLCH : public ITupleFunction
{
private:

    static constexpr Float64 eps = 1e-6;
    static constexpr Float64 rad2deg = 57.2957795131;  
    static constexpr size_t channels_num = 3;

    using Colors = std::array<Float64, channels_num>;
    using Mat3x3 = std::array<Float64, 9>;

    static constexpr Mat3x3 matrix1 = { 0.4122214708, 0.5363325363, 0.0514459929,
                                        0.2119034982, 0.6806995451, 0.1073969566,
                                        0.0883024619, 0.2817188376, 0.6299787005 };
    
    static constexpr Mat3x3 matrix2 = { 0.2104542553, 0.7936177850, -0.0040720468,
                                        1.9779984951, -2.4285922050, 0.45059370996,
                                        0.0259040371, 0.7827717662, -0.8086757660 };

public:

    static constexpr auto name = "colorSRGBToOKLCH";

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
                "First arguement for function {} must be a tuple.",
                getName());

        const auto * type = checkAndGetDataType<DataTypeTuple>(first_arg);

        const auto & tuple_elems  = type->getElements();
        
        if (tuple_elems.size() != channels_num) 
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The tuple argument for function {} must be of size {}.",
                getName(), channels_num);

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
        const auto * tuple_type       = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & tuple_elem_types = tuple_type->getElements();
        const auto   float_type       = std::make_shared<DataTypeFloat64>();

        const auto   tuple_elems      = getTupleElements(*arguments[0].column);
        Columns      rgb_float(channels_num);

        for (size_t i = 0; i < channels_num; ++i) 
        {
            ColumnWithTypeAndName  tuple_elem_wrapped{tuple_elems[i], tuple_elem_types[i], ""};
            rgb_float[i] = castColumn(tuple_elem_wrapped, float_type);
        }

        ColumnPtr gamma;
        if (arguments.size() == 2) 
        {
            gamma = castColumn(arguments[1], float_type);
        }
        else 
        {
            gamma = ColumnFloat64::create(input_rows_count, 2.2);
        }

        // const auto & r_data = getFloat64Data(rgb_float[0]); 
        // const auto & g_data = getFloat64Data(rgb_float[1]); 
        // const auto & b_data = getFloat64Data(rgb_float[2]); 

        const auto * gamma_float_ptr = checkAndGetColumn<ColumnFloat64>(gamma.get());
        const auto * r_float_ptr =  checkAndGetColumn<ColumnFloat64>(rgb_float[0].get());
        const auto * g_float_ptr = checkAndGetColumn<ColumnFloat64>(rgb_float[1].get());
        const auto * b_float_ptr = checkAndGetColumn<ColumnFloat64>(rgb_float[2].get());

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
            Colors rgb_data{r_float_ptr->getFloat64(row), 
                            g_float_ptr->getFloat64(row), 
                            b_float_ptr->getFloat64(row)};

            Colors res = srgb_to_oklch_base(rgb_data, gamma_float_ptr->getFloat64(row));
            l_data.push_back(res[0]);
            c_data.push_back(res[1]);
            h_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_l), std::move(col_c), std::move(col_h)}));
    }

private:

    Colors srgb_to_oklch_base(const Colors & rgb, Float64 gamma) const
    {
        Colors rgb_lin;
        for (size_t i = 0; i < channels_num; ++i) 
        {
            rgb_lin[i] = std::pow(rgb[i] / 255.0, gamma);     
        }

        Colors first_base{};
        for (size_t i = 0; i < channels_num; ++i) 
        {
            for (size_t channel = 0; channel < channels_num; ++channel) 
            {
                first_base[i] += rgb_lin[channel] * matrix1[(3 * i) + channel];
            } 
            first_base[i] = std::cbrt(first_base[i]);  
        }
 
        Colors second_base{};
        for (size_t i = 0; i < channels_num; ++i) 
        {
            for (size_t channel = 0; channel < channels_num; ++channel) 
            {
                second_base[i] += first_base[channel] * matrix2[(3 * i) + channel];
            } 
        }

        Float64 a = second_base[1];
        Float64 b = second_base[2];

        second_base[1] = std::sqrt(a * a + b * b);
        if (second_base[1] < eps) 
        {
            second_base[1] = 0;
            second_base[2] = 0;
        } 
        else 
        {
            Float64 degrees = std::atan2(b, a) * rad2deg;                  
            second_base[2]  = std::fmod(degrees + 360.0, 360.0);
        }

        return second_base;
    }

};

}

REGISTER_FUNCTION(ColorSRGBToOkLCH)
{
    factory.registerFunction<FunctionColorSRGBToOkLCH>();
}

}
