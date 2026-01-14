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

/** Base class for functions that convert color from sRGB color space to perceptual color spaces.
  * Uses CRTP (Curiously Recurring Template Pattern) to allow derived classes to provide
  * their specific color space conversion logic while sharing common validation and execution code.
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

            /// Call derived class conversion function
            ColorConversion::Color res = static_cast<const Derived*>(this)->convertFromSrgb(rgb_data, gamma_cur);

            channel0_data.push_back(res[0]);
            channel1_data.push_back(res[1]);
            channel2_data.push_back(res[2]);
        }

        return ColumnTuple::create(Columns({std::move(col_channel0), std::move(col_channel1), std::move(col_channel2)}));
    }

protected:
    /// Common helper: sRGB -> OKLab conversion (steps 1-2 of the conversion pipeline)
    /// This is shared by both OKLAB and OKLCH conversions
    static ColorConversion::Color srgbToOklab(const ColorConversion::Color & rgb, Float64 gamma)
    {
        /// Step 1: sRGB to Linear sRGB (gamma correction)
        ColorConversion::Color rgb_lin;
        for (size_t i = 0; i < ColorConversion::channels; ++i)
            rgb_lin[i] = std::pow(rgb[i] / 255.0, gamma);

        /// Step 2: Linear sRGB to LMS (cone response)
        ColorConversion::Color lms{};
        for (size_t i = 0; i < ColorConversion::channels; ++i)
        {
            for (size_t channel = 0; channel < ColorConversion::channels; ++channel)
                lms[i] = std::fma(rgb_lin[channel], ColorConversion::linear_to_lms_base[(3 * i) + channel], lms[i]);
            lms[i] = std::cbrt(lms[i]);
        }

        /// Step 3: LMS to OKLab
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
