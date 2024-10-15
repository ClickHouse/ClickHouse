#include "config.h"

#if USE_POCKETFFT
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wshadow"
#    pragma clang diagnostic ignored "-Wextra-semi-stmt"
#    pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"

#    include <pocketfft_hdronly.h>

#    pragma clang diagnostic pop

#    include <cmath>
#    include <Columns/ColumnArray.h>
#    include <Columns/ColumnsNumber.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Functions/FunctionFactory.h>
#    include <Functions/FunctionHelpers.h>
#    include <Functions/IFunction.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

/*Detect Period in time series data using FFT.
 * FFT - Fast Fourier transform (https://en.wikipedia.org/wiki/Fast_Fourier_transform)
 * 1. Convert time series data to frequency domain using FFT.
 * 2. Remove the 0th(the Dc component) and n/2th the Nyquist frequency
 * 3. Find the peak value (highest) for dominant frequency component.
 * 4. Inverse of the dominant frequency component is the period.
*/

class FunctionSeriesPeriodDetectFFT : public IFunction
{
public:
    static constexpr auto name = "seriesPeriodDetectFFT";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSeriesPeriodDetectFFT>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{{"time_series", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"}};
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr array_ptr = arguments[0].column;
        const ColumnArray & array = checkAndGetColumn<ColumnArray>(*array_ptr);

        const IColumn & src_data = array.getData();
        const ColumnArray::Offsets & offsets = array.getOffsets();

        auto res = ColumnFloat64::create(input_rows_count);
        auto & res_data = res->getData();

        ColumnArray::Offset prev_src_offset = 0;

        Float64 period;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnArray::Offset curr_offset = offsets[i];
            if (executeNumbers<UInt8>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<UInt16>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<UInt32>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<UInt64>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<Int8>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<Int16>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<Int32>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<Int64>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<Float32>(src_data, period, prev_src_offset, curr_offset)
                || executeNumbers<Float64>(src_data, period, prev_src_offset, curr_offset))
            {
                res_data[i] = period;
                prev_src_offset = curr_offset;
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(),
                    getName());
        }
        return res;
    }

    template <typename T>
    bool executeNumbers(const IColumn & src_data, Float64 & period, ColumnArray::Offset & start, ColumnArray::Offset & end) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        chassert(start <= end);
        size_t len = end - start;
        if (len < 4)
        {
            period = NAN; // At least four data points are required to detect period
            return true;
        }

        std::vector<Float64> src((src_vec.begin() + start), (src_vec.begin() + end));
        std::vector<std::complex<double>> out((len / 2) + 1);

        pocketfft::shape_t shape{len};

        pocketfft::shape_t axes;
        axes.reserve(shape.size());
        for (size_t i = 0; i < shape.size(); ++i)
            axes.push_back(i);

        pocketfft::stride_t stride_src{sizeof(double)};
        pocketfft::stride_t stride_out{sizeof(std::complex<double>)};

        pocketfft::r2c(shape, stride_src, stride_out, axes, pocketfft::FORWARD, src.data(), out.data(), static_cast<double>(1));

        size_t spec_len = (len - 1) / 2; //removing the nyquist element when len is even

        double max_mag = 0;
        size_t idx = 1;
        for (size_t i = 1; i < spec_len; ++i)
        {
            double magnitude = sqrt(out[i].real() * out[i].real() + out[i].imag() * out[i].imag());
            if (magnitude > max_mag)
            {
                max_mag = magnitude;
                idx = i;
            }
        }

        // In case all FFT values are zero, it means the input signal is flat.
        // It implies the period of the series should be 0.
        if (max_mag == 0)
        {
            period = 0;
            return true;
        }

        double step = 0.5 / (spec_len - 1);
        auto freq = idx * step;

        period = std::round(1 / freq);
        return true;
    }
};

REGISTER_FUNCTION(SeriesPeriodDetectFFT)
{
    factory.registerFunction<FunctionSeriesPeriodDetectFFT>(FunctionDocumentation{
        .description = R"(
Finds the period of the given time series data using FFT
FFT - Fast Fourier transform (https://en.wikipedia.org/wiki/Fast_Fourier_transform)

**Syntax**

``` sql
seriesPeriodDetectFFT(series);
```

**Arguments**

- `series` - An array of numeric values

**Returned value**

- A real value equal to the period of time series
- Returns NAN when number of data points are less than four.

Type: [Float64](../../sql-reference/data-types/float.md).

**Examples**

Query:

``` sql
SELECT seriesPeriodDetectFFT([1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6, 1, 4, 6]) AS print_0;
```

Result:

``` text
┌───────────print_0──────┐
│                      3 │
└────────────────────────┘
```

``` sql
SELECT seriesPeriodDetectFFT(arrayMap(x -> abs((x % 6) - 3), range(1000))) AS print_0;
```

Result:

``` text
┌─print_0─┐
│       6 │
└─────────┘
```
)",
        .categories{"Time series analysis"}});
}
}
#endif
