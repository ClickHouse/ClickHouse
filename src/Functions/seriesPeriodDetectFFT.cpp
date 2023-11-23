#include "config.h"

#if USE_POCKETFFT
#    ifdef __clang__
#        pragma clang diagnostic push
#        pragma clang diagnostic ignored "-Wshadow"
#        pragma clang diagnostic ignored "-Wextra-semi-stmt"
#        pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#    endif

#    include <pocketfft_hdronly.h>

#    ifdef __clang__
#        pragma clang diagnostic pop
#    endif

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
extern const int BAD_ARGUMENTS;
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
        FunctionArgumentDescriptors args{{"time_series", &isArray<IDataType>, nullptr, "Array"}};
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        ColumnPtr array_ptr = arguments[0].column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

        const IColumn & src_data = array->getData();

        auto res = ColumnFloat64::create(1);
        auto & res_data = res->getData();

        Float64 period;

        if (executeNumber<UInt8>(src_data, period) || executeNumber<UInt16>(src_data, period) || executeNumber<UInt32>(src_data, period)
            || executeNumber<UInt64>(src_data, period) || executeNumber<Int8>(src_data, period) || executeNumber<Int16>(src_data, period)
            || executeNumber<Int32>(src_data, period) || executeNumber<Int64>(src_data, period) || executeNumber<Float32>(src_data, period)
            || executeNumber<Float64>(src_data, period))
        {
            res_data[0] = period;
            return res;
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
    }

    template <typename T>
    bool executeNumber(const IColumn & src_data, Float64 & period) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        size_t len = src_vec.size();
        if (len < 4)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least four data points are needed for function {}", getName());

        std::vector<Float64> src(src_vec.begin(), src_vec.end());
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

        std::vector<double> xfreq(spec_len);
        double step = 0.5 / (spec_len - 1);
        for (size_t i = 0; i < spec_len; ++i)
            xfreq[i] = i * step;

        auto freq = xfreq[idx];

        period = std::round(1 / freq);
        return true;
    }
};

REGISTER_FUNCTION(SeriesPeriodDetectFFT)
{
    factory.registerFunction<FunctionSeriesPeriodDetectFFT>(FunctionDocumentation{
        .description = R"(
Detects period in time series data using FFT.)",
        .categories{"Time series analysis"}});
}
}
#endif
