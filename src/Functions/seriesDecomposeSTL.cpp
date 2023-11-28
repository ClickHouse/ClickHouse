#include "config.h"

#if USE_SEASONAL
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#endif

#include <stl.hpp>

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
}

/*
Decompose time series data based on STL(Seasonal-Trend Decomposition Procedure Based on Loess)
*/
class FunctionSeriesDecomposeSTL : public IFunction
{
public:
    static constexpr auto name = "seriesDecomposeSTL";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSeriesDecomposeSTL>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"time-series", &isArray<IDataType>, nullptr, "Array"},
            {"period", &isNativeNumber<IDataType>, nullptr, "Number"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        ColumnPtr array_ptr = arguments[0].column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

        const IColumn & src_data = array->getData();

        Float64 period;


        auto period_ptr = arguments[1].column->convertToFullColumnIfConst();
        if (checkAndGetColumn<ColumnUInt8>(period_ptr.get()) || checkAndGetColumn<ColumnUInt16>(period_ptr.get())
            || checkAndGetColumn<ColumnUInt32>(period_ptr.get()) || checkAndGetColumn<ColumnUInt64>(period_ptr.get()))
            period = period_ptr->getUInt(0);
        else if (checkAndGetColumn<ColumnFloat32>(period_ptr.get()) || checkAndGetColumn<ColumnFloat64>(period_ptr.get()))
        {
            period = period_ptr->getFloat64(0);
            if (isNaN(period) || !std::isfinite(period) || period < 0)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal value {} for second argument of function {}. Should be a positive number",
                    period,
                    getName());
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second argument of function {}",
                arguments[1].column->getName(),
                getName());


        std::vector<Float32> seasonal;
        std::vector<Float32> trend;
        std::vector<Float32> residue;


        if (executeNumber<UInt8>(src_data, period, seasonal, trend, residue)
            || executeNumber<UInt16>(src_data, period, seasonal, trend, residue)
            || executeNumber<UInt32>(src_data, period, seasonal, trend, residue)
            || executeNumber<UInt64>(src_data, period, seasonal, trend, residue)
            || executeNumber<Int8>(src_data, period, seasonal, trend, residue)
            || executeNumber<Int16>(src_data, period, seasonal, trend, residue)
            || executeNumber<Int32>(src_data, period, seasonal, trend, residue)
            || executeNumber<Int64>(src_data, period, seasonal, trend, residue)
            || executeNumber<Float32>(src_data, period, seasonal, trend, residue)
            || executeNumber<Float64>(src_data, period, seasonal, trend, residue))
        {
            auto ret = ColumnFloat32::create();
            auto & res_data = ret->getData();

            ColumnArray::ColumnOffsets::MutablePtr col_offsets = ColumnArray::ColumnOffsets::create();
            auto & col_offsets_data = col_offsets->getData();

            auto root_offsets = ColumnArray::ColumnOffsets::create();
            auto & root_offsets_data = root_offsets->getData();

            res_data.insert(res_data.end(), seasonal.begin(), seasonal.end());
            col_offsets_data.push_back(res_data.size());

            res_data.insert(res_data.end(), trend.begin(), trend.end());
            col_offsets_data.push_back(res_data.size());

            res_data.insert(res_data.end(), residue.begin(), residue.end());
            col_offsets_data.push_back(res_data.size());

            root_offsets_data.push_back(col_offsets->size());

            ColumnArray::MutablePtr nested_array_col = ColumnArray::create(std::move(ret), std::move(col_offsets));
            return ColumnArray::create(std::move(nested_array_col), std::move(root_offsets));
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
    }

    template <typename T>
    bool executeNumber(
        const IColumn & src_data,
        Float64 period,
        std::vector<Float32> & seasonal,
        std::vector<Float32> & trend,
        std::vector<Float32> & residue) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        size_t len = src_vec.size();
        if (len < 4)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least four data points are needed for function {}", getName());
        else if (period > (len / 2))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The series should have data of at least two period lengths for function {}", getName());

        std::vector<float> src(src_vec.begin(), src_vec.end());

        try
        {
            auto res = stl::params().fit(src, static_cast<size_t>(std::round(period)));

            if (res.seasonal.empty())
                return false;

            seasonal = res.seasonal;
            trend = res.trend;
            residue = res.remainder;
            return true;
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, e.what());
        }
    }
};
REGISTER_FUNCTION(seriesDecomposeSTL)
{
    factory.registerFunction<FunctionSeriesDecomposeSTL>(FunctionDocumentation{
        .description = R"(
Decompose time series data based on STL(Seasonal-Trend Decomposition Procedure Based on Loess)",
        .categories{"Time series analysis"}});
}
}
#endif
