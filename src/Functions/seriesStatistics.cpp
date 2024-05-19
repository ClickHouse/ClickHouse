#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>
#include "Columns/ColumnVector.h"
#include "Columns/IColumn.h"
#include "DataTypes/IDataType.h"
#include "base/types.h"
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


            
/*methodName(series, number_to_predict, params, [fill_gaps]) -> Array[values]
methodName(series, times, times_to_predict, params, [fill_gaps]) -> Array[values]
*/

template<typename SeriesSequentialStatistics>
class FunctionSeriesStatistics : public IFunction
{
public:
    static constexpr auto name = SeriesSequentialStatistics::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSeriesStatistics<SeriesSequentialStatistics>>(); }

    std::string getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return SeriesSequentialStatistics::paramsCount + 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        
        FunctionArgumentDescriptors mandatory_args{{"series", &isArray, nullptr, "Array"}};
        for (auto arg_name : SeriesSequentialStatistics::paramsNames){
            FunctionArgumentDescriptor arg{arg_name, &isNativeNumber, isColumnConst, "Number"};
            mandatory_args.push_back(arg);
        }
        validateFunctionArgumentTypes(*this, arguments, mandatory_args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr series = arguments[0].column;
        const ColumnArray * series_arr = checkAndGetColumn<ColumnArray>(series.get());

        const IColumn & series_data = series_arr->getData();
        const ColumnArray::Offsets & series_offsets = series_arr->getOffsets();

        ColumnPtr col_res;
        if (input_rows_count == 0)
            return ColumnArray::create(ColumnFloat64::create());
                
        SeriesSequentialStatistics estimator(arguments, 1);

        if (executeNumber<UInt8>(series_data, series_offsets,  estimator, col_res)
            || executeNumber<UInt16>(series_data, series_offsets,  estimator, col_res)
            || executeNumber<UInt32>(series_data, series_offsets,  estimator, col_res)
            || executeNumber<UInt64>(series_data,  series_offsets, estimator, col_res)
            || executeNumber<Int8>(series_data,  series_offsets, estimator,  col_res)
            || executeNumber<Int16>(series_data, series_offsets,  estimator, col_res)
            || executeNumber<Int32>(series_data, series_offsets,  estimator, col_res)
            || executeNumber<Int64>(series_data,  series_offsets, estimator, col_res)
            || executeNumber<Float32>(series_data, series_offsets,  estimator,  col_res)
            || executeNumber<Float64>(series_data,  series_offsets, estimator, col_res))
        {
            return col_res;
        }
        else 
        {
            throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
        }
    }

private:

    template <typename T>
    bool executeNumber(
        const IColumn & series_data,
        const ColumnArray::Offsets & series_offsets,
        SeriesSequentialStatistics & estimator,
        ColumnPtr & res_ptr) const
    {
        const ColumnVector<T> * series_concrete = checkAndGetColumn<ColumnVector<T>>(&series_data);
        if (!series_concrete)
            return false;

        const PaddedPODArray<T> & series_vec =  series_concrete->getData();

        auto res = ColumnFloat64::create();
        auto & res_data = res->getData();

        ColumnArray::ColumnOffsets::MutablePtr res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = res_offsets->getData();

        ColumnArray::Offset prev_offset = 0;
        for (auto cur_offset : series_offsets)
        {
            chassert(prev_offset <= cur_offset);
            bool first = true;
            for (size_t id = prev_offset; id < cur_offset; ++id) 
            {
                Float64 value = series_vec[id];
                if (first) 
                {
                    res_data.push_back(estimator.init(value));
                    first = false;
                }
                else
                {
                    res_data.push_back(estimator.add(value));
                }
            }
            res_offsets_data.push_back(res_data.size());
            prev_offset = cur_offset;
        }

        res_ptr = ColumnArray::create(std::move(res), std::move(res_offsets));
        return true;
    }
};

namespace seriesStatisticsHelper
{
bool is01RangeFloat(Float64 value) {
    if (isnan(value) || !isFinite(value) || value < 0.0 || value > 1.0) 
        return false;
    return true;
}
}

struct SeriesEmaImpl 
{
    static constexpr auto name = "seriesEMA";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char*, paramsCount> paramsNames = {"alpha"};
    static constexpr bool needs_fit = false;
    
    Float64 alpha;
    Float64 value;

    SeriesEmaImpl(const ColumnsWithTypeAndName & arguments, size_t offset) 
    {
        alpha = arguments[offset].column->getFloat64(0);

        if (!seriesStatisticsHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);        
    }


    Float64 init(Float64 value_)
    {
        value = value_;
        return value;
    }
    
    Float64 add(Float64 new_value)
    {
        value = alpha * new_value + (1 - alpha) * value;
        return value;
    }
};

REGISTER_FUNCTION(seriesStatistics)
{
    factory.registerFunction<FunctionSeriesStatistics<SeriesEmaImpl>>(FunctionDocumentation{
        .description =  R"(
Holts method for Time Series analysus.
Usage: HoltForecast(time_series, alpha, betta) -> (l_n, b_n).
)",
        .categories{"Time series analysis"}});
}
}
