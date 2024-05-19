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
#include "Columns/ColumnNullable.h"
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

template<typename ETSForecastingPolicy>
class FunctionETSForecast : public IFunction
{
public:
    static constexpr auto name = ETSForecastingPolicy::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionETSForecast<ETSForecastingPolicy>>(); }

    std::string getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool isSeriesTimed(const ColumnsWithTypeAndName & arguments) const {
        FunctionArgumentDescriptor times{"times", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"};
        FunctionArgumentDescriptor number_to_predict{"number_to_predict", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), nullptr, "Number"};
        if (times.isValid(arguments[1].type, arguments[1].column) != 0) 
        {
            if (number_to_predict.isValid(arguments[1].type, arguments[1].column) != 0) 
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Second argument of function {} must be either Array or Float{}.",
                    getName(),
                    (arguments[1].type ? ", got " + arguments[1].type->getName() : String{}));
            
            return false;
        }
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t min_params = 2 + ETSForecastingPolicy::paramsCount;
        size_t max_params = 4 + ETSForecastingPolicy::paramsCount; // TIMED 4+

        if (arguments.size() < min_params || arguments.size() > max_params)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes from {} to {} arguments; passed {}.",
                getName(),
                min_params,
                max_params,
                arguments.size());

        bool is_timed = isSeriesTimed(arguments);
        
        if (is_timed) 
        {
            FunctionArgumentDescriptors mandatory_args{
                {"series", &isArray, nullptr, "Array"},
                {"times", &isArray, nullptr, "Array"},
                {"times_to_predict", &isArray, nullptr, "Array"}};
            for (auto arg_name : ETSForecastingPolicy::paramsNames){
                FunctionArgumentDescriptor arg{arg_name, &isNativeNumber, isColumnConst, "Number"};
                mandatory_args.push_back(arg);
            }
            FunctionArgumentDescriptors optional_args{{"fill_gaps", &isUInt, isColumnConst, "String"}};
            validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);
        }
        else 
        {
            FunctionArgumentDescriptors mandatory_args{
                {"series", &isArray, nullptr, "Array"},
                {"number_to_predict", &isNumber, isColumnConst, "Number"}};
            for (auto arg_name : ETSForecastingPolicy::paramsNames){
                FunctionArgumentDescriptor arg{arg_name, &isNativeNumber, isColumnConst, "Number"};
                mandatory_args.push_back(arg);
            }
            FunctionArgumentDescriptors optional_args{{"fill_gaps", &isUInt, nullptr, "String"}};
            validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);
        }
        
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
        
        bool is_timed = isSeriesTimed(arguments);

        size_t params_offset = is_timed ? 3 : 2;
        
        ETSForecastingPolicy forcaster(arguments, params_offset);

    
        size_t fill_gaps_offset = params_offset + ETSForecastingPolicy::paramsCount;
        bool fill_gaps = false;
        if (fill_gaps_offset < arguments.size()) {
            fill_gaps = arguments[fill_gaps_offset].column->getUInt(0);
            if (fill_gaps == 0) 
            {
                fill_gaps = false;
            }
            else if (fill_gaps == 1) 
            {
                fill_gaps = true;
            }
            else 
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Last optional for function {} must be 0 or 1.",
                    getName());
            }
        }
    
        if (!is_timed) {
            size_t number_to_predict = arguments[1].column->getUInt(0);
            if (executeNumber<UInt8>(series_data, series_offsets,  forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<UInt16>(series_data, series_offsets,  forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<UInt32>(series_data, series_offsets,  forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<UInt64>(series_data,  series_offsets, forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Int8>(series_data,  series_offsets, forcaster, number_to_predict, fill_gaps,  col_res)
                || executeNumber<Int16>(series_data, series_offsets,  forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Int32>(series_data, series_offsets,  forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Int64>(series_data,  series_offsets, forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Float32>(series_data, series_offsets,  forcaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Float64>(series_data,  series_offsets, forcaster, number_to_predict, fill_gaps, col_res))
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
        throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Timed version is not implemented yet, function {}",
                    getName());
    }

private:

    template <typename T>
    bool executeNumber(
        const IColumn & series_column,
        const ColumnArray::Offsets & series_offsets,
        ETSForecastingPolicy & forcaster,
        size_t number_to_predict,
        bool fill_gaps,
        ColumnPtr & res_ptr) const
    {

        const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(series_column);
        const IColumn * series_data;
        const char8_t * null_map = nullptr;

        if (nullable_col)
        {
            series_data = &nullable_col->getNestedColumn();
            null_map = nullable_col->getNullMapData().data();
        }
        else
        {
            series_data = &series_column;
        }

        const ColumnVector<T> * series_concrete = checkAndGetColumn<ColumnVector<T>>(series_data);
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
            size_t ts = 0;
            for (size_t id = prev_offset; id < cur_offset; ++id, ++ts) {
                Float64 value = series_vec[id];
                if (null_map && null_map[id]) {
                    if (first || !fill_gaps)
                        continue;
                    forcaster.add(forcaster.forecast(ts));
                }
                else {
                    if (first) 
                    {
                        forcaster.init(value);
                        first = false;
                    }
                    else
                    {
                        forcaster.add(value);
                    }
                }
                forcaster.updateTs(ts);
            }
            if (first) 
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "No non-null data points provided for function {}",
                    getName());
            }
            for (size_t delta = 0; delta < number_to_predict; ++delta) {
                Float64 value = forcaster.forecast(number_to_predict + delta);
                res_data.push_back(value);
            }
            res_offsets_data.push_back(res_data.size());

            prev_offset = cur_offset;
        }

        res_ptr = ColumnArray::create(std::move(res), std::move(res_offsets));
        return true;
    }
};
namespace ETSForecastHelper {

bool is01RangeFloat(Float64 value) {
    if (isnan(value) || !isFinite(value) || value < 0.0 || value > 1.0) 
        return false;
    return true;
}

}

struct HoltsForecasterImpl 
{
    static constexpr auto name = "HoltsWinters";
    static constexpr size_t paramsCount = 2;
    static constexpr std::array<const char*, paramsCount> paramsNames = {"alpha", "betta"};
    static constexpr bool needs_fit = false;
    
    Float64 alpha, betta;
    Float64 trend, level;
    Float64 last_ts;

    HoltsForecasterImpl(const ColumnsWithTypeAndName & arguments, size_t offset) 
    {
        alpha = arguments[offset].column->getFloat64(0);
        betta = arguments[offset + 1].column->getFloat64(0);

        if (!ETSForecastHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);

        
        if (!ETSForecastHelper::is01RangeFloat(betta))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The betta argument of function {} must be in range [0, 1]", name);
    }

    void updateTs(Float64 ts)
    {
        last_ts = ts;
    }

    void init(Float64 value)
    {
        level = value;
        trend = 0;
    }
    
    void add(Float64 value)
    {
        Float64 new_level = alpha * value + (1.0 - alpha) * (level + trend);
        trend = betta * (new_level - level) + (1.0 - betta) * trend;
        level = new_level;
    }

    Float64 forecast(Float64 ts) const 
    {
        return level + trend * (ts - last_ts);
    }
};

REGISTER_FUNCTION(ETSForecast)
{
    factory.registerFunction<FunctionETSForecast<HoltsForecasterImpl>>(FunctionDocumentation{
        .description =  R"(
Holts method for Time Series analysus.
Usage: HoltForecast(time_series, alpha, betta) -> (l_n, b_n).
)",
        .categories{"Time series analysis"}});
}
}
