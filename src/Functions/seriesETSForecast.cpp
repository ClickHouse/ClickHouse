#include <cmath>
#include <cstddef>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
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

template <typename ETSForecastingPolicy>
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

    bool isSeriesTimed(const ColumnsWithTypeAndName & arguments) const
    {
        FunctionArgumentDescriptor times{"times", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"};
        FunctionArgumentDescriptor number_to_predict{
            "number_to_predict", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), nullptr, "Number"};
        if (times.isValid(arguments[1].type, arguments[1].column) != 0)
        {
            if (number_to_predict.isValid(arguments[1].type, arguments[1].column) != 0)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Second argument of function {} must be either Array or unsigned integer{}.",
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
            for (auto arg_name : ETSForecastingPolicy::paramsNames)
            {
                FunctionArgumentDescriptor arg{arg_name, &isNativeNumber, isColumnConst, "Number"};
                mandatory_args.push_back(arg);
            }
            FunctionArgumentDescriptors optional_args{{"fill_gaps", &isUInt, isColumnConst, "String"}};
            validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);
        }
        else
        {
            FunctionArgumentDescriptors mandatory_args{
                {"series", &isArray, nullptr, "Array"}, {"number_to_predict", &isNumber, isColumnConst, "Number"}};
            for (auto arg_name : ETSForecastingPolicy::paramsNames)
            {
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

        const IColumn & series_column = series_arr->getData();
        const ColumnArray::Offsets & series_offsets = series_arr->getOffsets();


        ColumnPtr col_res;
        if (input_rows_count == 0)
            return ColumnArray::create(ColumnFloat64::create());

        bool is_timed = isSeriesTimed(arguments);

        size_t params_offset = is_timed ? 3 : 2;

        ETSForecastingPolicy forecaster(arguments, params_offset);


        size_t fill_gaps_offset = params_offset + ETSForecastingPolicy::paramsCount;
        bool fill_gaps = false;
        if (fill_gaps_offset < arguments.size())
        {
            fill_gaps = arguments[fill_gaps_offset].column->getUInt(0);
            if (fill_gaps == 0)
                fill_gaps = false;
            else if (fill_gaps == 1)
                fill_gaps = true;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Last optional for function {} must be 0 or 1.", getName());
        }

        if (!is_timed)
        {
            size_t number_to_predict = arguments[1].column->getUInt(0);
            if (executeNumber<UInt8>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<UInt16>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<UInt32>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<UInt64>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Int8>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Int16>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Int32>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Int64>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Float32>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res)
                || executeNumber<Float64>(series_column, series_offsets, forecaster, number_to_predict, fill_gaps, col_res))
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
        else
        {
            ColumnPtr times = arguments[1].column;
            const ColumnArray * times_arr = checkAndGetColumn<ColumnArray>(times.get());

            const IColumn & times_column = times_arr->getData();
            const ColumnArray::Offsets & times_offsets = times_arr->getOffsets();

            ColumnPtr times_predict = arguments[2].column;
            const ColumnArray * times_predict_arr = checkAndGetColumn<ColumnArray>(times_predict.get());

            const IColumn & times_predict_column = times_predict_arr->getData();
            const ColumnArray::Offsets & times_predict_offsets = times_predict_arr->getOffsets();


            if (executeNumberTimed<UInt8>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<UInt16>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<UInt32>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<UInt64>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<Int8>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<Int16>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<Int32>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<Int64>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<Float32>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res)
                || executeNumberTimed<Float64>(
                    series_column,
                    series_offsets,
                    times_column,
                    times_offsets,
                    forecaster,
                    times_predict_column,
                    times_predict_offsets,
                    fill_gaps,
                    col_res))
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
    }

private:
    template <typename T>
    bool executeNumber(
        const IColumn & series_column,
        const ColumnArray::Offsets & series_offsets,
        ETSForecastingPolicy & forecaster,
        size_t number_to_predict,
        bool fill_gaps,
        ColumnPtr & res_ptr) const
    {
        const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(&series_column);
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

        const PaddedPODArray<T> & series_vec = series_concrete->getData();

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
            for (size_t id = prev_offset; id < cur_offset; ++id, ++ts)
            {
                Float64 value = series_vec[id];
                if (null_map && null_map[id])
                {
                    if (first || !fill_gaps)
                        continue;
                    forecaster.add(forecaster.forecast(ts));
                }
                else if (first)
                {
                    forecaster.init(value);
                    first = false;
                }
                else
                {
                    forecaster.add(value);
                }
                forecaster.updateTs(ts);
            }
            if (first)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No non-null data points provided for function {}", getName());
            for (size_t delta = 0; delta < number_to_predict; ++delta)
            {
                Float64 value = forecaster.forecast(ts + delta);
                res_data.push_back(value);
            }
            res_offsets_data.push_back(res_data.size());

            prev_offset = cur_offset;
        }

        res_ptr = ColumnArray::create(std::move(res), std::move(res_offsets));
        return true;
    }

    template <typename T>
    bool executeNumberTimed(
        const IColumn & series_column,
        const ColumnArray::Offsets & series_offsets,
        const IColumn & times_column,
        const ColumnArray::Offsets & times_offsets,
        ETSForecastingPolicy & forecaster,
        const IColumn & times_predict_column,
        const ColumnArray::Offsets & times_predict_offsets,
        bool fill_gaps,
        ColumnPtr & res_ptr) const
    {
        const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(&series_column);
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
        const PaddedPODArray<T> & series_vec = series_concrete->getData();


        const ColumnVector<UInt64> * times_concrete = checkAndGetColumn<ColumnVector<UInt64>>(&times_column);
        if (!times_concrete)
            return false;
        const PaddedPODArray<UInt64> & times_vec = times_concrete->getData();

        const ColumnVector<UInt64> * times_predict_concrete = checkAndGetColumn<ColumnVector<UInt64>>(&times_predict_column);
        if (!times_concrete)
            return false;
        const PaddedPODArray<UInt64> & times_predict_vec = times_predict_concrete->getData();

        if (times_offsets != series_offsets)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of series and times for function {} do not match", getName());
        if (series_offsets.size() != times_predict_offsets.size())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Numbers of given arrays for function {} do not match (times_to_predict and times)", getName());
        }

        auto res = ColumnFloat64::create();
        auto & res_data = res->getData();

        ColumnArray::ColumnOffsets::MutablePtr res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = res_offsets->getData();

        ColumnArray::Offset prev_offset_input = 0;
        ColumnArray::Offset prev_offset_output = 0;
        for (size_t index = 0; index < series_offsets.size(); ++index)
        {
            auto cur_offset_input = series_offsets[index];
            auto cur_offset_output = times_predict_offsets[index];

            chassert(prev_offset_input <= cur_offset_input);
            chassert(prev_offset_output <= cur_offset_output);

            bool first = true;
            for (size_t id = prev_offset_input; id < cur_offset_input; ++id)
            {
                Float64 value = series_vec[id];
                size_t ts = times_vec[id];
                if (null_map && null_map[id])
                {
                    if (first || !fill_gaps)
                        continue;
                    forecaster.add(forecaster.forecast(ts));
                }
                else if (first)
                {
                    forecaster.init(value);
                    first = false;
                }
                else
                {
                    forecaster.add(value);
                }
                forecaster.updateTs(ts);
            }
            if (first)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No non-null data points provided for function {}", getName());
            for (size_t id = prev_offset_output; id < cur_offset_output; ++id)
            {
                Float64 value = forecaster.forecast(times_predict_vec[id]);
                res_data.push_back(value);
            }
            res_offsets_data.push_back(res_data.size());

            prev_offset_input = cur_offset_input;
            prev_offset_output = cur_offset_output;
        }

        res_ptr = ColumnArray::create(std::move(res), std::move(res_offsets));
        return true;
    }
};
namespace ETSForecastHelper
{

bool is01RangeFloat(Float64 value)
{
    if (isnan(value) || !isFinite(value) || value < 0.0 || value > 1.0)
        return false;
    return true;
}

Float64 calculateAdditivedamped(Float64 phi, UInt64 time)
{
    if (phi == 1.0)
        return time;
    return (pow(phi, time + 1) - 1) / (phi - 1) - 1;
}

}

struct HoltsImpl
{
    static constexpr auto name = "seriesHolt";
    static constexpr size_t paramsCount = 2;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"alpha", "betta"};
    static constexpr bool needs_fit = false;

    Float64 alpha, betta;
    Float64 trend, level;
    UInt64 last_ts;

    HoltsImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        alpha = arguments[offset].column->getFloat64(0);
        betta = arguments[offset + 1].column->getFloat64(0);

        if (!ETSForecastHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);


        if (!ETSForecastHelper::is01RangeFloat(betta))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The betta argument of function {} must be in range [0, 1]", name);
    }

    void updateTs(UInt64 ts) { last_ts = ts; }

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

    Float64 forecast(UInt64 ts) const { return level + trend * (ts - last_ts); }
};

struct AdditiveDampedImpl
{
    static constexpr auto name = "seriesAdditiveDamped";
    static constexpr size_t paramsCount = 3;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"alpha", "betta", "phi"};
    static constexpr bool needs_fit = false;

    Float64 alpha, betta, phi;
    Float64 trend, level;
    UInt64 last_ts;

    AdditiveDampedImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        alpha = arguments[offset].column->getFloat64(0);
        betta = arguments[offset + 1].column->getFloat64(0);
        phi = arguments[offset + 2].column->getFloat64(0);

        if (!ETSForecastHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);


        if (!ETSForecastHelper::is01RangeFloat(betta))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The betta argument of function {} must be in range [0, 1]", name);


        if (!ETSForecastHelper::is01RangeFloat(phi))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The phi argument of function {} must be in range [0, 1]", name);
    }

    void updateTs(UInt64 ts) { last_ts = ts; }

    void init(Float64 value)
    {
        level = value;
        trend = 0;
    }

    void add(Float64 value)
    {
        Float64 new_level = alpha * value + (1.0 - alpha) * (level + phi * trend);
        trend = betta * (new_level - level) + (1.0 - betta) * phi * trend;
        level = new_level;
    }

    Float64 forecast(UInt64 ts) const { return level + trend * ETSForecastHelper::calculateAdditivedamped(phi, ts - last_ts); }
};


struct MultiplicativeDampedImpl
{
    static constexpr auto name = "seriesMultiplicativeDamped";
    static constexpr size_t paramsCount = 3;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"alpha", "betta", "phi"};
    static constexpr bool needs_fit = false;

    Float64 alpha, betta, phi;
    Float64 trend, level;
    UInt64 last_ts;

    MultiplicativeDampedImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        alpha = arguments[offset].column->getFloat64(0);
        betta = arguments[offset + 1].column->getFloat64(0);
        phi = arguments[offset + 2].column->getFloat64(0);

        if (!ETSForecastHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);


        if (!ETSForecastHelper::is01RangeFloat(betta))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The betta argument of function {} must be in range [0, 1]", name);


        if (!ETSForecastHelper::is01RangeFloat(phi))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The phi argument of function {} must be in range [0, 1]", name);
    }

    void updateTs(UInt64 ts) { last_ts = ts; }

    void init(Float64 value)
    {
        level = value;
        trend = 0;
    }

    void add(Float64 value)
    {
        Float64 new_level = alpha * value + (1.0 - alpha) * (level * pow(trend, phi));
        trend = betta * (new_level / level) + (1.0 - betta) * pow(trend, phi);
        level = new_level;
    }

    Float64 forecast(UInt64 ts) const { return level * pow(trend, ETSForecastHelper::calculateAdditivedamped(phi, ts - last_ts)); }
};


struct HoltWintersAdditiveImpl
{
    static constexpr auto name = "seriesHoltWintersAdditive";
    static constexpr size_t paramsCount = 4;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"alpha", "betta", "gamma", "window"};
    static constexpr bool needs_fit = false;

    Float64 alpha;
    Float64 betta;
    Float64 gamma;
    size_t window;

    UInt64 last_ts;

    Float64 level, trend;
    Float64 last_season;
    std::deque<Float64> seasons;

    HoltWintersAdditiveImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        alpha = arguments[offset].column->getFloat64(0);
        betta = arguments[offset + 1].column->getFloat64(0);
        gamma = arguments[offset + 2].column->getFloat64(0);
        window = arguments[offset + 3].column->getUInt(0);

        if (!ETSForecastHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);
        if (!ETSForecastHelper::is01RangeFloat(betta))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The betta argument of function {} must be in range [0, 1]", name);
        if (!ETSForecastHelper::is01RangeFloat(gamma))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The gamma argument of function {} must be in range [0, 1]", name);
        if (window < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The window argument of function {} can not be equal to 0 or 1.", name);
    }

    void updateTs(UInt64 ts) { last_ts = ts; }

    void init(Float64 value)
    {
        level = value;
        trend = 0;
        seasons.assign(window, 0.0);
        add(value);
    }

    void add(Float64 value)
    {
        Float64 new_level = alpha * (value - seasons[0]) + (1.0 - alpha) * (level + trend);
        Float64 new_trend = betta * (new_level - level) + (1.0 - betta) * trend;
        Float64 new_season = gamma * (value - level - trend) + (1.0 - gamma) * seasons[0];
        level = new_level;
        trend = new_trend;
        last_season = seasons[0];
        seasons.push_back(new_season);
        seasons.pop_front();
    }

    Float64 forecast(UInt64 ts) const
    {
        UInt64 delta = (ts - last_ts);
        if (delta % window == 0)
            return level + trend * delta + last_season;
        return level + trend * delta + seasons[(delta + (window - 1)) % window];
    }
};


struct HoltWintersMultiplicativeImpl
{
    static constexpr auto name = "seriesHoltWintersMultiplicative";
    static constexpr size_t paramsCount = 4;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"alpha", "betta", "gamma", "window"};
    static constexpr bool needs_fit = false;

    Float64 alpha;
    Float64 betta;
    Float64 gamma;
    size_t window;

    UInt64 last_ts;

    Float64 level, trend, last_season;
    std::deque<Float64> seasons;

    HoltWintersMultiplicativeImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        alpha = arguments[offset].column->getFloat64(0);
        betta = arguments[offset + 1].column->getFloat64(0);
        gamma = arguments[offset + 2].column->getFloat64(0);
        window = arguments[offset + 3].column->getUInt(0);

        if (!ETSForecastHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);
        if (!ETSForecastHelper::is01RangeFloat(betta))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The betta argument of function {} must be in range [0, 1]", name);
        if (!ETSForecastHelper::is01RangeFloat(gamma))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The gamma argument of function {} must be in range [0, 1]", name);
        if (window == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The window argument of function {} can not be equal to 0.", name);
    }

    void updateTs(UInt64 ts) { last_ts = ts; }

    void init(Float64 value)
    {
        level = value;
        trend = 0;
        seasons.assign(window, 1.0);
        add(value);
    }

    void add(Float64 value)
    {
        Float64 new_level = alpha * (value / seasons[0]) + (1.0 - alpha) * (level + trend);
        Float64 new_trend = betta * (new_level - level) + (1.0 - betta) * trend;
        Float64 new_season = gamma * (value / (level + trend)) + (1.0 - gamma) * seasons[0];
        level = new_level;
        trend = new_trend;
        last_season = seasons[0];
        seasons.push_back(new_season);
        seasons.pop_front();
    }

    Float64 forecast(UInt64 ts) const
    {
        UInt64 delta = (ts - last_ts);
        if (delta % window == 0)
            return (level + trend * delta) * last_season;
        return (level + trend * delta) * seasons[(delta + (window - 1)) % window];
    }
};

struct HoltWintersAdditiveDampedImpl
{
    static constexpr auto name = "seriesHoltWintersDamped";
    static constexpr size_t paramsCount = 5;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"alpha", "betta", "gamma", "phi", "window"};
    static constexpr bool needs_fit = false;

    Float64 alpha;
    Float64 betta;
    Float64 gamma;
    Float64 phi;
    size_t window;

    UInt64 last_ts;

    Float64 level, trend;
    Float64 last_season;
    std::deque<Float64> seasons;

    HoltWintersAdditiveDampedImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        alpha = arguments[offset].column->getFloat64(0);
        betta = arguments[offset + 1].column->getFloat64(0);
        gamma = arguments[offset + 2].column->getFloat64(0);
        phi = arguments[offset + 3].column->getFloat64(0);
        window = arguments[offset + 4].column->getUInt(0);

        if (!ETSForecastHelper::is01RangeFloat(alpha))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The alpha argument of function {} must be in range [0, 1]", name);
        if (!ETSForecastHelper::is01RangeFloat(betta))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The betta argument of function {} must be in range [0, 1]", name);
        if (!ETSForecastHelper::is01RangeFloat(gamma))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The gamma argument of function {} must be in range [0, 1]", name);
        if (!ETSForecastHelper::is01RangeFloat(phi))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The phi argument of function {} must be in range [0, 1]", name);
        if (window == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The window argument of function {} can not be equal to 0.", name);
    }

    void updateTs(UInt64 ts) { last_ts = ts; }

    void init(Float64 value)
    {
        level = value;
        trend = 0;
        seasons.assign(window, 0.0);
        add(value);
    }

    void add(Float64 value)
    {
        Float64 new_level = alpha * (value - seasons[0]) + (1.0 - alpha) * (level + phi * trend);
        Float64 new_trend = betta * (new_level - level) + (1.0 - betta) * phi * trend;
        Float64 new_season = gamma * (value - level - trend * phi) + (1.0 - gamma) * seasons[0];
        level = new_level;
        trend = new_trend;
        last_season = seasons[0];
        seasons.push_back(new_season);
        seasons.pop_front();
    }

    Float64 forecast(UInt64 ts) const
    {
        UInt64 delta = (ts - last_ts);
        if (delta % window == 0)
            return level + trend * ETSForecastHelper::calculateAdditivedamped(phi, delta) + last_season;
        return level + trend * ETSForecastHelper::calculateAdditivedamped(phi, delta) + seasons[(delta + (window - 1)) % window];
    }
};

REGISTER_FUNCTION(ETSForecast)
{
    factory.registerFunction<FunctionETSForecast<HoltsImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionETSForecast<AdditiveDampedImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionETSForecast<MultiplicativeDampedImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionETSForecast<HoltWintersAdditiveImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionETSForecast<HoltWintersMultiplicativeImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionETSForecast<HoltWintersAdditiveDampedImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
}
}
