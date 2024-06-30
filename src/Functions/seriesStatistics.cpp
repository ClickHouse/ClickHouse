#include <cmath>
#include <cstddef>
#include <deque>
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
}


/*methodName(series, number_to_predict, params, [fill_gaps]) -> Array[values]
methodName(series, times, times_to_predict, params, [fill_gaps]) -> Array[values]
*/

template <typename SeriesSequentialStatistics>
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
        for (auto arg_name : SeriesSequentialStatistics::paramsNames)
        {
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

        const IColumn & series_column = series_arr->getData();
        const ColumnArray::Offsets & series_offsets = series_arr->getOffsets();

        ColumnPtr col_res;
        if (input_rows_count == 0)
            return ColumnArray::create(ColumnFloat64::create());

        SeriesSequentialStatistics estimator(arguments, 1);

        if (executeNumber<UInt8>(series_column, series_offsets, estimator, col_res)
            || executeNumber<UInt16>(series_column, series_offsets, estimator, col_res)
            || executeNumber<UInt32>(series_column, series_offsets, estimator, col_res)
            || executeNumber<UInt64>(series_column, series_offsets, estimator, col_res)
            || executeNumber<Int8>(series_column, series_offsets, estimator, col_res)
            || executeNumber<Int16>(series_column, series_offsets, estimator, col_res)
            || executeNumber<Int32>(series_column, series_offsets, estimator, col_res)
            || executeNumber<Int64>(series_column, series_offsets, estimator, col_res)
            || executeNumber<Float32>(series_column, series_offsets, estimator, col_res)
            || executeNumber<Float64>(series_column, series_offsets, estimator, col_res))
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
        const IColumn & series_column,
        const ColumnArray::Offsets & series_offsets,
        SeriesSequentialStatistics & estimator,
        ColumnPtr & res_ptr) const
    {
        const ColumnVector<T> * series_concrete = checkAndGetColumn<ColumnVector<T>>(&series_column);
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
bool is01RangeFloat(Float64 value)
{
    if (isnan(value) || !isFinite(value) || value < 0.0 || value > 1.0)
        return false;
    return true;
}
}

struct SeriesEmaImpl
{
    static constexpr auto name = "seriesEMA";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"alpha"};
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

struct SeriesWindowSumImpl
{
    static constexpr auto name = "seriesWindowSum";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"window_size"};
    static constexpr bool needs_fit = false;
    size_t window_size;
    std::deque<Float64> values;
    Float64 curent_sum;

    SeriesWindowSumImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        window_size = arguments[offset].column->getUInt(0);
        if (window_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window_size argument of function {} can not be equal to 0", name);
    }


    Float64 init(Float64 value)
    {
        curent_sum = value;
        values = {value};
        return curent_sum;
    }

    Float64 add(Float64 new_value)
    {
        curent_sum += new_value;
        values.push_back(new_value);
        if (values.size() > window_size)
        {
            curent_sum -= values.front();
            values.pop_front();
        }
        return curent_sum;
    }
};

struct SeriesWindowAverageImpl
{
    static constexpr auto name = "seriesWindowAverage";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"window_size"};
    static constexpr bool needs_fit = false;
    size_t window_size;
    std::deque<Float64> values;
    Float64 curent_avg;

    SeriesWindowAverageImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        window_size = arguments[offset].column->getUInt(0);
        if (window_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window_size argument of function {} can not be equal to 0", name);
    }


    Float64 init(Float64 value)
    {
        curent_avg = value;
        values = {value};
        return curent_avg;
    }

    Float64 add(Float64 new_value)
    {
        size_t current_size = values.size();
        if (current_size == window_size)
        {
            curent_avg -= values.front() / window_size;
            curent_avg += new_value / window_size;
            values.pop_front();
        }
        else
        {
            curent_avg *= static_cast<Float64>(values.size()) / static_cast<Float64>(values.size() + 1);
            curent_avg += new_value / (values.size() + 1);
        }
        values.push_back(new_value);
        return curent_avg;
    }
};


struct SeriesWindowStandardDeviationImpl
{
    static constexpr auto name = "seriesWindowStandardDeviation";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"window_size"};
    static constexpr bool needs_fit = false;
    size_t window_size;
    std::deque<Float64> values;
    Float64 curent_avg;
    Float64 curent_avg_sq;

    SeriesWindowStandardDeviationImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        window_size = arguments[offset].column->getUInt(0);
        if (window_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window_size argument of function {} can not be equal to 0", name);
    }


    Float64 init(Float64 value)
    {
        curent_avg = value;
        curent_avg_sq = value * value;
        values = {value};
        return 0;
    }

    Float64 add(Float64 new_value)
    {
        size_t current_size = values.size();
        if (current_size == window_size)
        {
            curent_avg -= values.front() / window_size;
            curent_avg += new_value / window_size;

            curent_avg_sq -= values.front() * values.front() / window_size;
            curent_avg_sq += new_value * new_value / window_size;

            values.pop_front();
        }
        else
        {
            curent_avg *= static_cast<Float64>(values.size()) / static_cast<Float64>(values.size() + 1);
            curent_avg += new_value / static_cast<Float64>(values.size() + 1);

            curent_avg_sq *= static_cast<Float64>(values.size()) / static_cast<Float64>(values.size() + 1);
            curent_avg_sq += new_value * new_value / static_cast<Float64>(values.size() + 1);
        }
        values.push_back(new_value);
        return std::sqrt(curent_avg_sq - curent_avg * curent_avg);
    }
};


struct SeriesWindowMinImpl
{
    static constexpr auto name = "seriesWindowMin";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"window_size"};
    static constexpr bool needs_fit = false;
    size_t window_size;

    struct IndexedValue
    {
        Float64 value;
        size_t index;
    };

    std::deque<IndexedValue> values;
    size_t index;

    SeriesWindowMinImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        window_size = arguments[offset].column->getUInt(0);
        if (window_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window_size argument of function {} can not be equal to 0", name);
    }


    Float64 init(Float64 value)
    {
        index = 0;
        values.clear();
        return add(value);
    }

    Float64 add(Float64 new_value)
    {
        while (!values.empty() && values.back().value >= new_value)
            values.pop_back();
        values.push_back({new_value, index});
        index += 1;
        while (!values.empty() && values.front().index + window_size < index)
            values.pop_front();
        return values.front().value;
    }
};


struct SeriesWindowMaxImpl
{
    static constexpr auto name = "seriesWindowMax";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"window_size"};
    static constexpr bool needs_fit = false;
    size_t window_size;

    struct IndexedValue
    {
        Float64 value;
        size_t index;
    };

    std::deque<IndexedValue> values;
    size_t index;

    SeriesWindowMaxImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        window_size = arguments[offset].column->getUInt(0);
        if (window_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window_size argument of function {} can not be equal to 0", name);
    }


    Float64 init(Float64 value)
    {
        index = 0;
        values.clear();
        return add(value);
    }

    Float64 add(Float64 new_value)
    {
        while (!values.empty() && values.back().value <= new_value)
            values.pop_back();
        values.push_back({new_value, index});
        index += 1;
        while (!values.empty() && values.front().index + window_size < index)
            values.pop_front();
        return values.front().value;
    }
};
// Matches TALIB KaufmansAMA implementation
struct SeriesKaufmansAMAImpl
{
    static constexpr auto name = "seriesKaufmansAMA";
    static constexpr size_t paramsCount = 1;
    static constexpr std::array<const char *, paramsCount> paramsNames = {"count"};
    static constexpr bool needs_fit = false;


    const Float64 fastest_sc = 2.0 / (2.0 + 1.0);
    const Float64 slowest_sc = 2.0 / (30.0 + 1.0);


    size_t window_size;
    size_t cur_count;

    Float64 ama;

    SeriesKaufmansAMAImpl(const ColumnsWithTypeAndName & arguments, size_t offset)
    {
        window_size = arguments[offset].column->getUInt(0);
        if (window_size < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window_size argument of function {} can not be equal to 0 or 1.", name);
    }
    std::deque<Float64> values, diffs;
    Float64 volatility;


    Float64 init(Float64 value)
    {
        ama = value;
        values.push_back(value);
        diffs = {};
        volatility = 0.0;
        return ama;
    }

    Float64 add(Float64 new_value)
    {
        Float64 diff = abs(new_value - values.back());
        values.push_back(new_value);
        diffs.push_back(diff);
        volatility += diff;

        if (values.size() <= window_size)
        {
            ama = new_value;
            return new_value;
        }

        if (diffs.size() > window_size)
        {
            volatility -= diffs.front();
            diffs.pop_front();
        }

        Float64 change = abs(new_value - values[0]);
        values.pop_front();

        Float64 er = volatility == 0.0 ? 0 : change / volatility;
        Float64 sc = (er * (fastest_sc - slowest_sc) + slowest_sc) * (er * (fastest_sc - slowest_sc) + slowest_sc);
        ama = sc * new_value + (1.0 - sc) * ama;
        return ama;
    }
};


REGISTER_FUNCTION(seriesStatistics)
{
    factory.registerFunction<FunctionSeriesStatistics<SeriesEmaImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionSeriesStatistics<SeriesKaufmansAMAImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});

    factory.registerFunction<FunctionSeriesStatistics<SeriesWindowSumImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionSeriesStatistics<SeriesWindowAverageImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionSeriesStatistics<SeriesWindowStandardDeviationImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionSeriesStatistics<SeriesWindowMinImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
    factory.registerFunction<FunctionSeriesStatistics<SeriesWindowMaxImpl>>(FunctionDocumentation{
        .description = R"(
TODOTODOTODOTODO
)",
        .categories{"Time series analysis"}});
}
}
