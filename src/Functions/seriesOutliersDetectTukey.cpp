#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/NaNUtils.h>
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Detects a possible anomaly in series using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences)
class FunctionSeriesOutliersDetectTukey : public IFunction
{
public:
    static constexpr auto name = "seriesOutliersDetectTukey";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSeriesOutliersDetectTukey>(); }

    std::string getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs either 1 or 4 arguments; passed {}.",
                getName(),
                arguments.size());

        FunctionArgumentDescriptors mandatory_args{{"time_series", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"}};
        FunctionArgumentDescriptors optional_args{
            {"min_percentile", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), isColumnConst, "Number"},
            {"max_percentile", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), isColumnConst, "Number"},
            {"k", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeNumber), isColumnConst, "Number"}};

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr col = arguments[0].column;
        const ColumnArray & col_arr = checkAndGetColumn<ColumnArray>(*col);

        const IColumn & arr_data = col_arr.getData();
        const ColumnArray::Offsets & arr_offsets = col_arr.getOffsets();

        ColumnPtr col_res;
        if (input_rows_count == 0)
            return ColumnArray::create(ColumnFloat64::create());

        Float64 min_percentile = 0.25; /// default 25th percentile
        Float64 max_percentile = 0.75; /// default 75th percentile
        Float64 k = 1.50;

        if (arguments.size() > 1)
        {
            static constexpr Float64 min_percentile_lower_bound = 0.02;
            static constexpr Float64 max_percentile_upper_bound = 0.98;

            min_percentile = arguments[1].column->getFloat64(0);
            if (isnan(min_percentile) || !isFinite(min_percentile) || min_percentile < min_percentile_lower_bound|| min_percentile > max_percentile_upper_bound)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second argument of function {} must be in range [0.02, 0.98]", getName());

            max_percentile = arguments[2].column->getFloat64(0);
            if (isnan(max_percentile) || !isFinite(max_percentile) || max_percentile < min_percentile_lower_bound || max_percentile > max_percentile_upper_bound || max_percentile < min_percentile)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The third argument of function {} must be in range [0.02, 0.98]", getName());

            k = arguments[3].column->getFloat64(0);
            if (k < 0.0 || isnan(k) || !isFinite(k))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The fourth argument of function {} must be a non-negative number", getName());
        }

        if (executeNumber<UInt8>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<UInt16>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<UInt32>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<UInt64>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<Int8>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<Int16>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<Int32>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<Int64>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<Float32>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res)
            || executeNumber<Float64>(arr_data, arr_offsets, min_percentile, max_percentile, k, col_res))
        {
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }

private:
    template <typename T>
    bool executeNumber(
        const IColumn & arr_data,
        const ColumnArray::Offsets & arr_offsets,
        Float64 min_percentile,
        Float64 max_percentile,
        Float64 k,
        ColumnPtr & res_ptr) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&arr_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        auto outliers = ColumnFloat64::create();
        auto & outlier_data = outliers->getData();

        ColumnArray::ColumnOffsets::MutablePtr res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = res_offsets->getData();

        std::vector<Float64> src_sorted;

        ColumnArray::Offset prev_src_offset = 0;
        for (auto src_offset : arr_offsets)
        {
            chassert(prev_src_offset <= src_offset);
            size_t len = src_offset - prev_src_offset;
            if (len < 4)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least four data points are needed for function {}", getName());

            src_sorted.assign(src_vec.begin() + prev_src_offset, src_vec.begin() + src_offset);
            std::sort(src_sorted.begin(), src_sorted.end());

            Float64 q1;
            Float64 q2;

            Float64 p1 = len * min_percentile;
            if (p1 == static_cast<Int64>(p1))
            {
                size_t index = static_cast<size_t>(p1) - 1;
                q1 = (src_sorted[index] + src_sorted[index + 1]) / 2;
            }
            else
            {
                size_t index = static_cast<size_t>(std::ceil(p1)) - 1;
                q1 = src_sorted[index];
            }

            Float64 p2 = len * max_percentile;
            if (p2 == static_cast<Int64>(p2))
            {
                size_t index = static_cast<size_t>(p2) - 1;
                q2 = (src_sorted[index] + src_sorted[index + 1]) / 2;
            }
            else
            {
                size_t index = static_cast<size_t>(std::ceil(p2)) - 1;
                q2 = src_sorted[index];
            }

            Float64 iqr = q2 - q1; /// interquantile range

            Float64 lower_fence = q1 - k * iqr;
            Float64 upper_fence = q2 + k * iqr;

            for (ColumnArray::Offset j = prev_src_offset; j < src_offset; ++j)
            {
                auto score = std::min((src_vec[j] - lower_fence), 0.0) + std::max((src_vec[j] - upper_fence), 0.0);
                outlier_data.push_back(score);
            }
            res_offsets_data.push_back(outlier_data.size());
            prev_src_offset = src_offset;
        }

        res_ptr = ColumnArray::create(std::move(outliers), std::move(res_offsets));
        return true;
    }
};

REGISTER_FUNCTION(SeriesOutliersDetectTukey)
{
    factory.registerFunction<FunctionSeriesOutliersDetectTukey>(FunctionDocumentation{
        .description = R"(
Detects outliers in series data using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences).

**Syntax**

``` sql
seriesOutliersDetectTukey(series);
seriesOutliersDetectTukey(series, min_percentile, max_percentile, k);
```

**Arguments**

- `series` - An array of numeric values.
- `min_quantile` - The minimum quantile to be used to calculate inter-quantile range [(IQR)](https://en.wikipedia.org/wiki/Interquartile_range). The value must be in range [0.02,0.98]. The default is 0.25.
- `max_quantile` - The maximum quantile to be used to calculate inter-quantile range (IQR). The value must be in range [0.02, 0.98]. The default is 0.75.
- `k` - Non-negative constant value to detect mild or stronger outliers. The default value is 1.5

At least four data points are required in `series` to detect outliers.

**Returned value**

- Returns an array of the same length as the input array where each value represents score of possible anomaly of corresponding element in the series. A non-zero score indicates a possible anomaly.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4, 5, 12, 45, 12, 3, 3, 4, 5, 6]) AS print_0;
```

Result:

``` text
┌───────────print_0─────────────────┐
│[0,0,0,0,0,0,0,0,0,27,0,0,0,0,0,0] │
└───────────────────────────────────┘
```

Query:

``` sql
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 0.2, 0.8, 1.5) AS print_0;
```

Result:

``` text
┌─print_0──────────────────────────────┐
│ [0,0,0,0,0,0,0,0,0,19.5,0,0,0,0,0,0] │
└──────────────────────────────────────┘
```)",
        .categories{"Time series analysis"}});
}
}
