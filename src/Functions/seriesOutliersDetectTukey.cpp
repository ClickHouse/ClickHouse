#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
}

///Detects a possible anomaly in series using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences)
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
        FunctionArgumentDescriptors mandatory_args{{"time_series", &isArray<IDataType>, nullptr, "Array"}};
        FunctionArgumentDescriptors optional_args{
            {"kind", &isString<IDataType>, isColumnConst, "const String"},
            {"min_percentile", &isNativeNumber<IDataType>, isColumnConst, "Number"},
            {"max_percentile", &isNativeNumber<IDataType>, isColumnConst, "Number"},
            {"k", &isNativeNumber<IDataType>, isColumnConst, "Number"}};

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3, 4}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        ColumnPtr col = arguments[0].column;
        const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(col.get());

        const IColumn & arr_data = col_arr->getData();
        const ColumnArray::Offsets & arr_offsets = col_arr->getOffsets();

        Float64 min_percentile = 0.10; //default 10th percentile
        Float64 max_percentile = 0.90; //default 90th percentile

        if (arguments.size() > 1)
        {
            //const IColumn * arg_column = arguments[1].column.get();
            const ColumnConst * arg_string = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());

            if (!arg_string)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} must be constant String", getName());

            String kind = arg_string->getValue<String>();
            if (kind == "ctukey")
            {
                if (arguments.size() > 2)
                {
                    Float64 p_min = arguments[2].column->getFloat64(0);
                    if (p_min >= 2.0 && p_min <= 98.0)
                        min_percentile = p_min / 100;
                    else
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS, "The third argument of function {} must be in range [2.0, 98.0]", getName());
                }

                if (arguments.size() > 3)
                {
                    Float64 p_max = arguments[3].column->getFloat64(0);
                    if (p_max >= 2.0 && p_max <= 98.0 && p_max > min_percentile * 100)
                        max_percentile = p_max / 100;
                    else
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS, "The fourth argument of function {} must be in range [2.0, 98.0]", getName());
                }
            }
            else if (kind == "tukey")
            {
                min_percentile = 0.25;
                max_percentile = 0.75;
            }
            else
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "The second argument of function {} can only be 'tukey' or 'ctukey'.", getName());
        }

        Float64 K = 1.50;
        if (arguments.size() == 5)
        {
            auto k_val = arguments[4].column->getFloat64(0);
            if (k_val >= 0.0)
                K = k_val;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The fifth argument of function {} must be a positive number", getName());
        }

        ColumnPtr col_res;

        if (executeNumber<UInt8>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<UInt16>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<UInt32>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<UInt64>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<Int8>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<Int16>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<Int32>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<Int64>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<Float32>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res)
            || executeNumber<Float64>(arr_data, arr_offsets, min_percentile, max_percentile, K, col_res))
        {
            return col_res;
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
    }

private:
    template <typename T>
    bool executeNumber(
        const IColumn & arr_data,
        const ColumnArray::Offsets & arr_offsets,
        Float64 min_percentile,
        Float64 max_percentile,
        Float64 K,
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

            Float64 q1, q2;

            auto p1 = len * min_percentile;
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

            auto p2 = len * max_percentile;
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

            Float64 lower_fence = q1 - K * iqr;
            Float64 upper_fence = q2 + K * iqr;

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
Detects a possible anomaly in series using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences).

Detects a possible anomaly in series using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences).

**Syntax**

``` sql
seriesOutliersDetectTukey(series);
seriesOutliersDetectTukey(series, kind, min_percentile, max_percentile, K);
```

**Arguments**

- `series` - An array of numeric values.
- `kind` - Kind of algorithm to use. Supported values are 'tukey' for standard tukey and 'ctukey' for custom tukey algorithm. The default is 'ctukey'.
- `min_percentile` - The minimum percentile to be used to calculate inter-quantile range(IQR). The value must be in range [2,98]. The default is 10. This value is only supported for 'ctukey'.
- `max_percentile` - The maximum percentile to be used to calculate inter-quantile range(IQR). The value must be in range [2,98]. The default is 90. This value is only supported for 'ctukey'.
- `K` - Non-negative constant value to detect mild or stronger outliers. The default value is 1.5

Default quantile range:
- `tukey` - 25%/75%
- `ctukey` - 10%/90%

**Returned value**

- Returns an array of the same length where each value represents score of possible anomaly of corresponding element in the series.
- A non-zero score indicates a possible anomaly.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4, 5, 12, 45, 12, 3, 3, 4, 5, 6]) AS print_0;
```

Result:

``` text
┌───────────print_0───────────────────┐
│[0,0,0,0,0,0,0,0,0,10.5,0,0,0,0,0,0] │
└─────────────────────────────────────┘
```

Query:

``` sql
SELECT seriesOutliersDetectTukey([-3, 2, 15, 3, 5, 6, 4.50, 5, 12, 45, 12, 3.40, 3, 4, 5, 6], 'ctukey', 20, 80, 1.5) AS print_0;
```

Result:

``` text
┌─print_0────────────────────────────┐
│ [0,0,0,0,0,0,0,0,0,12,0,0,0,0,0,0] │
└────────────────────────────────────┘
```)",
        .categories{"Time series analysis"}});
}
}
