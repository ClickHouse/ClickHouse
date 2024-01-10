#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
}

//Detects a possible anomaly in series using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences)
class FunctionSeriesOutliersTukey : public IFunction
{
public:
    static constexpr auto name = "seriesOutliersTukey";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSeriesOutliersTukey>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{{"time_series", &isArray<IDataType>, nullptr, "Array"}};
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        ColumnPtr array_ptr = arguments[0].column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

        const IColumn & src_data = array->getData();
        const ColumnArray::Offsets & src_offsets = array->getOffsets();

        ColumnPtr res;

        if (executeNumber<UInt8>(src_data, src_offsets, res) || executeNumber<UInt16>(src_data, src_offsets, res)
            || executeNumber<UInt32>(src_data, src_offsets, res) || executeNumber<UInt64>(src_data, src_offsets, res)
            || executeNumber<Int8>(src_data, src_offsets, res) || executeNumber<Int16>(src_data, src_offsets, res)
            || executeNumber<Int32>(src_data, src_offsets, res) || executeNumber<Int64>(src_data, src_offsets, res)
            || executeNumber<Float32>(src_data, src_offsets, res) || executeNumber<Float64>(src_data, src_offsets, res))
        {
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
    bool executeNumber(const IColumn & src_data, const ColumnArray::Offsets & src_offsets, ColumnPtr & res_ptr) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        auto outliers = ColumnFloat64::create();
        auto & outlier_data = outliers->getData();

        ColumnArray::ColumnOffsets::MutablePtr res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_offsets_data = res_offsets->getData();

        ColumnArray::Offset prev_src_offset = 0;
        for (auto curr_src_offset : src_offsets)
        {
            chassert(prev_src_offset <= curr_src_offset);
            size_t len = curr_src_offset - prev_src_offset;
            if (len < 4)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least four data points are needed for function {}", getName());

            std::vector<Float64> src_sorted(src_vec.begin() + prev_src_offset, src_vec.begin() + curr_src_offset);
            std::sort(src_sorted.begin(), src_sorted.end());

            size_t q1_index = len / 4;
            size_t q3_index = (len * 3) / 4;

            Float64 q1 = (len % 2 != 0) ? src_sorted[q1_index] : (src_sorted[q1_index - 1] + src_sorted[q1_index]) / 2;
            Float64 q3 = (len % 2 != 0) ? src_sorted[q3_index] : (src_sorted[q3_index - 1] + src_sorted[q3_index]) / 2;

            Float64 iqr = q3 - q1;

            Float64 lower_fence = q1 - 1.5 * iqr;
            Float64 upper_fence = q3 + 1.5 * iqr;

            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j)
            {
                auto score = std::min((src_vec[j] - lower_fence) / iqr, 0.0) + std::max((src_vec[j] - upper_fence) / iqr, 0.0);
                outlier_data.push_back(score);
            }
            res_offsets_data.push_back(outlier_data.size());
            prev_src_offset = curr_src_offset;
        }

        res_ptr = ColumnArray::create(std::move(outliers), std::move(res_offsets));
        return true;
    }
};

REGISTER_FUNCTION(SeriesOutliersTukey)
{
    factory.registerFunction<FunctionSeriesOutliersTukey>(FunctionDocumentation{
        .description = R"(
Detects a possible anomaly in series using [Tukey Fences](https://en.wikipedia.org/wiki/Outlier#Tukey%27s_fences).

**Syntax**

``` sql
seriesOutliersTukey(series);
```

**Arguments**

- `series` - An array of numeric values

**Returned value**

- Returns an array of the same length where each value represents a modified Z-score of possible anomaly of corresponding element in the series.
- A value greater than 3 or lesser than -3 indicates a possible anomaly.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
seriesOutliersTukey([-3,2.4,15,3.9,5,6,4.5,5.2,3,4,5,16,7,5,5,4]) AS print_0;
```

Result:

``` text
┌───────────print_0──────────────────────────────────────────────────────────────────┐
│[-2.7121212121212137,0,4.196969696969699,0,0,0,0,0,0,0,0,4.803030303030305,0,0,0,0] │
└────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
seriesOutliersTukey(arrayMap(x -> sin(x / 10), range(30))) AS print_0;
```

Result:

``` text
┌───────────print_0────────────────────────────────────────────┐
│[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0] │
└──────────────────────────────────────────────────────────────┘
```)",
        .categories{"Time series analysis"}});
}
}
