#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#include <Functions/stl.hpp>
#pragma clang diagnostic pop

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
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

// Decompose time series data based on STL(Seasonal-Trend Decomposition Procedure Based on Loess)
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
            {"time_series", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"period", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), nullptr, "Unsigned Integer"},
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr array_ptr = arguments[0].column;
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());
        if (!array)
        {
            const ColumnConst * const_array = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get());
            if (!const_array)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(), getName());

            array_ptr = const_array->convertToFullColumn();
            array = assert_cast<const ColumnArray *>(array_ptr.get());
        }

        const IColumn & src_data = array->getData();
        const ColumnArray::Offsets & src_offsets = array->getOffsets();

        auto res = ColumnFloat32::create();
        auto & res_data = res->getData();

        ColumnArray::ColumnOffsets::MutablePtr res_col_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_col_offsets_data = res_col_offsets->getData();

        auto root_offsets = ColumnArray::ColumnOffsets::create();
        auto & root_offsets_data = root_offsets->getData();

        ColumnArray::Offset prev_src_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt64 period;
            auto period_ptr = arguments[1].column->convertToFullColumnIfConst();
            if (checkAndGetColumn<ColumnUInt8>(period_ptr.get())
                || checkAndGetColumn<ColumnUInt16>(period_ptr.get())
                || checkAndGetColumn<ColumnUInt32>(period_ptr.get())
                || checkAndGetColumn<ColumnUInt64>(period_ptr.get()))
                period = period_ptr->getUInt(i);
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of second argument of function {}",
                    arguments[1].column->getName(),
                    getName());


            std::vector<Float32> seasonal;
            std::vector<Float32> trend;
            std::vector<Float32> residue;

            ColumnArray::Offset curr_offset = src_offsets[i];

            if (executeNumber<UInt8>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<UInt16>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<UInt32>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<UInt64>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<Int8>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<Int16>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<Int32>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<Int64>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<Float32>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue)
                || executeNumber<Float64>(src_data, period, prev_src_offset, curr_offset, seasonal, trend, residue))
            {
                res_data.insert(seasonal.begin(), seasonal.end());
                res_col_offsets_data.push_back(res_data.size());

                res_data.insert(trend.begin(), trend.end());
                res_col_offsets_data.push_back(res_data.size());

                res_data.insert(residue.begin(), residue.end());
                res_col_offsets_data.push_back(res_data.size());

                // Create Baseline = seasonal + trend
                std::transform(seasonal.begin(), seasonal.end(), trend.begin(), std::back_inserter(res_data), std::plus<>());
                res_col_offsets_data.push_back(res_data.size());

                root_offsets_data.push_back(res_col_offsets->size());

                prev_src_offset = curr_offset;
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(),
                    getName());
        }
        ColumnArray::MutablePtr nested_array_col = ColumnArray::create(std::move(res), std::move(res_col_offsets));
        return ColumnArray::create(std::move(nested_array_col), std::move(root_offsets));
    }

    template <typename T>
    bool executeNumber(
        const IColumn & src_data,
        UInt64 period,
        ColumnArray::Offset start,
        ColumnArray::Offset end,
        std::vector<Float32> & seasonal,
        std::vector<Float32> & trend,
        std::vector<Float32> & residue) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);
        if (!src_data_concrete)
            return false;

        const PaddedPODArray<T> & src_vec = src_data_concrete->getData();

        chassert(start <= end);
        size_t len = end - start;
        if (len < 4)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least four data points are needed for function {}", getName());
        if (period > (len / 2))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The series should have data of at least two period lengths for function {}", getName());

        std::vector<float> src(src_vec.begin() + start, src_vec.begin() + end);

        auto res = stl::params().fit(src, period);

        if (res.seasonal.empty())
            return false;

        seasonal = std::move(res.seasonal);
        trend = std::move(res.trend);
        residue = std::move(res.remainder);
        return true;
    }
};
REGISTER_FUNCTION(seriesDecomposeSTL)
{
    factory.registerFunction<FunctionSeriesDecomposeSTL>(FunctionDocumentation{
        .description = R"(
Decomposes a time series using STL [(Seasonal-Trend Decomposition Procedure Based on Loess)](https://www.wessa.net/download/stl.pdf) into a season, a trend and a residual component.

**Syntax**

``` sql
seriesDecomposeSTL(series, period);
```

**Arguments**

- `series` - An array of numeric values
- `period` - A positive number

The number of data points in `series` should be at least twice the value of `period`.

**Returned value**

- An array of four arrays where the first array include seasonal components, the second array - trend, the third array - residue component, and the fourth array - baseline(seasonal + trend) component.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
SELECT seriesDecomposeSTL([10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34], 3) AS print_0;
```

Result:

``` text
┌───────────print_0──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [[
        -13.529999, -3.1799996, 16.71,      -13.53,     -3.1799996, 16.71,      -13.53,     -3.1799996,
        16.71,      -13.530001, -3.18,      16.710001,  -13.530001, -3.1800003, 16.710001,  -13.530001,
        -3.1800003, 16.710001,  -13.530001, -3.1799994, 16.71,      -13.529999, -3.1799994, 16.709997
    ],
    [
        23.63,     23.63,     23.630003, 23.630001, 23.630001, 23.630001, 23.630001, 23.630001,
        23.630001, 23.630001, 23.630001, 23.63,     23.630001, 23.630001, 23.63,     23.630001,
        23.630001, 23.63,     23.630001, 23.630001, 23.630001, 23.630001, 23.630001, 23.630003
    ],
    [
        0, 0.0000019073486, -0.0000019073486, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -0.0000019073486, 0,
        0
    ],
    [
        10.1, 20.449999, 40.340004, 10.100001, 20.45, 40.34, 10.100001, 20.45, 40.34, 10.1, 20.45, 40.34,
        10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.1, 20.45, 40.34, 10.100002, 20.45, 40.34
    ]]                                                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```)",
        .categories{"Time series analysis"}});
}
}
