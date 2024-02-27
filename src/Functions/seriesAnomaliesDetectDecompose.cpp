#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
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
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionSeriesAnomaliesDetectDecompose : public IFunction
{
public:
    static constexpr auto name = "seriesAnomaliesDetectDecompose";

    explicit FunctionSeriesAnomaliesDetectDecompose(ContextPtr context_) : context(context_) { }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionSeriesAnomaliesDetectDecompose>(context); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

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

        FunctionArgumentDescriptors mandatory_args{
            {"time_series", &isArray<IDataType>, nullptr, "Array"},
        };

        FunctionArgumentDescriptors optional_args{
            {"threshold", &isNativeNumber<IDataType>, isColumnConst, "const positive Float"},
            {"seasonality", &isNativeInteger<IDataType>, isColumnConst, "const Integer"},
            {"AD_method", &isString<IDataType>, isColumnConst, "const String"}};

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()));
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnArray::create(ColumnFloat64::create());

        /// Assigning default values for function arguments
        Float64 k_value = 1.50;
        Float64 min_percentile = 10;
        Float64 max_percentile = 90;
        Int64 seasonality = -1;

        if (arguments.size() > 1)
        {
            k_value = arguments[1].column->getFloat64(0);
            if (k_value < 0.0 || isnan(k_value) || !isFinite(k_value))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second argument can only be a positive number, got {}", k_value);

            seasonality = arguments[2].column->getInt(0);
            if (seasonality < -1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The third argument can only be -1, 0 or a positive number, got {}", seasonality);

            const auto * kind_const_col = checkAndGetColumnConstData<ColumnString>(arguments[3].column.get());

            String kind = kind_const_col->getDataAt(0).toString();
            if (kind == "tukey")
            {
                min_percentile = 25;
                max_percentile = 75;
            }
            else if (kind != "ctukey")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The fourth argument of function can only be 'tukey' or 'ctukey', got '{}'", kind);
        }

        ColumnsWithTypeAndName decompose_cols{arguments[0]};

        if (seasonality == -1)
        {
            ColumnsWithTypeAndName columns{arguments[0]};
            auto get_seasonality = FunctionFactory::instance().get("seriesPeriodDetectFFT", context)->build(columns);
            auto get_seasonality_res = get_seasonality->execute(columns, std::make_shared<DataTypeUInt64>(), input_rows_count);

            decompose_cols.push_back({get_seasonality_res, std::make_shared<DataTypeUInt64>(), "period"});
        }
        else
        {
            auto seasonality_col = arguments[2].column->convertToFullColumnIfConst();
            decompose_cols.push_back({seasonality_col, arguments[2].type, "period"});
        }

        auto get_decompose = FunctionFactory::instance().get("seriesDecomposeSTL", context)->build(decompose_cols);
        auto decompose_res_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
        auto get_decompose_res = get_decompose->execute(decompose_cols, decompose_res_type, input_rows_count);

        const ColumnArray * decompose_res_array = checkAndGetColumn<ColumnArray>(get_decompose_res.get());
        auto root_decompose_data = &decompose_res_array->getData();
        const IColumn::Offsets & root_decompose_offsets = decompose_res_array->getOffsets();

        const ColumnArray * decompose_data_array = checkAndGetColumn<ColumnArray>(root_decompose_data);
        const IColumn::Offsets & nested_decompose_offsets = decompose_data_array->getOffsets();
        const IColumn & src_data = decompose_data_array->getData();
        const ColumnVector<Float32> * src_data_concrete = checkAndGetColumn<ColumnVector<Float32>>(src_data);
        const PaddedPODArray<Float32> & decompose_data = src_data_concrete->getData();

        ColumnArray::Offset prev_decompose_offset = 0;

        auto res = ColumnFloat64::create();
        auto & res_data = res->getData();

        auto res_col_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_col_offsets_data = res_col_offsets->getData();

        auto root_offsets = ColumnArray::ColumnOffsets::create();
        auto & root_offsets_data = root_offsets->getData();

        for (size_t i = 0; i < root_decompose_offsets.size(); i++)
        {
            //extract residual component of the decomposed series
            ColumnArray::Offset residual_start_offset = nested_decompose_offsets[prev_decompose_offset + 1];
            ColumnArray::Offset residual_end_offset = nested_decompose_offsets[prev_decompose_offset + 2];

            auto residuals = ColumnFloat32::create();
            auto & residuals_data = residuals->getData();
            auto residuals_col_offsets = ColumnArray::ColumnOffsets::create();
            auto & residuals_col_offsets_data = residuals_col_offsets->getData();

            residuals_data.insertByOffsets(decompose_data, residual_start_offset, residual_end_offset);
            residuals_col_offsets_data.push_back(residuals_data.size());

            ColumnWithTypeAndName residuals_col;
            residuals_col.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<Float32>>());
            residuals_col.column = ColumnArray::create(std::move(residuals), std::move(residuals_col_offsets));

            auto min_percentile_column = DataTypeFloat64().createColumnConst(1, min_percentile);
            auto max_percentile_column = DataTypeFloat64().createColumnConst(1, max_percentile);
            auto k_value_column = DataTypeFloat64().createColumnConst(1, k_value);

            ColumnsWithTypeAndName outliers_arg{
                std::move(residuals_col),
                ColumnWithTypeAndName(min_percentile_column, std::make_shared<DataTypeFloat64>(), "min_percentile"),
                ColumnWithTypeAndName(max_percentile_column, std::make_shared<DataTypeFloat64>(), "max_percentile"),
                ColumnWithTypeAndName(k_value_column, std::make_shared<DataTypeFloat64>(), "K")};

            auto get_outliers = FunctionFactory::instance().get("seriesOutliersDetectTukey", context)->build(outliers_arg);
            auto get_outliers_res_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
            auto get_outliers_res = get_outliers->execute(outliers_arg, get_outliers_res_type, input_rows_count);

            const IColumn & outlier_score_col = checkAndGetColumn<ColumnArray>(get_outliers_res.get())->getData();
            const PaddedPODArray<Float64> & outlier_score = checkAndGetColumn<ColumnVector<Float64>>(&outlier_score_col)->getData();

            std::transform(
                outlier_score.begin(),
                outlier_score.end(),
                std::back_inserter(res_data),
                [](Float64 score)
                {
                    if (score > 0.0)
                        return 1;
                    else if (score < 0.0)
                        return -1;
                    return 0;
                });

            res_col_offsets_data.push_back(res_data.size());

            res_data.insert(outlier_score.begin(), outlier_score.end());
            res_col_offsets_data.push_back(res_data.size());

            ColumnArray::Offset baseline_start_offset = nested_decompose_offsets[prev_decompose_offset + 2];
            ColumnArray::Offset baseline_end_offset = nested_decompose_offsets[prev_decompose_offset + 3];

            std::transform(
                decompose_data.begin() + baseline_start_offset,
                decompose_data.begin() + baseline_end_offset,
                std::back_inserter(res_data),
                [](Float32 value) { return Float64(value); });

            res_col_offsets_data.push_back(res_data.size());

            root_offsets_data.push_back(res_col_offsets->size());

            prev_decompose_offset = root_decompose_offsets[i];
        }

        ColumnArray::MutablePtr nested_array_col = ColumnArray::create(std::move(res), std::move(res_col_offsets));
        return ColumnArray::create(std::move(nested_array_col), std::move(root_offsets));
    }

private:
    ContextPtr context;
};
REGISTER_FUNCTION(seriesAnomaliesDetectDecompose)
{
    factory.registerFunction<FunctionSeriesAnomaliesDetectDecompose>(FunctionDocumentation{
        .description = R"(Detect anomalies in series data by decomposing it into its constituent components through [series decomposition](#seriesDecomposeSTL) and then analyzing the residual component to detect unusual patterns.

**Syntax**

``` sql
seriesAnomaliesDetectDecompose(series);
seriesAnomaliesDetectDecompose(series, threshold, seasonality, AD_method);
```

**Arguments**

- `series` - An array of numeric values
- `threshold` - A positive number to detect mild or stronger anomalies. The default is 1.5, K value.
- `Seasonality` - Represents the period for the seasonal analysis of series data. Supported values are:
                - `-1`: Autodetect period using [seriesPeriodDetectFFT](#seriesPeriodDetectFFT). This is the default value.
                - `0`: Set period to 0 to skip extracting seasonal component.
                - A positive integer representing the period of series.
- `AD_method` - The method to use for anomaly detection on residual component. Supported values are:
                - `tukey` : [Tukey fence](#seriesoutliersdetecttukey) test with standard 25th-75th percentile range.
                - `ctukey`: Tukey fence test with custom 10th-90th percentile range. This is the default value.

The number of data points in `series` should be at least twice the value of `period`.

**Returned value**

- An array of three arrays where the first array includes a ternary series containing (+1,-1,0) marking up/down/no anomaly respectively, the second array - anomaly score,
the third array - baseline(seasonal + trend) component.

Type: [Array](../../sql-reference/data-types/array.md).

**Examples**

Query:

``` sql
SELECT seriesAnomaliesDetectDecompose([4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3, 2], 1.5, -1, 'tukey') AS print_0;
```

Result:

``` text
┌───────────print_0─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [[
        1, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, -1, 0
    ],
    [
        2.384185791015625e-7, 0, 0, 0, 0, 0, 2.384185791015625e-7, 2.384185791015625e-7, 0, 2.384185791015625e-7,
        0, 0, 0, 0, 2.384185791015625e-7, 0, 0, 0, 0, -2.384185791015625e-7, 0
    ],
    [
        3.999999761581421, 3, 2, 4, 3, 2, 4, 2.999999761581421, 1.9999998807907104, 4, 3, 2, 4,
        3,1.9999996423721313, 4, 3, 2, 4, 3.000000238418579, 2]
    ]                                                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
)",
        .categories{"Time series analysis"}});
}
}
