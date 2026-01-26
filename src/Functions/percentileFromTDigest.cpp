#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include "config.h"

#if USE_DATASKETCHES

#include <tdigest.hpp>
#include <AggregateFunctions/SketchDataUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

class FunctionPercentileFromTDigest : public IFunction
{
public:
    static constexpr auto name = "percentileFromTDigest";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPercentileFromTDigest>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be String (serialized TDigest sketch)", getName());

        if (!isNumber(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a numeric percentile value (0.0-1.0)", getName());

        return std::make_shared<DataTypeFloat64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_sketch = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_sketch)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "First argument for function {} must be ColumnString", getName());

        const auto & col_percentile = arguments[1].column;

        auto col_to = ColumnFloat64::create();
        auto & result_data = col_to->getData();
        result_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view sketch_data = col_sketch->getDataAt(row);
            Float64 percentile = col_percentile->getFloat64(row);

            if (percentile < 0.0 || percentile > 1.0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Percentile must be between 0.0 and 1.0 for function {}, got {}", getName(), percentile);

            Float64 result = std::numeric_limits<Float64>::quiet_NaN();

            if (!sketch_data.empty())
            {
                try
                {
                    /// ClickHouse aggregate functions (serializedTDigest, mergeSerializedTDigest) always
                    /// return raw binary data, never base64. Skip base64 detection for performance.
                    /// If users need to decode base64 sketch data from external sources, they should
                    /// use base64Decode() explicitly before calling this function.
                    std::string decoded_storage;
                    auto [data_ptr, data_size] = decodeSketchData(sketch_data, decoded_storage, /* base64_encoded= */ false);

                    if (data_ptr == nullptr || data_size == 0)
                    {
                        result_data.push_back(result);
                        continue;
                    }

                    auto sketch = datasketches::tdigest<double>::deserialize(data_ptr, data_size);

                    if (!sketch.is_empty())
                    {
                        result = sketch.get_quantile(percentile);
                    }
                }
                catch (...)
                {
                    /// Keep NaN on invalid/corrupted input (consistent with percentileFromQuantiles()).
                    result = std::numeric_limits<Float64>::quiet_NaN();
                }
            }

            result_data.push_back(result);
        }

        return col_to;
    }
};

REGISTER_FUNCTION(PercentileFromTDigest)
{
    factory.registerFunction<FunctionPercentileFromTDigest>(FunctionDocumentation{
        .description = R"(
Extracts a specific percentile value from a serialized TDigest sketch.

The percentile parameter should be between 0.0 and 1.0 (e.g., 0.5 for median, 0.95 for p95).
Returns NaN for empty sketches.
)",
        .examples{{"percentileFromTDigest", "SELECT percentileFromTDigest(serializedTDigest(value), 0.95) FROM table", ""}},
        .category = FunctionDocumentation::Category::Other
    });
}

}

#endif
