#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Core/Types.h>

#include "config.h"

#if USE_DATASKETCHES

#include <quantiles_sketch.hpp>
#include <AggregateFunctions/SketchDataUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionLatencyValuesAndWeights : public IFunction
{
public:
    static constexpr auto name = "latencyValuesAndWeights";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionLatencyValuesAndWeights>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be String (serialized Quantiles sketch)", getName());

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_sketch = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_sketch)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "First argument for function {} must be ColumnString", getName());

        auto col_to = ColumnString::create();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            std::string_view sketch_data = col_sketch->getDataAt(row);

            String result = R"({"values":[],"weights":[]})";

            if (!sketch_data.empty())
            {
                /// ClickHouse aggregate functions (serializedQuantiles, mergeSerializedQuantiles)
                /// always return raw binary data, never base64. Skip base64 detection for performance.
                /// If users need to decode base64 sketch data from external sources, they should
                /// use base64Decode() explicitly before calling this function.
                std::string decoded_storage;
                auto [data_ptr, data_size] = decodeSketchData(sketch_data, decoded_storage, /* base64_encoded= */ false);

                if (data_ptr == nullptr || data_size == 0)
                {
                    col_to->insertData(result.c_str(), result.size());
                    continue;
                }

                /// Fail close: malformed sketch bytes throw INCORRECT_DATA instead of
                /// being silently coerced to an empty result.
                auto sketch = deserializeSketch<datasketches::quantiles_sketch<double>>(data_ptr, data_size);

                /// Emit parallel arrays instead of an object keyed by value: a Quantiles sketch
                /// can legitimately retain the same value multiple times, and JSON object keys
                /// would collapse such entries, losing data.
                WriteBufferFromOwnString values_buf;
                WriteBufferFromOwnString weights_buf;
                bool first = true;
                for (const auto && node : sketch)
                {
                    double value = node.first;
                    UInt64 weight = static_cast<UInt64>(node.second);
                    if (!first)
                    {
                        writeChar(',', values_buf);
                        writeChar(',', weights_buf);
                    }
                    else
                        first = false;

                    writeText(value, values_buf);
                    writeText(weight, weights_buf);
                }

                WriteBufferFromOwnString buf;
                writeCString(R"({"values":[)", buf);
                writeString(values_buf.str(), buf);
                writeCString(R"(],"weights":[)", buf);
                writeString(weights_buf.str(), buf);
                writeCString("]}", buf);
                result = buf.str();
            }

            col_to->insertData(result.c_str(), result.size());
        }

        return col_to;
    }
};

REGISTER_FUNCTION(LatencyValuesAndWeights)
{
    factory.registerFunction<FunctionLatencyValuesAndWeights>(FunctionDocumentation{
        .description = R"(
Extracts values and weights from a serialized Quantiles sketch.

Returns a JSON string of the form {"values":[...],"weights":[...]} with parallel arrays
of retained values and their weights. Parallel arrays are used because a Quantiles sketch
can retain the same value multiple times.
)",
        .syntax = "latencyValuesAndWeights(serialized_sketch)",
        .examples{{"latencyValuesAndWeights", "SELECT latencyValuesAndWeights(serializedQuantiles(latency_ms)) FROM table", ""}},
        .introduced_in = {26, 1},
        .category = FunctionDocumentation::Category::Other
    });
}

}

#endif
