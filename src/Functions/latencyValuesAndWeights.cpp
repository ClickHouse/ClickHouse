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

            String result;

            if (!sketch_data.empty())
            {
                try
                {
                    /// ClickHouse aggregate functions (serializedQuantiles, mergeSerializedQuantiles)
                    /// always return raw binary data, never base64. Skip base64 detection for performance.
                    /// If users need to decode base64 sketch data from external sources, they should
                    /// use base64Decode() explicitly before calling this function.
                    std::string decoded_storage;
                    auto [data_ptr, data_size] = decodeSketchData(sketch_data, decoded_storage, /* base64_encoded= */ false);

                    if (data_ptr == nullptr || data_size == 0)
                    {
                        result = "{}";
                        col_to->insertData(result.c_str(), result.size());
                        continue;
                    }

                    auto sketch = datasketches::quantiles_sketch<double>::deserialize(data_ptr, data_size);

                    WriteBufferFromOwnString buf;
                    writeChar('{', buf);
                    bool first = true;
                    for (const auto&& node : sketch)
                    {
                        double value = node.first;
                        UInt64 weight = static_cast<UInt64>(node.second);
                        if (!first)
                        {
                            writeChar(',', buf);
                        }
                        else
                        {
                            first = false;
                        }
                        writeChar('"', buf);
                        writeText(value, buf);
                        writeChar('"', buf);
                        writeChar(':', buf);
                        writeText(weight, buf);
                    }
                    writeChar('}', buf);
                    result = buf.str();
                }
                catch (...)
                {
                    result = "{}";
                }
            }
            else
            {
                result = "{}";
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

Returns a JSON-formatted String containing value-weight pairs from the sketch.
)",
        .examples{{"latencyValuesAndWeights", "SELECT latencyValuesAndWeights(serializedQuantiles(latency_ms)) FROM table", ""}},
        .category = FunctionDocumentation::Category::Other
    });
}

}

#endif
