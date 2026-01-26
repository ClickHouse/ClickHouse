#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

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
}

class FunctionCentroidsFromTDigest : public IFunction
{
public:
    static constexpr auto name = "centroidsFromTDigest";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCentroidsFromTDigest>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be String (serialized TDigest sketch)", getName());

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
            String result = "{}";

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
                        col_to->insertData(result.c_str(), result.size());
                        continue;
                    }

                    auto sketch = datasketches::tdigest<double>::deserialize(data_ptr, data_size);

                    sketch.compress();

                    WriteBufferFromOwnString buf;
                    writeChar('{', buf);
                    bool first = true;
                    for (const auto& centroid : sketch.get_centroids())
                    {
                        if (!first)
                            writeChar(',', buf);
                        else
                            first = false;

                        writeChar('"', buf);
                        writeText(centroid.get_mean(), buf);
                        writeChar('"', buf);
                        writeChar(':', buf);
                        writeText(static_cast<Int64>(centroid.get_weight()), buf);
                    }
                    writeChar('}', buf);
                    result = buf.str();
                }
                catch (...) // NOLINT(bugprone-empty-catch)
                {
                    /// Best-effort: ignore on invalid/corrupted input.
                }
            }

            col_to->insertData(result.c_str(), result.size());
        }

        return col_to;
    }
};

REGISTER_FUNCTION(CentroidsFromTDigest)
{
    factory.registerFunction<FunctionCentroidsFromTDigest>(FunctionDocumentation{
        .description = R"(
Extracts centroids from a serialized TDigest sketch.

Returns a Map<Float64, Int64> where keys are centroid means and values are weights.
)",
        .examples{{"centroidsFromTDigest", "SELECT centroidsFromTDigest(serializedTDigest(value)) FROM table", ""}},
        .category = FunctionDocumentation::Category::Other
    });
}

}

#endif
