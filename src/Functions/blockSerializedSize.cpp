#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/NullWriteBuffer.h>


namespace DB
{
namespace
{

/// Returns size on disk for *columns* (without taking into account compression).
class FunctionBlockSerializedSize : public IFunction
{
public:
    static constexpr auto name = "blockSerializedSize";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBlockSerializedSize>();
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        UInt64 size = 0;

        for (const auto & arg : arguments)
            size += columnsSerializedSizeOne(arg);

        return DataTypeUInt64().createColumnConst(input_rows_count, size)->convertToFullColumnIfConst();
    }

    static UInt64 columnsSerializedSizeOne(const ColumnWithTypeAndName & elem)
    {
        ColumnPtr full_column = elem.column->convertToFullColumnIfConst();

        ISerialization::SerializeBinaryBulkSettings settings;
        NullWriteBuffer out;

        settings.getter = [&out](ISerialization::SubstreamPath) -> WriteBuffer * { return &out; };

        ISerialization::SerializeBinaryBulkStatePtr state;

        auto serialization = elem.type->getDefaultSerialization();

        serialization->serializeBinaryBulkStatePrefix(*full_column, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*full_column,
            0 /** offset */, 0 /** limit */,
            settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);

        out.finalize();
        return out.count();
    }
};

}

REGISTER_FUNCTION(BlockSerializedSize)
{
    FunctionDocumentation::Description description = R"(
Returns the uncompressed size in bytes of a block of values on disk.
)";
    FunctionDocumentation::Syntax syntax = "blockSerializedSize(x1[, x2[, ...]])";
    FunctionDocumentation::Arguments arguments = {
        {"x1[, x2, ...]", "Any number of values for which to get the uncompressed size of the block.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of bytes that will be written to disk for a block of values without compression.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT blockSerializedSize(maxState(1)) AS x;
        )",
        R"(
┌─x─┐
│ 2 │
└───┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBlockSerializedSize>(documentation);
}

}
