#include <Functions/IFunctionImpl.h>
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

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBlockSerializedSize>();
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

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

        IDataType::SerializeBinaryBulkSettings settings;
        NullWriteBuffer out;

        settings.getter = [&out](IDataType::SubstreamPath) -> WriteBuffer * { return &out; };

        IDataType::SerializeBinaryBulkStatePtr state;

        elem.type->serializeBinaryBulkStatePrefix(settings, state);
        elem.type->serializeBinaryBulkWithMultipleStreams(*full_column,
            0 /** offset */, 0 /** limit */,
            settings, state);
        elem.type->serializeBinaryBulkStateSuffix(settings, state);

        return out.count();
    }
};

}

void registerFunctionBlockSerializedSize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBlockSerializedSize>();
}

}
