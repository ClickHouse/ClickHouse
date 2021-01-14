#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Common/memcpySmall.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Function for transforming numbers and dates to strings that contain the same set of bytes in the machine representation. */
class FunctionReinterpretAsString : public IFunction
{
public:
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionReinterpretAsString>(); }

    static constexpr auto name = "reinterpretAsString";

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType & type = *arguments[0];

        if (type.isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return std::make_shared<DataTypeString>();
        throw Exception("Cannot reinterpret " + type.getName() + " as String because it is not contiguous in memory", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static void executeToString(const IColumn & src, ColumnString & dst)
    {
        size_t rows = src.size();
        ColumnString::Chars & data_to = dst.getChars();
        ColumnString::Offsets & offsets_to = dst.getOffsets();
        offsets_to.resize(rows);

        ColumnString::Offset offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            StringRef data = src.getDataAt(i);

            /// Cut trailing zero bytes.
            while (data.size && data.data[data.size - 1] == 0)
                --data.size;

            data_to.resize(offset + data.size + 1);
            memcpySmallAllowReadWriteOverflow15(&data_to[offset], data.data, data.size);
            offset += data.size;
            data_to[offset] = 0;
            ++offset;
            offsets_to[i] = offset;
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const IColumn & src = *arguments[0].column;
        MutableColumnPtr dst = result_type->createColumn();

        if (ColumnString * dst_concrete = typeid_cast<ColumnString *>(dst.get()))
            executeToString(src, *dst_concrete);
        else
            throw Exception("Illegal column " + src.getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        return dst;
    }
};

}

void registerFunctionReinterpretAsString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReinterpretAsString>();
}

}
