#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnFixedString.h>
#include <Common/typeid_cast.h>
#include <Common/memcpySmall.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionReinterpretAsFixedString : public IFunction
{
public:
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionReinterpretAsFixedString>(); }

    static constexpr auto name = "reinterpretAsFixedString";

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType & type = *arguments[0];

        if (type.isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            return std::make_shared<DataTypeFixedString>(type.getSizeOfValueInMemory());
        throw Exception("Cannot reinterpret " + type.getName() + " as FixedString because it is not fixed size and contiguous in memory", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    static void NO_INLINE executeToFixedString(const IColumn & src, ColumnFixedString & dst, size_t n)
    {
        size_t rows = src.size();
        ColumnFixedString::Chars & data_to = dst.getChars();
        data_to.resize(n * rows);

        ColumnFixedString::Offset offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            StringRef data = src.getDataAt(i);
            memcpySmallAllowReadWriteOverflow15(&data_to[offset], data.data, n);
            offset += n;
        }
    }

    static void NO_INLINE executeContiguousToFixedString(const IColumn & src, ColumnFixedString & dst, size_t n)
    {
        size_t rows = src.size();
        ColumnFixedString::Chars & data_to = dst.getChars();
        data_to.resize(n * rows);

        memcpy(data_to.data(), src.getRawData().data, data_to.size());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const IColumn & src = *arguments[0].column;
        MutableColumnPtr dst = result_type->createColumn();

        if (ColumnFixedString * dst_concrete = typeid_cast<ColumnFixedString *>(dst.get()))
        {
            if (src.isFixedAndContiguous() && src.sizeOfValueIfFixed() == dst_concrete->getN())
                executeContiguousToFixedString(src, *dst_concrete, dst_concrete->getN());
            else
                executeToFixedString(src, *dst_concrete, dst_concrete->getN());
        }
        else
            throw Exception("Illegal column " + src.getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        return dst;
    }
};

}

void registerFunctionReinterpretAsFixedString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReinterpretAsFixedString>();
}

}

