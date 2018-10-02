#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Common/memcpySmall.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/** Functions for transforming numbers and dates to strings that contain the same set of bytes in the machine representation, and vice versa.
  */


template <typename Name>
class FunctionReinterpretAsStringImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionReinterpretAsStringImpl>(); }

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

    void executeToString(const IColumn & src, ColumnString & dst)
    {
        size_t rows = src.size();
        ColumnString::Chars_t & data_to = dst.getChars();
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn & src = *block.getByPosition(arguments[0]).column;
        MutableColumnPtr dst = block.getByPosition(result).type->createColumn();

        if (ColumnString * dst_concrete = typeid_cast<ColumnString *>(dst.get()))
            executeToString(src, *dst_concrete);
        else
            throw Exception("Illegal column " + src.getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(dst);
    }
};


template <typename Name>
class FunctionReinterpretAsFixedStringImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionReinterpretAsFixedStringImpl>(); }

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

    void executeToFixedString(const IColumn & src, ColumnFixedString & dst, size_t n)
    {
        size_t rows = src.size();
        ColumnFixedString::Chars_t & data_to = dst.getChars();
        data_to.resize(n * rows);

        ColumnFixedString::Offset offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            StringRef data = src.getDataAt(i);
            memcpySmallAllowReadWriteOverflow15(&data_to[offset], data.data, n);
            offset += n;
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn & src = *block.getByPosition(arguments[0]).column;
        MutableColumnPtr dst = block.getByPosition(result).type->createColumn();

        if (ColumnFixedString * dst_concrete = typeid_cast<ColumnFixedString *>(dst.get()))
            executeToFixedString(src, *dst_concrete, dst_concrete->getN());
        else
            throw Exception("Illegal column " + src.getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(dst);
    }
};


template <typename ToDataType, typename Name>
class FunctionReinterpretStringAs : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionReinterpretStringAs>(); }

    using ToFieldType = typename ToDataType::FieldType;

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType & type = *arguments[0];
        if (!isStringOrFixedString(type))
            throw Exception("Cannot reinterpret " + type.getName() + " as " + ToDataType().getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (const ColumnString * col_from = typeid_cast<const ColumnString *>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_res = ColumnVector<ToFieldType>::create();

            const ColumnString::Chars_t & data_from = col_from->getChars();
            const ColumnString::Offsets & offsets_from = col_from->getOffsets();
            size_t size = offsets_from.size();
            typename ColumnVector<ToFieldType>::Container & vec_res = col_res->getData();
            vec_res.resize(size);

            size_t offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                ToFieldType value = 0;
                memcpy(&value, &data_from[offset], std::min(static_cast<UInt64>(sizeof(ToFieldType)), offsets_from[i] - offset - 1));
                vec_res[i] = value;
                offset = offsets_from[i];
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col_from = typeid_cast<const ColumnFixedString *>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_res = ColumnVector<ToFieldType>::create();

            const ColumnString::Chars_t & data_from = col_from->getChars();
            size_t step = col_from->getN();
            size_t size = data_from.size() / step;
            typename ColumnVector<ToFieldType>::Container & vec_res = col_res->getData();
            vec_res.resize(size);

            size_t offset = 0;
            size_t copy_size = std::min(step, sizeof(ToFieldType));
            for (size_t i = 0; i < size; ++i)
            {
                ToFieldType value = 0;
                memcpy(&value, &data_from[offset], copy_size);
                vec_res[i] = value;
                offset += step;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


struct NameReinterpretAsUInt8       { static constexpr auto name = "reinterpretAsUInt8"; };
struct NameReinterpretAsUInt16      { static constexpr auto name = "reinterpretAsUInt16"; };
struct NameReinterpretAsUInt32      { static constexpr auto name = "reinterpretAsUInt32"; };
struct NameReinterpretAsUInt64      { static constexpr auto name = "reinterpretAsUInt64"; };
struct NameReinterpretAsInt8        { static constexpr auto name = "reinterpretAsInt8"; };
struct NameReinterpretAsInt16       { static constexpr auto name = "reinterpretAsInt16"; };
struct NameReinterpretAsInt32       { static constexpr auto name = "reinterpretAsInt32"; };
struct NameReinterpretAsInt64       { static constexpr auto name = "reinterpretAsInt64"; };
struct NameReinterpretAsFloat32     { static constexpr auto name = "reinterpretAsFloat32"; };
struct NameReinterpretAsFloat64     { static constexpr auto name = "reinterpretAsFloat64"; };
struct NameReinterpretAsDate        { static constexpr auto name = "reinterpretAsDate"; };
struct NameReinterpretAsDateTime    { static constexpr auto name = "reinterpretAsDateTime"; };
struct NameReinterpretAsString      { static constexpr auto name = "reinterpretAsString"; };
struct NameReinterpretAsFixedString      { static constexpr auto name = "reinterpretAsFixedString"; };

using FunctionReinterpretAsUInt8 = FunctionReinterpretStringAs<DataTypeUInt8,       NameReinterpretAsUInt8>;
using FunctionReinterpretAsUInt16 = FunctionReinterpretStringAs<DataTypeUInt16,     NameReinterpretAsUInt16>;
using FunctionReinterpretAsUInt32 = FunctionReinterpretStringAs<DataTypeUInt32,     NameReinterpretAsUInt32>;
using FunctionReinterpretAsUInt64 = FunctionReinterpretStringAs<DataTypeUInt64,     NameReinterpretAsUInt64>;
using FunctionReinterpretAsInt8 = FunctionReinterpretStringAs<DataTypeInt8,         NameReinterpretAsInt8>;
using FunctionReinterpretAsInt16 = FunctionReinterpretStringAs<DataTypeInt16,       NameReinterpretAsInt16>;
using FunctionReinterpretAsInt32 = FunctionReinterpretStringAs<DataTypeInt32,       NameReinterpretAsInt32>;
using FunctionReinterpretAsInt64 = FunctionReinterpretStringAs<DataTypeInt64,       NameReinterpretAsInt64>;
using FunctionReinterpretAsFloat32 = FunctionReinterpretStringAs<DataTypeFloat32,   NameReinterpretAsFloat32>;
using FunctionReinterpretAsFloat64 = FunctionReinterpretStringAs<DataTypeFloat64,   NameReinterpretAsFloat64>;
using FunctionReinterpretAsDate = FunctionReinterpretStringAs<DataTypeDate,         NameReinterpretAsDate>;
using FunctionReinterpretAsDateTime = FunctionReinterpretStringAs<DataTypeDateTime, NameReinterpretAsDateTime>;

using FunctionReinterpretAsString = FunctionReinterpretAsStringImpl<NameReinterpretAsString>;
using FunctionReinterpretAsFixedString = FunctionReinterpretAsStringImpl<NameReinterpretAsFixedString>;


}
