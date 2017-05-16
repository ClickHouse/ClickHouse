#pragma once

#include <IO/ReadBufferFromString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>


namespace DB
{

/** Functions for transforming numbers and dates to strings that contain the same set of bytes in the machine representation, and vice versa.
    */


template<typename Name>
class FunctionReinterpretAsStringImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionReinterpretAsStringImpl>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType * type = &*arguments[0];
        if (!type->isNumeric() &&
            !typeid_cast<const DataTypeDate *>(type) &&
            !typeid_cast<const DataTypeDateTime *>(type))
            throw Exception("Cannot reinterpret " + type->getName() + " as String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template<typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<T> * col_from = typeid_cast<const ColumnVector<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_to = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = col_to;

            const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();
            data_to.resize(size * (sizeof(T) + 1));
            offsets_to.resize(size);
            int pos = 0;

            for (size_t i = 0; i < size; ++i)
            {
                memcpy(&data_to[pos], &vec_from[i], sizeof(T));

                int len = sizeof(T);
                while (len > 0 && data_to[pos + len - 1] == '\0')
                    --len;

                pos += len;
                data_to[pos++] = '\0';

                offsets_to[i] = pos;
            }
            data_to.resize(pos);
        }
        else if (const ColumnConst<T> * col_from = typeid_cast<const ColumnConst<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            std::string res(reinterpret_cast<const char *>(&col_from->getData()), sizeof(T));
            while (!res.empty() && res[res.length() - 1] == '\0')
                res.erase(res.end() - 1);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(col_from->size(), res);
        }
        else
        {
            return false;
        }

        return true;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeType<UInt8>(block, arguments, result)
            ||    executeType<UInt16>(block, arguments, result)
            ||    executeType<UInt32>(block, arguments, result)
            ||    executeType<UInt64>(block, arguments, result)
            ||    executeType<Int8>(block, arguments, result)
            ||    executeType<Int16>(block, arguments, result)
            ||    executeType<Int32>(block, arguments, result)
            ||    executeType<Int64>(block, arguments, result)
            ||    executeType<Float32>(block, arguments, result)
            ||    executeType<Float64>(block, arguments, result)))
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
    }
};

template<typename ToDataType, typename Name>
class FunctionReinterpretStringAs : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionReinterpretStringAs>(); };

    using ToFieldType = typename ToDataType::FieldType;

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType * type = &*arguments[0];
        if (!typeid_cast<const DataTypeString *>(type) &&
            !typeid_cast<const DataTypeFixedString *>(type))
            throw Exception("Cannot reinterpret " + type->getName() + " as " + ToDataType().getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<ToDataType>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (ColumnString * col_from = typeid_cast<ColumnString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_res = std::make_shared<ColumnVector<ToFieldType>>();
            block.safeGetByPosition(result).column = col_res;

            ColumnString::Chars_t & data_from = col_from->getChars();
            ColumnString::Offsets_t & offsets_from = col_from->getOffsets();
            size_t size = offsets_from.size();
            typename ColumnVector<ToFieldType>::Container_t & vec_res = col_res->getData();
            vec_res.resize(size);

            size_t offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                ToFieldType value = 0;
                memcpy(&value, &data_from[offset], std::min(sizeof(ToFieldType), offsets_from[i] - offset - 1));
                vec_res[i] = value;
                offset = offsets_from[i];
            }
        }
        else if (ColumnFixedString * col_from = typeid_cast<ColumnFixedString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_res = std::make_shared<ColumnVector<ToFieldType>>();
            block.safeGetByPosition(result).column = col_res;

            ColumnString::Chars_t & data_from = col_from->getChars();
            size_t step = col_from->getN();
            size_t size = data_from.size() / step;
            typename ColumnVector<ToFieldType>::Container_t & vec_res = col_res->getData();
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
        }
        else if (ColumnConst<String> * col = typeid_cast<ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            ToFieldType value = 0;
            const String & str = col->getData();
            memcpy(&value, str.data(), std::min(sizeof(ToFieldType), str.length()));
            auto col_res = std::make_shared<ColumnConst<ToFieldType>>(col->size(), value);
            block.safeGetByPosition(result).column = col_res;
        }
        else
        {
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
            + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


struct NameReinterpretAsUInt8         { static constexpr auto name = "reinterpretAsUInt8"; };
struct NameReinterpretAsUInt16        { static constexpr auto name = "reinterpretAsUInt16"; };
struct NameReinterpretAsUInt32        { static constexpr auto name = "reinterpretAsUInt32"; };
struct NameReinterpretAsUInt64        { static constexpr auto name = "reinterpretAsUInt64"; };
struct NameReinterpretAsInt8         { static constexpr auto name = "reinterpretAsInt8"; };
struct NameReinterpretAsInt16         { static constexpr auto name = "reinterpretAsInt16"; };
struct NameReinterpretAsInt32        { static constexpr auto name = "reinterpretAsInt32"; };
struct NameReinterpretAsInt64        { static constexpr auto name = "reinterpretAsInt64"; };
struct NameReinterpretAsFloat32        { static constexpr auto name = "reinterpretAsFloat32"; };
struct NameReinterpretAsFloat64        { static constexpr auto name = "reinterpretAsFloat64"; };
struct NameReinterpretAsDate        { static constexpr auto name = "reinterpretAsDate"; };
struct NameReinterpretAsDateTime    { static constexpr auto name = "reinterpretAsDateTime"; };
struct NameReinterpretAsString        { static constexpr auto name = "reinterpretAsString"; };

using FunctionReinterpretAsUInt8 = FunctionReinterpretStringAs<DataTypeUInt8,        NameReinterpretAsUInt8>    ;
using FunctionReinterpretAsUInt16 = FunctionReinterpretStringAs<DataTypeUInt16,    NameReinterpretAsUInt16>;
using FunctionReinterpretAsUInt32 = FunctionReinterpretStringAs<DataTypeUInt32,    NameReinterpretAsUInt32>;
using FunctionReinterpretAsUInt64 = FunctionReinterpretStringAs<DataTypeUInt64,    NameReinterpretAsUInt64>;
using FunctionReinterpretAsInt8 = FunctionReinterpretStringAs<DataTypeInt8,        NameReinterpretAsInt8>    ;
using FunctionReinterpretAsInt16 = FunctionReinterpretStringAs<DataTypeInt16,        NameReinterpretAsInt16>    ;
using FunctionReinterpretAsInt32 = FunctionReinterpretStringAs<DataTypeInt32,        NameReinterpretAsInt32>    ;
using FunctionReinterpretAsInt64 = FunctionReinterpretStringAs<DataTypeInt64,        NameReinterpretAsInt64>    ;
using FunctionReinterpretAsFloat32 = FunctionReinterpretStringAs<DataTypeFloat32,    NameReinterpretAsFloat32>;
using FunctionReinterpretAsFloat64 = FunctionReinterpretStringAs<DataTypeFloat64,    NameReinterpretAsFloat64>;
using FunctionReinterpretAsDate = FunctionReinterpretStringAs<DataTypeDate,        NameReinterpretAsDate>    ;
using FunctionReinterpretAsDateTime = FunctionReinterpretStringAs<DataTypeDateTime,    NameReinterpretAsDateTime>;

using FunctionReinterpretAsString = FunctionReinterpretAsStringImpl<NameReinterpretAsString>;


}
