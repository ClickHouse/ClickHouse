#pragma once

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace detail
{
/** Transformation of numbers, dates, datetimes to strings: through formatting.
  */
template <typename DataType>
struct FormatImpl
{
    template <typename ReturnType = void>
    static ReturnType execute(const typename DataType::FieldType x, WriteBuffer & wb, const DataType *, const DateLUTImpl *)
    {
        writeText(x, wb);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDate>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDate::FieldType x, WriteBuffer & wb, const DataTypeDate *, const DateLUTImpl * time_zone)
    {
        writeDateText(DayNum(x), wb, *time_zone);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDate32>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDate32::FieldType x, WriteBuffer & wb, const DataTypeDate32 *, const DateLUTImpl * time_zone)
    {
        writeDateText(ExtendedDayNum(x), wb, *time_zone);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDateTime>
{
    template <typename ReturnType = void>
    static ReturnType
    execute(const DataTypeDateTime::FieldType x, WriteBuffer & wb, const DataTypeDateTime *, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(x, wb, *time_zone);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDateTime64>
{
    template <typename ReturnType = void>
    static ReturnType
    execute(const DataTypeDateTime64::FieldType x, WriteBuffer & wb, const DataTypeDateTime64 * type, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(DateTime64(x), type->getScale(), wb, *time_zone);
        return ReturnType(true);
    }
};


template <typename FieldType>
struct FormatImpl<DataTypeEnum<FieldType>>
{
    template <typename ReturnType = void>
    static ReturnType execute(const FieldType x, WriteBuffer & wb, const DataTypeEnum<FieldType> * type, const DateLUTImpl *)
    {
        static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

        if constexpr (throw_exception)
        {
            writeString(type->getNameForValue(x), wb);
        }
        else
        {
            StringRef res;
            bool is_ok = type->getNameForValue(x, res);
            if (is_ok)
                writeString(res, wb);
            return ReturnType(is_ok);
        }
    }
};

template <typename FieldType>
struct FormatImpl<DataTypeDecimal<FieldType>>
{
    template <typename ReturnType = void>
    static ReturnType execute(const FieldType x, WriteBuffer & wb, const DataTypeDecimal<FieldType> * type, const DateLUTImpl *)
    {
        writeText(x, type->getScale(), wb, false);
        return ReturnType(true);
    }
};

}
}
