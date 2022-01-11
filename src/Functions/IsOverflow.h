#pragma once
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>


namespace DB
{

template <typename FromType, typename ToType>
struct IsOverflowNoOp
{
    static inline bool overflow(const FromType &)
    {
        return false;
    }
};

template <typename FromType, typename ToType>
struct IsOverflowDateOrDate32From32Or64
{
    static inline bool overflow(const FromType & from)
    {
        if (std::is_same_v<ToType, UInt16>)
            return from >= DATE_LUT_MAX_DAY_NUM;
        else
            return from >= DATE_LUT_MAX_EXTEND_DAY_NUM;
    }
};

template <typename FromType, typename ToType>
struct IsOverflowDateOrDate32From32Or64Signed
{
    static inline bool overflow(const FromType & from)
    {
        if (std::is_same_v<ToType, UInt16>)
        {
            return from < 0 || from >= DATE_LUT_MAX_DAY_NUM;
        }
        else
        {
            static const Int32 daynum_min_offset = DataTypeDate32::getZeroValue();
            return from < daynum_min_offset || from >= DATE_LUT_MAX_EXTEND_DAY_NUM;
        }
    }
};

template <typename FromType, typename ToType>
struct IsOverflowDateOrDate32From8Or16Signed
{
    static inline bool overflow(const FromType &)
    {
        return false;
    }
};

template <typename Type> constexpr bool IsInteger = false;

template <> inline constexpr bool IsInteger<Int8> = true;
template <> inline constexpr bool IsInteger<Int16> = true;
template <> inline constexpr bool IsInteger<Int32> = true;
template <> inline constexpr bool IsInteger<Int64> = true;
template <> inline constexpr bool IsInteger<UInt8> = true;
template <> inline constexpr bool IsInteger<UInt16> = true;
template <> inline constexpr bool IsInteger<UInt32> = true;
template <> inline constexpr bool IsInteger<UInt64> = true;

}
