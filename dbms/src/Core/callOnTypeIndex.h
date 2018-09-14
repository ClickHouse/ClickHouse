#pragma once

#include <utility>

#include <Core/Types.h>

namespace DB
{

template <typename T, typename U>
struct TypePair
{
    using LeftType = T;
    using RightType = U;
};



template <typename T, bool _int, bool _float, bool _dec, typename F>
bool callOnBasicType(TypeIndex number, F && f)
{
    if constexpr (_int)
    {
        switch (number)
        {
            case TypeIndex::UInt8:        return f(TypePair<T, UInt8>());
            case TypeIndex::UInt16:       return f(TypePair<T, UInt16>());
            case TypeIndex::UInt32:       return f(TypePair<T, UInt32>());
            case TypeIndex::UInt64:       return f(TypePair<T, UInt64>());
            //case TypeIndex::UInt128>:     return f(TypePair<T, UInt128>());

            case TypeIndex::Int8:         return f(TypePair<T, Int8>());
            case TypeIndex::Int16:        return f(TypePair<T, Int16>());
            case TypeIndex::Int32:        return f(TypePair<T, Int32>());
            case TypeIndex::Int64:        return f(TypePair<T, Int64>());
            case TypeIndex::Int128:       return f(TypePair<T, Int128>());

            default:
                break;
        }
    }

    if constexpr (_dec)
    {
        switch (number)
        {
            case TypeIndex::Decimal32:    return f(TypePair<T, Decimal32>());
            case TypeIndex::Decimal64:    return f(TypePair<T, Decimal64>());
            case TypeIndex::Decimal128:   return f(TypePair<T, Decimal128>());
            default:
                break;
        }
    }

    if constexpr (_float)
    {
        switch (number)
        {
            case TypeIndex::Float32:      return f(TypePair<T, Float32>());
            case TypeIndex::Float64:      return f(TypePair<T, Float64>());
            default:
                break;
        }
    }

    return false;
}

/// Unroll template using TypeIndex
template <bool _int, bool _float, bool _dec, typename F>
inline bool callOnBasicTypes(TypeIndex type_num1, TypeIndex type_num2, F && f)
{
    if constexpr (_int)
    {
        switch (type_num1)
        {
            case TypeIndex::UInt8: return callOnBasicType<UInt8, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::UInt16: return callOnBasicType<UInt16, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::UInt32: return callOnBasicType<UInt32, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::UInt64: return callOnBasicType<UInt64, _int, _float, _dec>(type_num2, std::forward<F>(f));
            //case TypeIndex::UInt128: return callOnBasicType<UInt128, _int, _float, _dec>(type_num2, std::forward<F>(f));

            case TypeIndex::Int8: return callOnBasicType<Int8, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::Int16: return callOnBasicType<Int16, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::Int32: return callOnBasicType<Int32, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::Int64: return callOnBasicType<Int64, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::Int128: return callOnBasicType<Int128, _int, _float, _dec>(type_num2, std::forward<F>(f));
            default:
                break;
        }
    }

    if constexpr (_dec)
    {
        switch (type_num1)
        {
            case TypeIndex::Decimal32: return callOnBasicType<Decimal32, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::Decimal64: return callOnBasicType<Decimal64, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::Decimal128: return callOnBasicType<Decimal128, _int, _float, _dec>(type_num2, std::forward<F>(f));
            default:
                break;
        }
    }

    if constexpr (_float)
    {
        switch (type_num1)
        {
            case TypeIndex::Float32: return callOnBasicType<Float32, _int, _float, _dec>(type_num2, std::forward<F>(f));
            case TypeIndex::Float64: return callOnBasicType<Float64, _int, _float, _dec>(type_num2, std::forward<F>(f));
            default:
                break;
        }
    }

    return false;
}


class DataTypeDate;
class DataTypeDateTime;
class DataTypeString;
class DataTypeFixedString;
class DataTypeUUID;
template <typename T> class DataTypeEnum;
template <typename T> class DataTypeNumber;
template <typename T> class DataTypeDecimal;


template <typename T, typename F>
bool callOnIndexAndDataType(TypeIndex number, F && f)
{
    switch (number)
    {
        case TypeIndex::UInt8:          return f(TypePair<DataTypeNumber<UInt8>, T>());
        case TypeIndex::UInt16:         return f(TypePair<DataTypeNumber<UInt16>, T>());
        case TypeIndex::UInt32:         return f(TypePair<DataTypeNumber<UInt32>, T>());
        case TypeIndex::UInt64:         return f(TypePair<DataTypeNumber<UInt64>, T>());

        case TypeIndex::Int8:           return f(TypePair<DataTypeNumber<Int8>, T>());
        case TypeIndex::Int16:          return f(TypePair<DataTypeNumber<Int16>, T>());
        case TypeIndex::Int32:          return f(TypePair<DataTypeNumber<Int32>, T>());
        case TypeIndex::Int64:          return f(TypePair<DataTypeNumber<Int64>, T>());

        case TypeIndex::Float32:        return f(TypePair<DataTypeNumber<Float32>, T>());
        case TypeIndex::Float64:        return f(TypePair<DataTypeNumber<Float64>, T>());

        case TypeIndex::Decimal32:      return f(TypePair<DataTypeDecimal<Decimal32>, T>());
        case TypeIndex::Decimal64:      return f(TypePair<DataTypeDecimal<Decimal64>, T>());
        case TypeIndex::Decimal128:     return f(TypePair<DataTypeDecimal<Decimal128>, T>());

        case TypeIndex::Date:           return f(TypePair<DataTypeDate, T>());
        case TypeIndex::DateTime:       return f(TypePair<DataTypeDateTime, T>());

        case TypeIndex::String:         return f(TypePair<DataTypeString, T>());
        case TypeIndex::FixedString:    return f(TypePair<DataTypeFixedString, T>());

        case TypeIndex::Enum8:          return f(TypePair<DataTypeEnum<Int8>, T>());
        case TypeIndex::Enum16:         return f(TypePair<DataTypeEnum<Int16>, T>());

        case TypeIndex::UUID:           return f(TypePair<DataTypeUUID, T>());

        default:
            break;
    }

    return false;
}

}
