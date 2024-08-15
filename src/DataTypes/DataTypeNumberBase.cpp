#include <type_traits>
#include <DataTypes/DataTypeNumberBase.h>
#include <Columns/ColumnVector.h>


namespace DB
{

template <typename T>
Field DataTypeNumberBase<T>::getDefault() const
{
    return NearestFieldType<FieldType>();
}
template <typename T>
String DataTypeNumberBase<T>::getSQLCompatibleName() const
{
    if constexpr (std::is_same_v<T, Int8>)
        return "TINYINT";
    else if constexpr (std::is_same_v<T, Int16>)
        return "SMALLINT";
    else if constexpr (std::is_same_v<T, Int32>)
        return "INTEGER";
    else if constexpr (std::is_same_v<T, Int64>)
        return "BIGINT";
    else if constexpr (std::is_same_v<T, UInt8>)
        return "TINYINT UNSIGNED";
    else if constexpr (std::is_same_v<T, UInt16>)
        return "SMALLINT UNSIGNED";
    else if constexpr (std::is_same_v<T, UInt32>)
        return "INTEGER UNSIGNED";
    else if constexpr (std::is_same_v<T, UInt64>)
        return "BIGINT UNSIGNED";
    else if constexpr (std::is_same_v<T, Float32>)
        return "FLOAT";
    else if constexpr (std::is_same_v<T, Float64>)
        return "DOUBLE";
    /// Unsupported types are converted to TEXT
    else
        return "TEXT";
}

template <typename T>
MutableColumnPtr DataTypeNumberBase<T>::createColumn() const
{
    return ColumnVector<T>::create();
}

template <typename T>
bool DataTypeNumberBase<T>::isValueRepresentedByInteger() const
{
    return is_integer<T>;
}

template <typename T>
bool DataTypeNumberBase<T>::isValueRepresentedByUnsignedInteger() const
{
    return is_integer<T> && is_unsigned_v<T>;
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class DataTypeNumberBase<UInt8>;
template class DataTypeNumberBase<UInt16>;
template class DataTypeNumberBase<UInt32>;
template class DataTypeNumberBase<UInt64>;
template class DataTypeNumberBase<UInt128>;
template class DataTypeNumberBase<UInt256>;
template class DataTypeNumberBase<Int8>;
template class DataTypeNumberBase<Int16>;
template class DataTypeNumberBase<Int32>;
template class DataTypeNumberBase<Int64>;
template class DataTypeNumberBase<Int128>;
template class DataTypeNumberBase<Int256>;
template class DataTypeNumberBase<Float32>;
template class DataTypeNumberBase<Float64>;

}
