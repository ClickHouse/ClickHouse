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

template <typename T>
const std::map<std::string, std::string> DataTypeNumberBase<T>::mysqlTypeMap = {
    {"UInt8", "tinyint unsigned"},
    {"UInt16", "smallint unsigned"},
    {"UInt32", "mediumint unsigned"},
    {"UInt64", "bigint unsigned"},
    {"UInt128", "bigint unsigned"},
    {"UInt256", "bigint unsigned"},
    {"Int8", "tinyint"},
    {"Int16", "smallint"},
    {"Int32", "int"},
    {"Int64", "bigint"},
    {"Int128", "bigint"},
    {"Int256", "bigint"},
    {"Float32", "float"},
    {"Float64", "double"},
};

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
