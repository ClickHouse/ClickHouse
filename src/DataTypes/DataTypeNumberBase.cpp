#include <type_traits>
#include <DataTypes/DataTypeNumberBase.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/NaNUtils.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Formats/FormatSettings.h>


namespace DB
{

template <Arithmetic T>
Field DataTypeNumberBase<T>::getDefault() const
{
    return NearestFieldType<FieldType>();
}

template <Arithmetic T>
MutableColumnPtr DataTypeNumberBase<T>::createColumn() const
{
    return ColumnVector<T>::create();
}

template <Arithmetic T>
bool DataTypeNumberBase<T>::isValueRepresentedByInteger() const
{
    return Integral<T>;
}

template <Arithmetic T>
bool DataTypeNumberBase<T>::isValueRepresentedByUnsignedInteger() const
{
    return Integral<T> && Unsigned<T>;
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
