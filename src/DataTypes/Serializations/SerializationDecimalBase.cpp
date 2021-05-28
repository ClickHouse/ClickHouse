#include <DataTypes/Serializations/SerializationDecimalBase.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

template <typename T>
void SerializationDecimalBase<T>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    FieldType x = get<DecimalField<T>>(field);
    writeBinary(x, ostr);
}

template <typename T>
void SerializationDecimalBase<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const FieldType & x = assert_cast<const ColumnType &>(column).getElement(row_num);
    writeBinary(x, ostr);
}

template <typename T>
void SerializationDecimalBase<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnType::Container & x = typeid_cast<const ColumnType &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(FieldType) * limit);
}

template <typename T>
void SerializationDecimalBase<T>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    typename FieldType::NativeType x;
    readBinary(x, istr);
    field = DecimalField(T(x), this->scale);
}

template <typename T>
void SerializationDecimalBase<T>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    typename FieldType::NativeType x;
    readBinary(x, istr);
    assert_cast<ColumnType &>(column).getData().push_back(FieldType(x));
}

template <typename T>
void SerializationDecimalBase<T>::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double) const
{
    typename ColumnType::Container & x = typeid_cast<ColumnType &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(FieldType) * limit);
    x.resize(initial_size + size / sizeof(FieldType));
}

template class SerializationDecimalBase<Decimal32>;
template class SerializationDecimalBase<Decimal64>;
template class SerializationDecimalBase<Decimal128>;
template class SerializationDecimalBase<Decimal256>;
template class SerializationDecimalBase<DateTime64>;

}
