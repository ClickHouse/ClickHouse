#include <DataTypes/DataTypeDecimalBase.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readDecimalText.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>

#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


bool decimalCheckComparisonOverflow(const Context & context) { return context.getSettingsRef().decimal_check_overflow; }
bool decimalCheckArithmeticOverflow(const Context & context) { return context.getSettingsRef().decimal_check_overflow; }

template <typename T>
bool DataTypeDecimalBase<T>::equals(const IDataType & rhs) const
{
    if (auto * ptype = dynamic_cast<const DataTypeDecimalBase<T> *>(&rhs))
        return scale == ptype->getScale();
    return false;
}

template <typename T>
void DataTypeDecimalBase<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    T value = assert_cast<const ColumnType &>(column).getData()[row_num];
    writeText(value, scale, ostr);
}

template <typename T>
bool DataTypeDecimalBase<T>::tryReadText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale)
{
    UInt32 unread_scale = scale;
    bool done = tryReadDecimalText(istr, x, precision, unread_scale);
    x *= getScaleMultiplier(unread_scale);
    return done;
}

template <typename T>
void DataTypeDecimalBase<T>::readText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale, bool csv)
{
    UInt32 unread_scale = scale;
    if (csv)
        readCSVDecimalText(istr, x, precision, unread_scale);
    else
        readDecimalText(istr, x, precision, unread_scale);
    x *= getScaleMultiplier(unread_scale);
}

template <typename T>
void DataTypeDecimalBase<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    readText(x, istr);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename T>
void DataTypeDecimalBase<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    readText(x, istr, true);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename T>
T DataTypeDecimalBase<T>::parseFromString(const String & str) const
{
    ReadBufferFromMemory buf(str.data(), str.size());
    T x;
    UInt32 unread_scale = scale;
    readDecimalText(buf, x, precision, unread_scale, true);
    x *= getScaleMultiplier(unread_scale);
    return x;
}


template <typename T>
void DataTypeDecimalBase<T>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    FieldType x = get<DecimalField<T>>(field);
    writeBinary(x, ostr);
}

template <typename T>
void DataTypeDecimalBase<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const FieldType & x = assert_cast<const ColumnType &>(column).getData()[row_num];
    writeBinary(x, ostr);
}

template <typename T>
void DataTypeDecimalBase<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnType::Container & x = typeid_cast<const ColumnType &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(FieldType) * limit);
}


template <typename T>
void DataTypeDecimalBase<T>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    typename FieldType::NativeType x;
    readBinary(x, istr);
    field = DecimalField(T(x), scale);
}

template <typename T>
void DataTypeDecimalBase<T>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    typename FieldType::NativeType x;
    readBinary(x, istr);
    assert_cast<ColumnType &>(column).getData().push_back(FieldType(x));
}

template <typename T>
void DataTypeDecimalBase<T>::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double) const
{
    typename ColumnType::Container & x = typeid_cast<ColumnType &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(FieldType) * limit);
    x.resize(initial_size + size / sizeof(FieldType));
}


template <typename T>
void DataTypeDecimalBase<T>::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;
    value_index = static_cast<bool>(protobuf.writeDecimal(assert_cast<const ColumnType &>(column).getData()[row_num], scale));
}


template <typename T>
void DataTypeDecimalBase<T>::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    T decimal;
    if (!protobuf.readDecimal(decimal, precision, scale))
        return;

    auto & container = assert_cast<ColumnType &>(column).getData();
    if (allow_add_row)
    {
        container.emplace_back(decimal);
        row_added = true;
    }
    else
        container.back() = decimal;
}


template <typename T>
Field DataTypeDecimalBase<T>::getDefault() const
{
    return DecimalField(T(0), scale);
}

template <typename T>
MutableColumnPtr DataTypeDecimalBase<T>::createColumn() const
{
    return ColumnType::create(0, scale);
}

template <>
Decimal32 DataTypeDecimalBase<Decimal32>::getScaleMultiplier(UInt32 scale_)
{
    return decimalScaleMultiplier<Int32>(scale_);
}

template <>
Decimal64 DataTypeDecimalBase<Decimal64>::getScaleMultiplier(UInt32 scale_)
{
    return decimalScaleMultiplier<Int64>(scale_);
}

template <>
Decimal128 DataTypeDecimalBase<Decimal128>::getScaleMultiplier(UInt32 scale_)
{
    return decimalScaleMultiplier<Int128>(scale_);
}


/// Explicit template instantiations.
template class DataTypeDecimalBase<Decimal32>;
template class DataTypeDecimalBase<Decimal64>;
template class DataTypeDecimalBase<Decimal128>;

}
