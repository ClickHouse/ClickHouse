#include <type_traits>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFactory.h>
//#include <Columns/ColumnConst.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readFloatText.h>
//#include <Formats/FormatSettings.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


//

template <typename T>
std::string DataTypeDecimal<T>::getName() const
{
    std::stringstream ss;
    ss << "Decimal(" << precision << ", " << scale << ")";
    return ss.str();
}

template <typename T>
bool DataTypeDecimal<T>::equals(const IDataType & rhs) const
{
    if (auto * ptype = typeid_cast<const DataTypeDecimal<T> *>(&rhs))
        return scale == ptype->getScale();
    return false;
}

template <typename T>
void DataTypeDecimal<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const T & value = static_cast<const ColumnType &>(column).getData()[row_num];

    writeIntText(wholePart(value), ostr);
    writeChar('.', ostr);
    if (scale)
        writeIntText(fractionalPart(value), ostr);
}


template <typename T>
void DataTypeDecimal<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    UInt32 unread_scale = scale;
    readDecimalText(istr, x, precision, unread_scale);
    x *= getScaleMultiplier(unread_scale);
    static_cast<ColumnType &>(column).getData().push_back(x);
}


template <typename T>
void DataTypeDecimal<T>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    /// ColumnType::value_type is a narrower type. For example, UInt8, when the Field type is UInt64
    typename ColumnType::value_type x = get<FieldType>(field);
    writeBinary(x, ostr);
}

template <typename T>
void DataTypeDecimal<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
}

template <typename T>
void DataTypeDecimal<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnType::Container & x = typeid_cast<const ColumnType &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(typename ColumnType::value_type) * limit);
}


template <typename T>
void DataTypeDecimal<T>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    typename ColumnType::value_type x;
    readBinary(x, istr);
    field = FieldType(x);
}

template <typename T>
void DataTypeDecimal<T>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    typename ColumnType::value_type x;
    readBinary(x, istr);
    static_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename T>
void DataTypeDecimal<T>::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double ) const
{
    typename ColumnType::Container & x = typeid_cast<ColumnType &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(typename ColumnType::value_type) * limit);
    x.resize(initial_size + size / sizeof(typename ColumnType::value_type));
}


template <typename T>
Field DataTypeDecimal<T>::getDefault() const
{
    return FieldType();
}


template <typename T>
MutableColumnPtr DataTypeDecimal<T>::createColumn() const
{
    return ColumnType::create();
}


//

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("Decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * precision = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    const ASTLiteral * scale = typeid_cast<const ASTLiteral *>(arguments->children[1].get());

    if (!precision || precision->value.getType() != Field::Types::UInt64 ||
        !scale || !(scale->value.getType() == Field::Types::Int64 || scale->value.getType() == Field::Types::UInt64))
        throw Exception("Decimal data type family must have a two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision_value = precision->value.get<UInt64>();
    Int64 scale_value = scale->value.get<Int64>();

    if (precision_value < minDecimalPrecision() || precision_value > maxDecimalPrecision<Int128>())
        throw Exception("Wrong precision", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (scale_value < 0 || static_cast<UInt64>(scale_value) > precision_value)
        throw Exception("Negative scales and scales larger than presicion are not supported", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (precision_value <= maxDecimalPrecision<Int32>())
        return std::make_shared<DataTypeDecimal<Int32>>(precision_value, scale_value);
    else if (precision_value <= maxDecimalPrecision<Int64>())
        return std::make_shared<DataTypeDecimal<Int64>>(precision_value, scale_value);
    return std::make_shared<DataTypeDecimal<Int128>>(precision_value, scale_value);
}


void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("Decimal", create, DataTypeFactory::CaseInsensitive);
}


template <>
Int32 DataTypeDecimal<Int32>::getScaleMultiplier(UInt32 scale_)
{
    static const Int32 values[] = {
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000
    };
    return values[scale_];
}

template <>
Int64 DataTypeDecimal<Int64>::getScaleMultiplier(UInt32 scale_)
{
    static const Int64 values[] = {
        1ll,
        10ll,
        100ll,
        1000ll,
        10000ll,
        100000ll,
        1000000ll,
        10000000ll,
        100000000ll,
        1000000000ll,
        10000000000ll,
        100000000000ll,
        1000000000000ll,
        10000000000000ll,
        100000000000000ll,
        1000000000000000ll,
        10000000000000000ll,
        100000000000000000ll,
        1000000000000000000ll
    };
    return values[scale_];
}

template <>
Int128 DataTypeDecimal<Int128>::getScaleMultiplier(UInt32 scale_)
{
    static const Int128 values[] = {
        static_cast<Int128>(1ll),
        static_cast<Int128>(10ll),
        static_cast<Int128>(100ll),
        static_cast<Int128>(1000ll),
        static_cast<Int128>(10000ll),
        static_cast<Int128>(100000ll),
        static_cast<Int128>(1000000ll),
        static_cast<Int128>(10000000ll),
        static_cast<Int128>(100000000ll),
        static_cast<Int128>(1000000000ll),
        static_cast<Int128>(10000000000ll),
        static_cast<Int128>(100000000000ll),
        static_cast<Int128>(1000000000000ll),
        static_cast<Int128>(10000000000000ll),
        static_cast<Int128>(100000000000000ll),
        static_cast<Int128>(1000000000000000ll),
        static_cast<Int128>(10000000000000000ll),
        static_cast<Int128>(100000000000000000ll),
        static_cast<Int128>(1000000000000000000ll),
        static_cast<Int128>(1000000000000000000ll) * 10ll,
        static_cast<Int128>(1000000000000000000ll) * 100ll,
        static_cast<Int128>(1000000000000000000ll) * 1000ll,
        static_cast<Int128>(1000000000000000000ll) * 10000ll,
        static_cast<Int128>(1000000000000000000ll) * 100000ll,
        static_cast<Int128>(1000000000000000000ll) * 1000000ll,
        static_cast<Int128>(1000000000000000000ll) * 10000000ll,
        static_cast<Int128>(1000000000000000000ll) * 100000000ll,
        static_cast<Int128>(1000000000000000000ll) * 1000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 10000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 100000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 1000000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 10000000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 100000000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 1000000000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 10000000000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 100000000000000000ll,
        static_cast<Int128>(1000000000000000000ll) * 100000000000000000ll * 10ll,
        static_cast<Int128>(1000000000000000000ll) * 100000000000000000ll * 100ll,
        static_cast<Int128>(1000000000000000000ll) * 100000000000000000ll * 1000ll
    };
    return values[scale_];
}


/// Explicit template instantiations.
template class DataTypeDecimal<Int32>;
template class DataTypeDecimal<Int64>;
template class DataTypeDecimal<Int128>;

}
