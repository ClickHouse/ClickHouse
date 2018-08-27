#include <type_traits>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readFloatText.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


bool decimalCheckComparisonOverflow(const Context & context) { return context.getSettingsRef().decimal_check_comparison_overflow; }
bool decimalCheckArithmeticOverflow(const Context & context) { return context.getSettingsRef().decimal_check_arithmetic_overflow; }


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
    T value = static_cast<const ColumnType &>(column).getData()[row_num];
    writeText(value, scale, ostr);
}

template <typename T>
void DataTypeDecimal<T>::readText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale)
{
    UInt32 unread_scale = scale;
    readDecimalText(istr, x, precision, unread_scale);
    x *= getScaleMultiplier(unread_scale);
}

template <typename T>
void DataTypeDecimal<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    readText(x, istr);
    static_cast<ColumnType &>(column).getData().push_back(x);
}


template <typename T>
T DataTypeDecimal<T>::parseFromString(const String & str) const
{
    ReadBufferFromMemory buf(str.data(), str.size());
    T x;
    UInt32 unread_scale = scale;
    readDecimalText(buf, x, precision, unread_scale, true);
    x *= getScaleMultiplier(unread_scale);
    return x;
}


template <typename T>
void DataTypeDecimal<T>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    FieldType x = get<DecimalField<T>>(field);
    writeBinary(x, ostr);
}

template <typename T>
void DataTypeDecimal<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const FieldType & x = static_cast<const ColumnType &>(column).getData()[row_num];
    writeBinary(x, ostr);
}

template <typename T>
void DataTypeDecimal<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnType::Container & x = typeid_cast<const ColumnType &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(FieldType) * limit);
}


template <typename T>
void DataTypeDecimal<T>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    typename FieldType::NativeType x;
    readBinary(x, istr);
    field = DecimalField(T(x), scale);
}

template <typename T>
void DataTypeDecimal<T>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    typename FieldType::NativeType x;
    readBinary(x, istr);
    static_cast<ColumnType &>(column).getData().push_back(FieldType(x));
}

template <typename T>
void DataTypeDecimal<T>::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double ) const
{
    typename ColumnType::Container & x = typeid_cast<ColumnType &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(FieldType) * limit);
    x.resize(initial_size + size / sizeof(FieldType));
}


template <typename T>
Field DataTypeDecimal<T>::getDefault() const
{
    return DecimalField(T(0), scale);
}


template <typename T>
MutableColumnPtr DataTypeDecimal<T>::createColumn() const
{
    return ColumnType::create(0, scale);
}


//

DataTypePtr createDecimal(UInt64 precision_value, UInt64 scale_value)
{
    if (precision_value < minDecimalPrecision() || precision_value > maxDecimalPrecision<Decimal128>())
        throw Exception("Wrong precision", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (static_cast<UInt64>(scale_value) > precision_value)
        throw Exception("Negative scales and scales larger than presicion are not supported", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (precision_value <= maxDecimalPrecision<Decimal32>())
        return std::make_shared<DataTypeDecimal<Decimal32>>(precision_value, scale_value);
    else if (precision_value <= maxDecimalPrecision<Decimal64>())
        return std::make_shared<DataTypeDecimal<Decimal64>>(precision_value, scale_value);
    return std::make_shared<DataTypeDecimal<Decimal128>>(precision_value, scale_value);
}

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
    UInt64 scale_value = scale->value.get<UInt64>();

    return createDecimal(precision_value, scale_value);
}

template <typename T>
static DataTypePtr createExect(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * scale_arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());

    if (!scale_arg || !(scale_arg->value.getType() == Field::Types::Int64 || scale_arg->value.getType() == Field::Types::UInt64))
        throw Exception("Decimal data type family must have a two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision = maxDecimalPrecision<T>();
    UInt64 scale = scale_arg->value.get<UInt64>();

    return createDecimal(precision, scale);
}

void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("Decimal32", createExect<Decimal32>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("Decimal64", createExect<Decimal64>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("Decimal128", createExect<Decimal128>, DataTypeFactory::CaseInsensitive);

    factory.registerDataType("Decimal", create, DataTypeFactory::CaseInsensitive);
    factory.registerAlias("DEC", "Decimal", DataTypeFactory::CaseInsensitive);
}


template <>
Decimal32 DataTypeDecimal<Decimal32>::getScaleMultiplier(UInt32 scale_)
{
    return decimalScaleMultiplier<Int32>(scale_);
}

template <>
Decimal64 DataTypeDecimal<Decimal64>::getScaleMultiplier(UInt32 scale_)
{
    return decimalScaleMultiplier<Int64>(scale_);
}

template <>
Decimal128 DataTypeDecimal<Decimal128>::getScaleMultiplier(UInt32 scale_)
{
    return decimalScaleMultiplier<Int128>(scale_);
}


/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;

}
