#include <DataTypes/DataTypesDecimal.h>

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
    extern const int DECIMAL_OVERFLOW;
}


template <typename T>
std::string DataTypeDecimal<T>::doGetName() const
{
    return fmt::format("Decimal({}, {})", this->precision, this->scale);
}


template <typename T>
bool DataTypeDecimal<T>::equals(const IDataType & rhs) const
{
    if (auto * ptype = typeid_cast<const DataTypeDecimal<T> *>(&rhs))
        return this->scale == ptype->getScale();
    return false;
}

template <typename T>
DataTypePtr DataTypeDecimal<T>::promoteNumericType() const
{
    using PromotedType = DataTypeDecimal<Decimal128>;
    return std::make_shared<PromotedType>(PromotedType::maxPrecision(), this->scale);
}

template <typename T>
void DataTypeDecimal<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    T value = assert_cast<const ColumnType &>(column).getData()[row_num];
    writeText(value, this->scale, ostr);
}

template <typename T>
bool DataTypeDecimal<T>::tryReadText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale)
{
    UInt32 unread_scale = scale;
    if (!tryReadDecimalText(istr, x, precision, unread_scale))
        return false;

    if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        return false;

    return true;
}

template <typename T>
void DataTypeDecimal<T>::readText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale, bool csv)
{
    UInt32 unread_scale = scale;
    if (csv)
        readCSVDecimalText(istr, x, precision, unread_scale);
    else
        readDecimalText(istr, x, precision, unread_scale);

    if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
}

template <typename T>
void DataTypeDecimal<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    readText(x, istr);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename T>
void DataTypeDecimal<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    T x;
    readText(x, istr, true);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename T>
T DataTypeDecimal<T>::parseFromString(const String & str) const
{
    ReadBufferFromMemory buf(str.data(), str.size());
    T x;
    UInt32 unread_scale = this->scale;
    readDecimalText(buf, x, this->precision, unread_scale, true);

    if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);

    return x;
}

template <typename T>
void DataTypeDecimal<T>::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;
    value_index = static_cast<bool>(protobuf.writeDecimal(assert_cast<const ColumnType &>(column).getData()[row_num], this->scale));
}


template <typename T>
void DataTypeDecimal<T>::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    T decimal;
    if (!protobuf.readDecimal(decimal, this->precision, this->scale))
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


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("Decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * precision = arguments->children[0]->as<ASTLiteral>();
    const auto * scale = arguments->children[1]->as<ASTLiteral>();

    if (!precision || precision->value.getType() != Field::Types::UInt64 ||
        !scale || !(scale->value.getType() == Field::Types::Int64 || scale->value.getType() == Field::Types::UInt64))
        throw Exception("Decimal data type family must have two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision_value = precision->value.get<UInt64>();
    UInt64 scale_value = scale->value.get<UInt64>();

    return createDecimal<DataTypeDecimal>(precision_value, scale_value);
}

template <typename T>
static DataTypePtr createExact(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * scale_arg = arguments->children[0]->as<ASTLiteral>();

    if (!scale_arg || !(scale_arg->value.getType() == Field::Types::Int64 || scale_arg->value.getType() == Field::Types::UInt64))
        throw Exception("Decimal data type family must have a two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision = DecimalUtils::maxPrecision<T>();
    UInt64 scale = scale_arg->value.get<UInt64>();

    return createDecimal<DataTypeDecimal>(precision, scale);
}

void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("Decimal32", createExact<Decimal32>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("Decimal64", createExact<Decimal64>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("Decimal128", createExact<Decimal128>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("Decimal256", createExact<Decimal256>, DataTypeFactory::CaseInsensitive);

    factory.registerDataType("Decimal", create, DataTypeFactory::CaseInsensitive);
    factory.registerAlias("DEC", "Decimal", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NUMERIC", "Decimal", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("FIXED", "Decimal", DataTypeFactory::CaseInsensitive);
}

/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;
template class DataTypeDecimal<Decimal256>;

}
