#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Common/UTF8Helpers.h>
#include <Poco/UTF8Encoding.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int EMPTY_DATA_PASSED;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


template <typename FieldType> struct EnumName;
template <> struct EnumName<Int8> { static constexpr auto value = "Enum8"; };
template <> struct EnumName<Int16> { static constexpr auto value = "Enum16"; };


template <typename Type>
const char * DataTypeEnum<Type>::getFamilyName() const
{
    return EnumName<FieldType>::value;
}


template <typename Type>
std::string DataTypeEnum<Type>::generateName(const Values & values)
{
    WriteBufferFromOwnString out;

    writeString(EnumName<FieldType>::value, out);
    writeChar('(', out);

    auto first = true;
    for (const auto & name_and_value : values)
    {
        if (!first)
            writeString(", ", out);

        first = false;

        writeQuotedString(name_and_value.first, out);
        writeString(" = ", out);
        writeText(name_and_value.second, out);
    }

    writeChar(')', out);

    return out.str();
}

template <typename Type>
void DataTypeEnum<Type>::fillMaps()
{
    for (const auto & name_and_value : values)
    {
        const auto name_to_value_pair = name_to_value_map.insert(
            { StringRef{name_and_value.first}, name_and_value.second });

        if (!name_to_value_pair.second)
            throw Exception{"Duplicate names in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
                    + " and '" + name_to_value_pair.first->first.toString() + "' = " + toString(name_to_value_pair.first->second),
                ErrorCodes::SYNTAX_ERROR};

        const auto value_to_name_pair = value_to_name_map.insert(
            { name_and_value.second, StringRef{name_and_value.first} });

        if (!value_to_name_pair.second)
            throw Exception{"Duplicate values in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
                    + " and '" + value_to_name_pair.first->second.toString() + "' = " + toString(value_to_name_pair.first->first),
                ErrorCodes::SYNTAX_ERROR};
    }
}

template <typename Type>
DataTypeEnum<Type>::DataTypeEnum(const Values & values_) : values{values_}
{
    if (values.empty())
        throw Exception{"DataTypeEnum enumeration cannot be empty", ErrorCodes::EMPTY_DATA_PASSED};

    std::sort(std::begin(values), std::end(values), [] (auto & left, auto & right)
    {
        return left.second < right.second;
    });

    fillMaps();
    type_name = generateName(values);
}

template <typename Type>
void DataTypeEnum<Type>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const FieldType x = get<typename NearestFieldType<FieldType>::Type>(field);
    writeBinary(x, ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    FieldType x;
    readBinary(x, istr);
    field = nearestFieldType(x);
}

template <typename Type>
void DataTypeEnum<Type>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    typename ColumnType::value_type x;
    readBinary(x, istr);
    static_cast<ColumnType &>(column).getData().push_back(x);
}

template <typename Type>
void DataTypeEnum<Type>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    /// NOTE It would be nice to do without creating a temporary object - at least extract std::string out.
    std::string field_name;
    readEscapedString(field_name, istr);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(field_name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    std::string field_name;
    readQuotedStringWithSQLStyle(field_name, istr);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(field_name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr, settings);
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    std::string field_name;
    readJSONString(field_name, istr);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(field_name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    std::string field_name;
    readCSVString(field_name, istr, settings.csv);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(field_name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeBinaryBulk(
    const IColumn & column, WriteBuffer & ostr, const size_t offset, size_t limit) const
{
    const auto & x = typeid_cast<const ColumnType &>(column).getData();
    const auto size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(FieldType) * limit);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeBinaryBulk(
    IColumn & column, ReadBuffer & istr, const size_t limit, const double /*avg_value_size_hint*/) const
{
    auto & x = typeid_cast<ColumnType &>(column).getData();
    const auto initial_size = x.size();
    x.resize(initial_size + limit);
    const auto size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(FieldType) * limit);
    x.resize(initial_size + size / sizeof(FieldType));
}

template <typename Type>
Field DataTypeEnum<Type>::getDefault() const
{
    return typename NearestFieldType<FieldType>::Type(values.front().second);
}

template <typename Type>
void DataTypeEnum<Type>::insertDefaultInto(IColumn & column) const
{
    static_cast<ColumnType &>(column).getData().push_back(values.front().second);
}

template <typename Type>
bool DataTypeEnum<Type>::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && type_name == static_cast<const DataTypeEnum<Type> &>(rhs).type_name;
}


template <typename Type>
bool DataTypeEnum<Type>::textCanContainOnlyValidUTF8() const
{
    for (const auto & elem : values)
    {
        const char * pos = elem.first.data();
        const char * end = pos + elem.first.size();
        while (pos < end)
        {
            size_t length = UTF8::seqLength(*pos);
            if (pos + length > end)
                return false;

            if (Poco::UTF8Encoding::isLegal(reinterpret_cast<const unsigned char *>(pos), length))
                pos += length;
            else
                return false;
        }
    }
    return true;
}

template <typename Type>
static void checkOverflow(Int64 value)
{
    if (!(std::numeric_limits<Type>::min() <= value && value <= std::numeric_limits<Type>::max()))
        throw Exception("DataTypeEnum: Unexpected value " + toString(value), ErrorCodes::BAD_TYPE_OF_FIELD);
}

template <typename Type>
Field DataTypeEnum<Type>::castToName(const Field & value_or_name) const
{
    if (value_or_name.getType() == Field::Types::String)
    {
        getValue(value_or_name.get<String>()); /// Check correctness
        return value_or_name.get<String>();
    }
    else if (value_or_name.getType() == Field::Types::Int64)
    {
        Int64 value = value_or_name.get<Int64>();
        checkOverflow<Type>(value);
        return getNameForValue(static_cast<Type>(value)).toString();
    }
    else
        throw Exception(String("DataTypeEnum: Unsupported type of field ") + value_or_name.getTypeName(), ErrorCodes::BAD_TYPE_OF_FIELD);
}

template <typename Type>
Field DataTypeEnum<Type>::castToValue(const Field & value_or_name) const
{
    if (value_or_name.getType() == Field::Types::String)
    {
        return static_cast<Int64>(getValue(value_or_name.get<String>()));
    }
    else if (value_or_name.getType() == Field::Types::Int64
          || value_or_name.getType() == Field::Types::UInt64)
    {
        Int64 value = value_or_name.get<Int64>();
        checkOverflow<Type>(value);
        getNameForValue(static_cast<Type>(value)); /// Check correctness
        return value;
    }
    else
        throw Exception(String("DataTypeEnum: Unsupported type of field ") + value_or_name.getTypeName(), ErrorCodes::BAD_TYPE_OF_FIELD);
}


/// Explicit instantiations.
template class DataTypeEnum<Int8>;
template class DataTypeEnum<Int16>;


template <typename DataTypeEnum>
static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception("Enum data type cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);

    typename DataTypeEnum::Values values;
    values.reserve(arguments->children.size());

    using FieldType = typename DataTypeEnum::FieldType;

    /// Children must be functions 'equals' with string literal as left argument and numeric literal as right argument.
    for (const ASTPtr & child : arguments->children)
    {
        const ASTFunction * func = typeid_cast<const ASTFunction *>(child.get());
        if (!func
            || func->name != "equals"
            || func->parameters
            || !func->arguments
            || func->arguments->children.size() != 2)
            throw Exception("Elements of Enum data type must be of form: 'name' = number, where name is string literal and number is an integer",
                ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        const ASTLiteral * name_literal = typeid_cast<const ASTLiteral *>(func->arguments->children[0].get());
        const ASTLiteral * value_literal = typeid_cast<const ASTLiteral *>(func->arguments->children[1].get());

        if (!name_literal
            || !value_literal
            || name_literal->value.getType() != Field::Types::String
            || (value_literal->value.getType() != Field::Types::UInt64 && value_literal->value.getType() != Field::Types::Int64))
            throw Exception("Elements of Enum data type must be of form: 'name' = number, where name is string literal and number is an integer",
                ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        const String & field_name = name_literal->value.get<String>();
        const auto value = value_literal->value.get<typename NearestFieldType<FieldType>::Type>();

        if (value > std::numeric_limits<FieldType>::max() || value < std::numeric_limits<FieldType>::min())
            throw Exception{"Value " + toString(value) + " for element '" + field_name + "' exceeds range of " + EnumName<FieldType>::value,
                ErrorCodes::ARGUMENT_OUT_OF_BOUND};

        values.emplace_back(field_name, value);
    }

    return std::make_shared<DataTypeEnum>(values);
}


void registerDataTypeEnum(DataTypeFactory & factory)
{
    factory.registerDataType("Enum8", create<DataTypeEnum<Int8>>);
    factory.registerDataType("Enum16", create<DataTypeEnum<Int16>>);
}

}
