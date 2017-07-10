#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypeEnum.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int EMPTY_DATA_PASSED;
}


template <typename FieldType> struct EnumName;
template <> struct EnumName<Int8> { static constexpr auto value = "Enum8"; };
template <> struct EnumName<Int16> { static constexpr auto value = "Enum16"; };


template <typename Type>
std::string DataTypeEnum<Type>::generateName(const Values & values)
{
    std::string name;

    {
        WriteBufferFromString out{name};

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
    }

    return name;
}

template <typename Type>
void DataTypeEnum<Type>::fillMaps()
{
    for (const auto & name_and_value : values)
    {
        const auto name_to_value_pair = name_to_value_map.insert(
            { StringRef{name_and_value.first}, name_and_value.second });
        if (!name_to_value_pair.second)
            throw Exception{
                "Duplicate names in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
                    + " and '" + name_to_value_pair.first->first.toString() + "' = " + toString(name_to_value_pair.first->second),
                ErrorCodes::SYNTAX_ERROR};

        const auto value_to_name_pair = value_to_name_map.insert(
            { name_and_value.second, StringRef{name_and_value.first} });
        if (!value_to_name_pair.second)
            throw Exception{
                "Duplicate values in enum: '" + name_and_value.first + "' = " + toString(name_and_value.second)
                    + " and '" + value_to_name_pair.first->second.toString() + "' = " + toString(value_to_name_pair.first->first),
                ErrorCodes::SYNTAX_ERROR};
    }
}

template <typename Type>
DataTypeEnum<Type>::DataTypeEnum(const Values & values_) : values{values_}
{
    if (values.empty())
        throw Exception{
            "DataTypeEnum enumeration cannot be empty",
            ErrorCodes::EMPTY_DATA_PASSED
        };

    fillMaps();

    std::sort(std::begin(values), std::end(values), [] (auto & left, auto & right) {
        return left.second < right.second;
    });

    name = generateName(values);
}

template <typename Type>
DataTypeEnum<Type>::DataTypeEnum(const DataTypeEnum & other) : values{other.values}, name{other.name}
{
    fillMaps();
}

template <typename Type>
DataTypePtr DataTypeEnum<Type>::clone() const
{
    return std::make_shared<DataTypeEnum>(*this);
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
void DataTypeEnum<Type>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeEscapedString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    /// NOTE It would be nice to do without creating a temporary object - at least extract std::string out.
    std::string name;
    readEscapedString(name, istr);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeQuotedString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    std::string name;
    readQuotedStringWithSQLStyle(name, istr);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const
{
    writeJSONString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeXMLString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    std::string name;
    readJSONString(name, istr);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(name)));
}

template <typename Type>
void DataTypeEnum<Type>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeCSVString(getNameForValue(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void DataTypeEnum<Type>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
    std::string name;
    readCSVString(name, istr, delimiter);
    static_cast<ColumnType &>(column).getData().push_back(getValue(StringRef(name)));
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
    IColumn & column, ReadBuffer & istr, const size_t limit, const double avg_value_size_hint) const
{
    auto & x = typeid_cast<ColumnType &>(column).getData();
    const auto initial_size = x.size();
    x.resize(initial_size + limit);
    const auto size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(FieldType) * limit);
    x.resize(initial_size + size / sizeof(FieldType));
}

template <typename Type>
ColumnPtr DataTypeEnum<Type>::createConstColumn(const size_t size, const Field & field) const
{
    return std::make_shared<ConstColumnType>(size, get<typename NearestFieldType<FieldType>::Type>(field));
}

template <typename Type>
Field DataTypeEnum<Type>::getDefault() const
{
    return typename NearestFieldType<FieldType>::Type(values.front().second);
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

}
