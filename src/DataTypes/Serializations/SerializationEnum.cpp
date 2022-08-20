#include <DataTypes/Serializations/SerializationEnum.h>

#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <Common/assert_cast.h>

namespace DB
{

template <typename Type>
void SerializationEnum<Type>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(this->getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void SerializationEnum<Type>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(this->getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]).toView(), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.tsv.input_format_enum_as_number)
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
    else
    {
        /// NOTE It would be nice to do without creating a temporary object - at least extract std::string out.
        std::string field_name;
        readEscapedString(field_name, istr);
        assert_cast<ColumnType &>(column).getData().push_back(this->getValue(StringRef(field_name), true));
    }
}

template <typename Type>
void SerializationEnum<Type>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(this->getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    std::string field_name;
    readQuotedStringWithSQLStyle(field_name, istr);
    assert_cast<ColumnType &>(column).getData().push_back(this->getValue(StringRef(field_name)));
}

template <typename Type>
void SerializationEnum<Type>::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.tsv.input_format_enum_as_number)
    {
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
        if (!istr.eof())
            ISerialization::throwUnexpectedDataAfterParsedValue(column, istr, settings, "Enum");
    }
    else
    {
        std::string field_name;
        readStringUntilEOF(field_name, istr);
        assert_cast<ColumnType &>(column).getData().push_back(this->getValue(StringRef(field_name), true));
    }
}

template <typename Type>
void SerializationEnum<Type>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(this->getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]).toView(), ostr, settings);
}

template <typename Type>
void SerializationEnum<Type>::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(this->getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]).toView(), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    if (!istr.eof() && *istr.position() != '"')
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
    else
    {
        std::string field_name;
        readJSONString(field_name, istr);
        assert_cast<ColumnType &>(column).getData().push_back(this->getValue(StringRef(field_name)));
    }
}

template <typename Type>
void SerializationEnum<Type>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString(this->getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.csv.input_format_enum_as_number)
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
    else
    {
        std::string field_name;
        readCSVString(field_name, istr, settings.csv);
        assert_cast<ColumnType &>(column).getData().push_back(this->getValue(StringRef(field_name), true));
    }
}

template class SerializationEnum<Int8>;
template class SerializationEnum<Int16>;

}
