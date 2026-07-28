#include <DataTypes/Serializations/SerializationEnum.h>

#include <Columns/ColumnVector.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <Common/assert_cast.h>

namespace DB
{

template <typename Type>
void SerializationEnum<Type>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(ref_enum_values.getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void SerializationEnum<Type>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(ref_enum_values.getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]).toView(), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.tsv.enum_as_number)
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
    else
    {
        /// NOTE It would be nice to do without creating a temporary object - at least extract std::string out.
        std::string field_name;
        settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(field_name, istr) : readEscapedString(field_name, istr);
        assert_cast<ColumnType &>(column).getData().push_back(ref_enum_values.getValue(StringRef(field_name)));
    }
}

template <typename Type>
bool SerializationEnum<Type>::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    FieldType x;
    if (settings.tsv.enum_as_number)
    {
        if (!tryReadValue(istr, x))
            return false;
    }
    else
    {
        std::string field_name;
        readEscapedString(field_name, istr);
        if (!ref_enum_values.tryGetValue(x, StringRef(field_name)))
            return false;
    }

    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

template <typename Type>
void SerializationEnum<Type>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(ref_enum_values.getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    std::string field_name;
    readQuotedStringWithSQLStyle(field_name, istr);
    assert_cast<ColumnType &>(column).getData().push_back(ref_enum_values.getValue(StringRef(field_name)));
}

template <typename Type>
bool SerializationEnum<Type>::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    std::string field_name;
    if (!tryReadQuotedStringWithSQLStyle(field_name, istr))
        return false;

    FieldType x;
    if (!ref_enum_values.tryGetValue(x, StringRef(field_name)))
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

template <typename Type>
void SerializationEnum<Type>::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.tsv.enum_as_number)
    {
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
        if (!istr.eof())
            ISerialization::throwUnexpectedDataAfterParsedValue(column, istr, settings, "Enum");
    }
    else
    {
        std::string field_name;
        readStringUntilEOF(field_name, istr);
        assert_cast<ColumnType &>(column).getData().push_back(ref_enum_values.getValue(StringRef(field_name)));
    }
}

template <typename Type>
bool SerializationEnum<Type>::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    FieldType x;
    if (settings.tsv.enum_as_number)
    {
        if (!tryReadValue(istr, x) || !istr.eof())
            return false;
    }
    else
    {
        std::string field_name;
        readStringUntilEOF(field_name, istr);
        if (!ref_enum_values.tryGetValue(x, StringRef(field_name)))
            return false;
    }

    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

template <typename Type>
void SerializationEnum<Type>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(ref_enum_values.getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]).toView(), ostr, settings);
}

template <typename Type>
void SerializationEnum<Type>::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(ref_enum_values.getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]).toView(), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (!istr.eof() && *istr.position() != '"')
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
    else
    {
        std::string field_name;
        readJSONString(field_name, istr, settings.json);
        assert_cast<ColumnType &>(column).getData().push_back(ref_enum_values.getValue(StringRef(field_name)));
    }
}

template <typename Type>
bool SerializationEnum<Type>::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    FieldType x;
    if (!istr.eof() && *istr.position() != '"')
    {
        if (!tryReadValue(istr, x))
            return false;
    }
    else
    {
        std::string field_name;
        readJSONString(field_name, istr, settings.json);
        if (!ref_enum_values.tryGetValue(x, StringRef(field_name)))
            return false;
    }

    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

template <typename Type>
void SerializationEnum<Type>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString(ref_enum_values.getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]), ostr);
}

template <typename Type>
void SerializationEnum<Type>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.csv.enum_as_number)
        assert_cast<ColumnType &>(column).getData().push_back(readValue(istr));
    else
    {
        std::string field_name;
        readCSVString(field_name, istr, settings.csv);
        assert_cast<ColumnType &>(column).getData().push_back(ref_enum_values.getValue(StringRef(field_name)));
    }
}

template <typename Type>
bool SerializationEnum<Type>::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    FieldType x;

    if (settings.csv.enum_as_number)
    {
        if (!tryReadValue(istr, x))
            return false;
    }
    else
    {
        std::string field_name;
        readCSVString(field_name, istr, settings.csv);
        if (!ref_enum_values.tryGetValue(x, StringRef(field_name)))
            return false;
    }

    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

template <typename Type>
void SerializationEnum<Type>::serializeTextMarkdown(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.markdown.escape_special_characters)
        writeMarkdownEscapedString(ref_enum_values.getNameForValue(assert_cast<const ColumnType &>(column).getData()[row_num]).toView(), ostr);
    else
        serializeTextEscaped(column, row_num, ostr, settings);
}

template class SerializationEnum<Int8>;
template class SerializationEnum<Int16>;

}
