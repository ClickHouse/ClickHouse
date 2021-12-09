#include <DataTypes/Serializations/SerializationCustomSimpleText.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace
{

using namespace DB;

String serializeToString(const SerializationCustomSimpleText & domain, const IColumn & column, size_t row_num, const FormatSettings & settings)
{
    WriteBufferFromOwnString buffer;
    domain.serializeText(column, row_num, buffer, settings);

    return buffer.str();
}

void deserializeFromString(const SerializationCustomSimpleText & domain, IColumn & column, const String & s, const FormatSettings & settings)
{
    ReadBufferFromString istr(s);
    domain.deserializeText(column, istr, settings, true);
}

}

namespace DB
{

SerializationCustomSimpleText::SerializationCustomSimpleText(const SerializationPtr & nested_)
    : SerializationWrapper(nested_)
{
}

void SerializationCustomSimpleText::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readStringUntilEOF(str, istr);
    deserializeFromString(*this, column, str, settings);
}

void SerializationCustomSimpleText::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeEscapedString(serializeToString(*this, column, row_num, settings), ostr);
}

void SerializationCustomSimpleText::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readEscapedString(str, istr);
    deserializeFromString(*this, column, str, settings);
}

void SerializationCustomSimpleText::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeQuotedString(serializeToString(*this, column, row_num, settings), ostr);
}

void SerializationCustomSimpleText::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readQuotedString(str, istr);
    deserializeFromString(*this, column, str, settings);
}

void SerializationCustomSimpleText::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCSVString(serializeToString(*this, column, row_num, settings), ostr);
}

void SerializationCustomSimpleText::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readCSVString(str, istr, settings.csv);
    deserializeFromString(*this, column, str, settings);
}

void SerializationCustomSimpleText::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(serializeToString(*this, column, row_num, settings), ostr, settings);
}

void SerializationCustomSimpleText::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readJSONString(str, istr);
    deserializeFromString(*this, column, str, settings);
}

void SerializationCustomSimpleText::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeXMLStringForTextElement(serializeToString(*this, column, row_num, settings), ostr);
}

}
