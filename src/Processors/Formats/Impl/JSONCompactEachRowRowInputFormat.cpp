#include <Processors/Formats/Impl/JSONCompactEachRowRowInputFormat.h>

#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Formats/FormatFactory.h>
#include <Formats/verbosePrintString.h>
#include <Formats/JSONEachRowUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


JSONCompactEachRowRowInputFormat::JSONCompactEachRowRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    Params params_,
    bool with_names_,
    bool with_types_,
    bool yield_strings_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(header_, in_, std::move(params_), with_names_, with_types_, format_settings_)
    , yield_strings(yield_strings_)
{
}

void JSONCompactEachRowRowInputFormat::skipRowStartDelimiter()
{
    skipWhitespaceIfAny(*in);
    assertChar('[', *in);
}

void JSONCompactEachRowRowInputFormat::skipFieldDelimiter()
{
    skipWhitespaceIfAny(*in);
    assertChar(',', *in);
}

void JSONCompactEachRowRowInputFormat::skipRowEndDelimiter()
{
    skipWhitespaceIfAny(*in);
    assertChar(']', *in);

    skipWhitespaceIfAny(*in);
    if (!in->eof() && (*in->position() == ',' || *in->position() == ';'))
        ++in->position();

    skipWhitespaceIfAny(*in);
}

String JSONCompactEachRowRowInputFormat::readFieldIntoString()
{
    skipWhitespaceIfAny(*in);
    String field;
    readJSONString(field, *in);
    return field;
}

void JSONCompactEachRowRowInputFormat::skipField(size_t file_column)
{
    skipWhitespaceIfAny(*in);
    skipJSONField(*in, column_mapping->names_of_columns[file_column]);
}

void JSONCompactEachRowRowInputFormat::skipHeaderRow()
{
    skipRowStartDelimiter();
    size_t i = 0;
    do
    {
        if (i >= column_mapping->names_of_columns.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "The number of columns in a row differs from the number of column names");
        skipField(i++);
        skipWhitespaceIfAny(*in);
    }
    while (checkChar(',', *in));

    skipRowEndDelimiter();
}

std::vector<String> JSONCompactEachRowRowInputFormat::readHeaderRow()
{
    skipRowStartDelimiter();
    std::vector<String> fields;
    do
    {
        fields.push_back(readFieldIntoString());
        skipWhitespaceIfAny(*in);
    }
    while (checkChar(',', *in));

    skipRowEndDelimiter();
    return fields;
}

bool JSONCompactEachRowRowInputFormat::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool /*is_last_file_column*/, const String & column_name)
{
    skipWhitespaceIfAny(*in);
    return readFieldImpl(*in, column, type, serialization, column_name, format_settings, yield_strings);
}

void JSONCompactEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

bool JSONCompactEachRowRowInputFormat::parseRowStartWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespaceIfAny(*in);
    if (!checkChar('[', *in))
    {
        out << "ERROR: There is no '[' before the row.\n";
        return false;
    }

    return true;
}

bool JSONCompactEachRowRowInputFormat::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    try
    {
        skipWhitespaceIfAny(*in);
        assertChar(',', *in);
    }
    catch (const DB::Exception &)
    {
        if (*in->position() == ']')
        {
            out << "ERROR: Closing parenthesis (']') found where comma is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has unescaped quotes in values.\n";
        }
        else
        {
            out << "ERROR: There is no comma. ";
            verbosePrintString(in->position(), in->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool JSONCompactEachRowRowInputFormat::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespaceIfAny(*in);

    if (in->eof())
    {
        out << "ERROR: Unexpected end of file. ']' expected at the end of row.";
        return false;
    }

    if (!checkChar(']', *in))
    {
        out << "ERROR: There is no closing parenthesis (']') at the end of the row. ";
        verbosePrintString(in->position(), in->position() + 1, out);
        out << " found instead.\n";
        return false;
    }

    skipWhitespaceIfAny(*in);

    if (in->eof())
        return true;

    if ((*in->position() == ',' || *in->position() == ';'))
        ++in->position();

    skipWhitespaceIfAny(*in);
    return true;
}

void registerInputFormatJSONCompactEachRow(FormatFactory & factory)
{
    for (bool yield_strings : {true, false})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerInputFormat(format_name, [with_names, with_types, yield_strings](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
            {
                return std::make_shared<JSONCompactEachRowRowInputFormat>(sample, buf, std::move(params), with_names, with_types, yield_strings, settings);
            });
        };

        registerWithNamesAndTypes(yield_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", register_func);
    }
}

void registerFileSegmentationEngineJSONCompactEachRow(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        /// In case when we have names and/or types in the first two/one rows,
        /// we need to read at least one more row of actual data. So, set
        /// the minimum of rows for segmentation engine according to
        /// parameters with_names and with_types.
        size_t min_rows = 1 + int(with_names) + int(with_types);
        factory.registerFileSegmentationEngine(format_name, [min_rows](ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
        {
            return fileSegmentationEngineJSONCompactEachRow(in, memory, min_chunk_size, min_rows);
        });
    };

    registerWithNamesAndTypes("JSONCompactEachRow", register_func);
    registerWithNamesAndTypes("JSONCompactStringsEachRow", register_func);
}

}
