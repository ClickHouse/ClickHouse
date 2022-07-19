#include <Processors/Formats/Impl/JSONCompactEachRowRowInputFormat.h>

#include <IO/ReadHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/Operators.h>
#include <Formats/FormatFactory.h>
#include <Formats/verbosePrintString.h>
#include <Formats/JSONUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

JSONCompactEachRowRowInputFormat::JSONCompactEachRowRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    Params params_,
    bool with_names_,
    bool with_types_,
    bool yield_strings_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<JSONCompactEachRowFormatReader>(in_, yield_strings_, format_settings_))
{
}

void JSONCompactEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

JSONCompactEachRowFormatReader::JSONCompactEachRowFormatReader(ReadBuffer & in_, bool yield_strings_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesReader(in_, format_settings_), yield_strings(yield_strings_)
{
}

void JSONCompactEachRowFormatReader::skipRowStartDelimiter()
{
    skipWhitespaceIfAny(*in);
    assertChar('[', *in);
}

void JSONCompactEachRowFormatReader::skipFieldDelimiter()
{
    skipWhitespaceIfAny(*in);
    assertChar(',', *in);
}

void JSONCompactEachRowFormatReader::skipRowEndDelimiter()
{
    skipWhitespaceIfAny(*in);
    assertChar(']', *in);

    skipWhitespaceIfAny(*in);
    if (!in->eof() && (*in->position() == ',' || *in->position() == ';'))
        ++in->position();

    skipWhitespaceIfAny(*in);
}

void JSONCompactEachRowFormatReader::skipField()
{
    skipWhitespaceIfAny(*in);
    skipJSONField(*in, "skipped_field");
}

void JSONCompactEachRowFormatReader::skipHeaderRow()
{
    skipRowStartDelimiter();
    do
    {
        skipField();
        skipWhitespaceIfAny(*in);
    }
    while (checkChar(',', *in));

    skipRowEndDelimiter();
}

std::vector<String> JSONCompactEachRowFormatReader::readHeaderRow()
{
    skipRowStartDelimiter();
    std::vector<String> fields;
    String field;
    do
    {
        skipWhitespaceIfAny(*in);
        readJSONString(field, *in);
        fields.push_back(field);
        skipWhitespaceIfAny(*in);
    }
    while (checkChar(',', *in));

    skipRowEndDelimiter();
    return fields;
}

bool JSONCompactEachRowFormatReader::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool /*is_last_file_column*/, const String & column_name)
{
    skipWhitespaceIfAny(*in);
    return JSONUtils::readField(*in, column, type, serialization, column_name, format_settings, yield_strings);
}

bool JSONCompactEachRowFormatReader::parseRowStartWithDiagnosticInfo(WriteBuffer & out)
{
    skipWhitespaceIfAny(*in);
    if (!checkChar('[', *in))
    {
        out << "ERROR: There is no '[' before the row.\n";
        return false;
    }

    return true;
}

bool JSONCompactEachRowFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
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

bool JSONCompactEachRowFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
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

JSONCompactEachRowRowSchemaReader::JSONCompactEachRowRowSchemaReader(
    ReadBuffer & in_, bool with_names_, bool with_types_, bool yield_strings_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(in_, format_settings_, with_names_, with_types_, &reader)
    , reader(in_, yield_strings_, format_settings_)
{
    bool allow_bools_as_numbers = format_settings_.json.read_bools_as_numbers;
    setCommonTypeChecker([allow_bools_as_numbers](const DataTypePtr & first, const DataTypePtr & second)
    {
        return JSONUtils::getCommonTypeForJSONFormats(first, second, allow_bools_as_numbers);
    });
}

DataTypes JSONCompactEachRowRowSchemaReader::readRowAndGetDataTypes()
{
    if (first_row)
        first_row = false;
    else
    {
        skipWhitespaceIfAny(in);
        /// ',' and ';' are possible between the rows.
        if (!in.eof() && (*in.position() == ',' || *in.position() == ';'))
            ++in.position();
    }

    skipWhitespaceIfAny(in);
    if (in.eof())
        return {};

    return JSONUtils::readRowAndGetDataTypesForJSONCompactEachRow(in, reader.yieldStrings());
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
        markFormatWithNamesAndTypesSupportsSamplingColumns(yield_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", factory);
    }
}

void registerJSONCompactEachRowSchemaReader(FormatFactory & factory)
{
    for (bool json_strings : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerSchemaReader(format_name, [=](ReadBuffer & buf, const FormatSettings & settings)
            {
                return std::make_shared<JSONCompactEachRowRowSchemaReader>(buf, with_names, with_types, json_strings, settings);
            });
        };
        registerWithNamesAndTypes(json_strings ? "JSONCompactStringsEachRow" : "JSONCompactEachRow", register_func);
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
            return JSONUtils::fileSegmentationEngineJSONCompactEachRow(in, memory, min_chunk_size, min_rows);
        });
    };

    registerWithNamesAndTypes("JSONCompactEachRow", register_func);
    registerWithNamesAndTypes("JSONCompactStringsEachRow", register_func);
}

}
