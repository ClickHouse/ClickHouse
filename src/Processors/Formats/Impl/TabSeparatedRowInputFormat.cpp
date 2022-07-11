#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Formats/verbosePrintString.h>
#include <Formats/EscapingRuleUtils.h>
#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/** Check for a common error case - usage of Windows line feed.
  */
static void checkForCarriageReturn(ReadBuffer & in)
{
    if (!in.eof() && (in.position()[0] == '\r' || (in.position() != in.buffer().begin() && in.position()[-1] == '\r')))
        throw Exception("\nYou have carriage return (\\r, 0x0D, ASCII 13) at end of first row."
            "\nIt's like your input data has DOS/Windows style line separators, that are illegal in TabSeparated format."
            " You must transform your file to Unix format."
            "\nBut if you really need carriage return at end of string value of last column, you need to escape it as \\r.",
            ErrorCodes::INCORRECT_DATA);
}

TabSeparatedRowInputFormat::TabSeparatedRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool is_raw_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(header_, in_, params_, with_names_, with_types_, format_settings_, std::make_unique<TabSeparatedFormatReader>(in_, format_settings_, is_raw_))
{
}

TabSeparatedFormatReader::TabSeparatedFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_, bool is_raw_)
    : FormatWithNamesAndTypesReader(in_, format_settings_), is_raw(is_raw_)
{
}

void TabSeparatedFormatReader::skipFieldDelimiter()
{
    assertChar('\t', *in);
}

void TabSeparatedFormatReader::skipRowEndDelimiter()
{
    if (in->eof())
        return;

    if (unlikely(first_row))
    {
        checkForCarriageReturn(*in);
        first_row = false;
    }

    assertChar('\n', *in);
}

String TabSeparatedFormatReader::readFieldIntoString()
{
    String field;
    if (is_raw)
        readString(field, *in);
    else
        readEscapedString(field, *in);
    return field;
}

void TabSeparatedFormatReader::skipField()
{
    readFieldIntoString();
}

void TabSeparatedFormatReader::skipHeaderRow()
{
    do
    {
        skipField();
    }
    while (checkChar('\t', *in));

    skipRowEndDelimiter();
}

std::vector<String> TabSeparatedFormatReader::readRow()
{
    std::vector<String> fields;
    do
    {
        fields.push_back(readFieldIntoString());
    }
    while (checkChar('\t', *in));

    skipRowEndDelimiter();
    return fields;
}

bool TabSeparatedFormatReader::readField(IColumn & column, const DataTypePtr & type,
    const SerializationPtr & serialization, bool is_last_file_column, const String & /*column_name*/)
{
    const bool at_delimiter = !is_last_file_column && !in->eof() && *in->position() == '\t';
    const bool at_last_column_line_end = is_last_file_column && (in->eof() || *in->position() == '\n');

    if (format_settings.tsv.empty_as_default && (at_delimiter || at_last_column_line_end))
    {
        column.insertDefault();
        return false;
    }

    bool as_nullable = format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable();

    if (is_raw)
    {
        if (as_nullable)
            return SerializationNullable::deserializeTextRawImpl(column, *in, format_settings, serialization);

        serialization->deserializeTextRaw(column, *in, format_settings);
        return true;
    }


    if (as_nullable)
        return SerializationNullable::deserializeTextEscapedImpl(column, *in, format_settings, serialization);

    serialization->deserializeTextEscaped(column, *in, format_settings);
    return true;
}

bool TabSeparatedFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    try
    {
        assertChar('\t', *in);
    }
    catch (const DB::Exception &)
    {
        if (*in->position() == '\n')
        {
            out << "ERROR: Line feed found where tab is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, "
                   "maybe it has an unescaped backslash in value before tab, which causes the tab to be escaped.\n";
        }
        else if (*in->position() == '\r')
        {
            out << "ERROR: Carriage return found where tab is expected.\n";
        }
        else
        {
            out << "ERROR: There is no tab. ";
            verbosePrintString(in->position(), in->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool TabSeparatedFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    if (in->eof())
        return true;

    try
    {
        assertChar('\n', *in);
    }
    catch (const DB::Exception &)
    {
        if (*in->position() == '\t')
        {
            out << "ERROR: Tab found where line feed is expected."
                   " It's like your file has more columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has an unescaped tab in a value.\n";
        }
        else if (*in->position() == '\r')
        {
            out << "ERROR: Carriage return found where line feed is expected."
                   " It's like your file has DOS/Windows style line separators, that is illegal in TabSeparated format.\n";
        }
        else
        {
            out << "ERROR: There is no line feed. ";
            verbosePrintString(in->position(), in->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

void TabSeparatedFormatReader::checkNullValueForNonNullable(DataTypePtr type)
{
    bool can_be_parsed_as_null = type->isNullable() || type->isLowCardinalityNullable() || format_settings.null_as_default;

    // check null value for type is not nullable. don't cross buffer bound for simplicity, so maybe missing some case
    if (!can_be_parsed_as_null && !in->eof())
    {
        if (*in->position() == '\\' && in->available() >= 2)
        {
            ++in->position();
            if (*in->position() == 'N')
            {
                ++in->position();
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected NULL value of not Nullable type {}", type->getName());
            }
            else
            {
                --in->position();
            }
        }
    }
}

void TabSeparatedRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

TabSeparatedSchemaReader::TabSeparatedSchemaReader(
    ReadBuffer & in_, bool with_names_, bool with_types_, bool is_raw_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(
        in_,
        format_settings_,
        with_names_,
        with_types_,
        &reader,
        getDefaultDataTypeForEscapingRule(is_raw_ ? FormatSettings::EscapingRule::Raw : FormatSettings::EscapingRule::Escaped))
    , reader(in_, format_settings_, is_raw_)
{
}

DataTypes TabSeparatedSchemaReader::readRowAndGetDataTypes()
{
    if (in.eof())
        return {};

    auto fields = reader.readRow();
    return determineDataTypesByEscapingRule(fields, reader.getFormatSettings(), reader.getEscapingRule());
}

void registerInputFormatTabSeparated(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerInputFormat(format_name, [with_names, with_types, is_raw](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
            {
                return std::make_shared<TabSeparatedRowInputFormat>(sample, buf, std::move(params), with_names, with_types, is_raw, settings);
            });
        };

        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
    }
}

void registerTSVSchemaReader(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerSchemaReader(format_name, [with_names, with_types, is_raw](ReadBuffer & buf, const FormatSettings & settings)
            {
                return std::make_shared<TabSeparatedSchemaReader>(buf, with_names, with_types, is_raw, settings);
            });
        };

        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
    }
}

static std::pair<bool, size_t> fileSegmentationEngineTabSeparatedImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, bool is_raw, size_t min_rows)
{
    bool need_more_data = true;
    char * pos = in.position();
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        if (is_raw)
            pos = find_first_symbols<'\r', '\n'>(pos, in.buffer().end());
        else
            pos = find_first_symbols<'\\', '\r', '\n'>(pos, in.buffer().end());

        if (pos > in.buffer().end())
            throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);

        if (pos == in.buffer().end())
            continue;

        if (!is_raw && *pos == '\\')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos))
                ++pos;
        }
        else if (*pos == '\n' || *pos == '\r')
        {
            if (*pos == '\n')
                ++number_of_rows;

            if ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size) && number_of_rows >= min_rows)
                need_more_data = false;
            ++pos;
        }
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineTabSeparated(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            size_t min_rows = 1 + int(with_names) + int(with_types);
            factory.registerFileSegmentationEngine(format_name, [is_raw, min_rows](ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
            {
                return fileSegmentationEngineTabSeparatedImpl(in, memory, min_chunk_size, is_raw, min_rows);
            });
        };

        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
    }

    // We can use the same segmentation engine for TSKV.
    factory.registerFileSegmentationEngine("TSKV", [](
        ReadBuffer & in,
        DB::Memory<> & memory,
        size_t min_chunk_size)
    {
        return fileSegmentationEngineTabSeparatedImpl(in, memory, min_chunk_size, false, 1);
    });
}

}
