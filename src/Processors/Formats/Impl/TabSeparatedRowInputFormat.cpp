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
        throw Exception(ErrorCodes::INCORRECT_DATA, "\nYou have carriage return (\\r, 0x0D, ASCII 13) at end of first row."
            "\nIt's like your input data has DOS/Windows style line separators, that are illegal in TabSeparated format."
            " You must transform your file to Unix format."
            "\nBut if you really need carriage return at end of string value of last column, you need to escape it as \\r.");
}

TabSeparatedRowInputFormat::TabSeparatedRowInputFormat(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool is_raw_,
    const FormatSettings & format_settings_)
    : TabSeparatedRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_, with_names_, with_types_, is_raw_, format_settings_)
{
}

TabSeparatedRowInputFormat::TabSeparatedRowInputFormat(
    const Block & header_,
    std::unique_ptr<PeekableReadBuffer> in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool is_raw,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        *in_,
        params_,
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<TabSeparatedFormatReader>(*in_, format_settings_, is_raw),
        format_settings_.tsv.try_detect_header)
    , buf(std::move(in_))
{
}

void TabSeparatedRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf->setSubBuffer(in_);
}

void TabSeparatedRowInputFormat::resetParser()
{
    RowInputFormatWithNamesAndTypes::resetParser();
    buf->reset();
}

TabSeparatedFormatReader::TabSeparatedFormatReader(PeekableReadBuffer & in_, const FormatSettings & format_settings_, bool is_raw_)
    : FormatWithNamesAndTypesReader(in_, format_settings_), buf(&in_), is_raw(is_raw_)
{
}

void TabSeparatedFormatReader::skipFieldDelimiter()
{
    assertChar('\t', *buf);
}

void TabSeparatedFormatReader::skipRowEndDelimiter()
{
    if (buf->eof())
        return;

    if (unlikely(first_row))
    {
        checkForCarriageReturn(*buf);
        first_row = false;
    }

    assertChar('\n', *buf);
}

template <bool read_string>
String TabSeparatedFormatReader::readFieldIntoString()
{
    String field;
    if (is_raw)
        readString(field, *buf);
    else
    {
        if constexpr (read_string)
            readEscapedString(field, *buf);
        else
            readTSVField(field, *buf);
    }
    return field;
}

void TabSeparatedFormatReader::skipField()
{
    NullOutput out;
    if (is_raw)
        readStringInto(out, *buf);
    else
        readEscapedStringInto(out, *buf);
}

void TabSeparatedFormatReader::skipHeaderRow()
{
    do
    {
        skipField();
    }
    while (checkChar('\t', *buf));

    skipRowEndDelimiter();
}

template <bool is_header_row>
std::vector<String> TabSeparatedFormatReader::readRowImpl()
{
    std::vector<String> fields;
    do
    {
        fields.push_back(readFieldIntoString<is_header_row>());
    }
    while (checkChar('\t', *buf));

    skipRowEndDelimiter();
    return fields;
}

bool TabSeparatedFormatReader::readField(IColumn & column, const DataTypePtr & type,
    const SerializationPtr & serialization, bool is_last_file_column, const String & /*column_name*/)
{
    const bool at_delimiter = !is_last_file_column && !buf->eof() && *buf->position() == '\t';
    const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n');

    if (format_settings.tsv.empty_as_default && (at_delimiter || at_last_column_line_end))
    {
        column.insertDefault();
        return false;
    }

    bool as_nullable = format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type);

    if (is_raw)
    {
        if (as_nullable)
            return SerializationNullable::deserializeTextRawImpl(column, *buf, format_settings, serialization);

        serialization->deserializeTextRaw(column, *buf, format_settings);
        return true;
    }


    if (as_nullable)
        return SerializationNullable::deserializeTextEscapedImpl(column, *buf, format_settings, serialization);

    serialization->deserializeTextEscaped(column, *buf, format_settings);
    return true;
}

bool TabSeparatedFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    try
    {
        assertChar('\t', *buf);
    }
    catch (const DB::Exception &)
    {
        if (*buf->position() == '\n')
        {
            out << "ERROR: Line feed found where tab is expected."
                   " It's like your file has less columns than expected.\n"
                   "And if your file has the right number of columns, "
                   "maybe it has an unescaped backslash in value before tab, which causes the tab to be escaped.\n";
        }
        else if (*buf->position() == '\r')
        {
            out << "ERROR: Carriage return found where tab is expected.\n";
        }
        else
        {
            out << "ERROR: There is no tab. ";
            verbosePrintString(buf->position(), buf->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

bool TabSeparatedFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    if (buf->eof())
        return true;

    try
    {
        assertChar('\n', *buf);
    }
    catch (const DB::Exception &)
    {
        if (*buf->position() == '\t')
        {
            out << "ERROR: Tab found where line feed is expected."
                   " It's like your file has more columns than expected.\n"
                   "And if your file has the right number of columns, maybe it has an unescaped tab in a value.\n";
        }
        else if (*buf->position() == '\r')
        {
            out << "ERROR: Carriage return found where line feed is expected."
                   " It's like your file has DOS/Windows style line separators, that is illegal in TabSeparated format.\n";
        }
        else
        {
            out << "ERROR: There is no line feed. ";
            verbosePrintString(buf->position(), buf->position() + 1, out);
            out << " found instead.\n";
        }
        return false;
    }

    return true;
}

void TabSeparatedFormatReader::checkNullValueForNonNullable(DataTypePtr type)
{
    bool can_be_parsed_as_null = isNullableOrLowCardinalityNullable(type) || format_settings.null_as_default;

    // check null value for type is not nullable. don't cross buffer bound for simplicity, so maybe missing some case
    if (!can_be_parsed_as_null && !buf->eof())
    {
        if (*buf->position() == '\\' && buf->available() >= 2)
        {
            ++buf->position();
            if (*buf->position() == 'N')
            {
                ++buf->position();
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected NULL value of not Nullable type {}", type->getName());
            }
            else
            {
                --buf->position();
            }
        }
    }
}

void TabSeparatedFormatReader::skipPrefixBeforeHeader()
{
    for (size_t i = 0; i != format_settings.tsv.skip_first_lines; ++i)
        readRow();
}

void TabSeparatedRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*buf);
}

void TabSeparatedFormatReader::setReadBuffer(ReadBuffer & in_)
{
    buf = assert_cast<PeekableReadBuffer *>(&in_);
    FormatWithNamesAndTypesReader::setReadBuffer(*buf);
}

bool TabSeparatedFormatReader::checkForSuffix()
{
    if (!format_settings.tsv.skip_trailing_empty_lines)
        return buf->eof();

    PeekableReadBufferCheckpoint checkpoint(*buf);
    while (checkChar('\n', *buf) || checkChar('\r', *buf));
    if (buf->eof())
        return true;

    buf->rollbackToCheckpoint();
    return false;
}

void TabSeparatedFormatReader::skipRow()
{
    ReadBuffer & istr = *buf;
    while (!istr.eof())
    {
        char * pos;
        if (is_raw)
            pos = find_first_symbols<'\r', '\n'>(istr.position(), istr.buffer().end());
        else
            pos = find_first_symbols<'\\', '\r', '\n'>(istr.position(), istr.buffer().end());

        istr.position() = pos;

        if (istr.position() > istr.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        else if (pos == istr.buffer().end())
            continue;

        if (!is_raw && *istr.position() == '\\')
        {
            ++istr.position();
            if (!istr.eof())
                ++istr.position();
            continue;
        }

        if (*istr.position() == '\n')
        {
            ++istr.position();
            if (!istr.eof() && *istr.position() == '\r')
                ++istr.position();
            return;
        }
        else if (*istr.position() == '\r')
        {
            ++istr.position();
            if (!istr.eof() && *istr.position() == '\n')
            {
                ++istr.position();
                return;
            }
        }
    }
}

bool TabSeparatedFormatReader::checkForEndOfRow()
{
    return buf->eof() || *buf->position() == '\n';
}

TabSeparatedSchemaReader::TabSeparatedSchemaReader(
    ReadBuffer & in_, bool with_names_, bool with_types_, bool is_raw_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(
        buf,
        format_settings_,
        with_names_,
        with_types_,
        &reader,
        getDefaultDataTypeForEscapingRule(is_raw_ ? FormatSettings::EscapingRule::Raw : FormatSettings::EscapingRule::Escaped),
        format_settings_.tsv.try_detect_header)
    , buf(in_)
    , reader(buf, format_settings_, is_raw_)
{
}

std::optional<std::pair<std::vector<String>, DataTypes>> TabSeparatedSchemaReader::readRowAndGetFieldsAndDataTypes()
{
    if (buf.eof())
        return {};

    auto fields = reader.readRow();
    auto data_types = tryInferDataTypesByEscapingRule(fields, reader.getFormatSettings(), reader.getEscapingRule());
    return std::make_pair(fields, data_types);
}

std::optional<DataTypes> TabSeparatedSchemaReader::readRowAndGetDataTypesImpl()
{
    auto fields_with_types = readRowAndGetFieldsAndDataTypes();
    if (!fields_with_types)
        return {};
    return std::move(fields_with_types->second);
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
            if (!with_types)
            {
                factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [with_names, is_raw](const FormatSettings & settings)
                {
                    String result = getAdditionalFormatInfoByEscapingRule(
                        settings, is_raw ? FormatSettings::EscapingRule::Raw : FormatSettings::EscapingRule::Escaped);
                    if (!with_names)
                        result += fmt::format(
                            ", column_names_for_schema_inference={}, try_detect_header={}",
                            settings.column_names_for_schema_inference,
                            settings.tsv.try_detect_header);
                    return result;
                });
            }
        };

        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
    }
}

static std::pair<bool, size_t> fileSegmentationEngineTabSeparatedImpl(ReadBuffer & in, DB::Memory<> & memory, bool is_raw, size_t min_bytes, size_t min_rows, size_t max_rows)
{
    bool need_more_data = true;
    char * pos = in.position();
    size_t number_of_rows = 0;

    if (max_rows && (max_rows < min_rows))
        max_rows = min_rows;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        if (is_raw)
            pos = find_first_symbols<'\r', '\n'>(pos, in.buffer().end());
        else
            pos = find_first_symbols<'\\', '\r', '\n'>(pos, in.buffer().end());

        if (pos > in.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        else if (pos == in.buffer().end())
            continue;

        if (!is_raw && *pos == '\\')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos))
                ++pos;
            continue;
        }

        if (*pos == '\n')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos) && *pos == '\r')
                ++pos;
        }
        else if (*pos == '\r')
        {
            ++pos;
            if (loadAtPosition(in, memory, pos) && *pos == '\n')
                ++pos;
            else
                continue;
        }

        ++number_of_rows;
        if ((number_of_rows >= min_rows)
            && ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows)))
            need_more_data = false;
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineTabSeparated(FormatFactory & factory)
{
    for (bool is_raw : {false, true})
    {
        auto register_func = [&](const String & format_name, bool, bool)
        {
            static constexpr size_t min_rows = 3; /// Make it 3 for header auto detection (first 3 rows must be always in the same segment).
            factory.registerFileSegmentationEngine(format_name, [is_raw](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
            {
                return fileSegmentationEngineTabSeparatedImpl(in, memory, is_raw, min_bytes, min_rows, max_rows);
            });
        };

        registerWithNamesAndTypes(is_raw ? "TSVRaw" : "TSV", register_func);
        registerWithNamesAndTypes(is_raw ? "TabSeparatedRaw" : "TabSeparated", register_func);
        markFormatWithNamesAndTypesSupportsSamplingColumns(is_raw ? "TSVRaw" : "TSV", factory);
        markFormatWithNamesAndTypesSupportsSamplingColumns(is_raw ? "TabSeparatedRaw" : "TabSeparated", factory);
    }

    // We can use the same segmentation engine for TSKV.
    factory.registerFileSegmentationEngine("TSKV", [](ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
    {
        return fileSegmentationEngineTabSeparatedImpl(in, memory, false, min_bytes, 1, max_rows);
    });
}

}
