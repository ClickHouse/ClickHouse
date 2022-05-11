#include <Processors/Formats/Impl/CustomSeparatedRowInputFormat.h>
#include <Processors/Formats/Impl/TemplateRowInputFormat.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static FormatSettings updateFormatSettings(const FormatSettings & settings)
{
    if (settings.custom.escaping_rule != FormatSettings::EscapingRule::CSV || settings.custom.field_delimiter.empty())
        return settings;

    auto updated = settings;
    updated.csv.delimiter = settings.custom.field_delimiter.front();
    return updated;
}

CustomSeparatedRowInputFormat::CustomSeparatedRowInputFormat(
    const Block & header_,
    ReadBuffer & in_buf_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool ignore_spaces_,
    const FormatSettings & format_settings_)
    : CustomSeparatedRowInputFormat(
        header_, std::make_unique<PeekableReadBuffer>(in_buf_), params_, with_names_, with_types_, ignore_spaces_, updateFormatSettings(format_settings_))
{
}

CustomSeparatedRowInputFormat::CustomSeparatedRowInputFormat(
    const Block & header_,
    std::unique_ptr<PeekableReadBuffer> buf_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool ignore_spaces_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_,
        *buf_,
        params_,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<CustomSeparatedFormatReader>(*buf_, ignore_spaces_, format_settings_))
    , buf(std::move(buf_))
{
    /// In case of CustomSeparatedWithNames(AndTypes) formats and enabled setting input_format_with_names_use_header we don't know
    /// the exact number of columns in data (because it can contain unknown columns). So, if field_delimiter and row_after_delimiter are
    /// the same and row_between_delimiter is empty, we won't be able to determine the end of row while reading column names or types.
    if ((with_types_ || with_names_) && format_settings_.with_names_use_header
        && format_settings_.custom.field_delimiter == format_settings_.custom.row_after_delimiter
        && format_settings_.custom.row_between_delimiter.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Input format CustomSeparatedWithNames(AndTypes) cannot work properly with enabled setting input_format_with_names_use_header, "
                        "when format_custom_field_delimiter and format_custom_row_after_delimiter are the same and format_custom_row_between_delimiter is empty.");
    }
}


bool CustomSeparatedRowInputFormat::allowSyncAfterError() const
{
    return !format_settings.custom.row_after_delimiter.empty() || !format_settings.custom.row_between_delimiter.empty();
}

void CustomSeparatedRowInputFormat::syncAfterError()
{
    skipToNextRowOrEof(*buf, format_settings.custom.row_after_delimiter, format_settings.custom.row_between_delimiter, ignore_spaces);
    end_of_stream = buf->eof();
    /// It can happen that buf->position() is not at the beginning of row
    /// if some delimiters is similar to row_format.delimiters.back() and row_between_delimiter.
    /// It will cause another parsing error.
}

void CustomSeparatedRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    RowInputFormatWithNamesAndTypes::setReadBuffer(*buf);
}

CustomSeparatedFormatReader::CustomSeparatedFormatReader(
    PeekableReadBuffer & buf_, bool ignore_spaces_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesReader(buf_, format_settings_), buf(&buf_), ignore_spaces(ignore_spaces_)
{
}

void CustomSeparatedRowInputFormat::resetParser()
{
    RowInputFormatWithNamesAndTypes::resetParser();
    buf->reset();
}

void CustomSeparatedFormatReader::skipPrefixBeforeHeader()
{
    skipSpaces();
    assertString(format_settings.custom.result_before_delimiter, *buf);
}

void CustomSeparatedFormatReader::skipRowStartDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.row_before_delimiter, *buf);
}

void CustomSeparatedFormatReader::skipFieldDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.field_delimiter, *buf);
}

void CustomSeparatedFormatReader::skipRowEndDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.row_after_delimiter, *buf);
}

void CustomSeparatedFormatReader::skipRowBetweenDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.row_between_delimiter, *buf);
}

void CustomSeparatedFormatReader::skipField()
{
    skipSpaces();
    skipFieldByEscapingRule(*buf, format_settings.custom.escaping_rule, format_settings);
}

bool CustomSeparatedFormatReader::checkEndOfRow()
{
    PeekableReadBufferCheckpoint checkpoint{*buf, true};

    skipSpaces();
    if (!checkString(format_settings.custom.row_after_delimiter, *buf))
        return false;

    skipSpaces();

    /// At the end of row after row_after_delimiter we expect result_after_delimiter or row_between_delimiter.

    if (checkString(format_settings.custom.row_between_delimiter, *buf))
        return true;

    buf->rollbackToCheckpoint();
    skipSpaces();
    buf->ignore(format_settings.custom.row_after_delimiter.size());
    return checkForSuffixImpl(true);
}

template <bool is_header>
String CustomSeparatedFormatReader::readFieldIntoString(bool is_first)
{
    if (!is_first)
        skipFieldDelimiter();
    skipSpaces();
    if constexpr (is_header)
        return readStringByEscapingRule(*buf, format_settings.custom.escaping_rule, format_settings);
    else
        return readFieldByEscapingRule(*buf, format_settings.custom.escaping_rule, format_settings);
}

template <bool is_header>
std::vector<String> CustomSeparatedFormatReader::readRowImpl()
{
    std::vector<String> values;
    skipRowStartDelimiter();

    if (columns == 0)
    {
        do
        {
            values.push_back(readFieldIntoString<is_header>(values.empty()));
        } while (!checkEndOfRow());
        columns = values.size();
    }
    else
    {
        for (size_t i = 0; i != columns; ++i)
            values.push_back(readFieldIntoString<is_header>(i == 0));
    }

    skipRowEndDelimiter();
    return values;
}

void CustomSeparatedFormatReader::skipHeaderRow()
{
    skipRowStartDelimiter();
    bool first = true;
    do
    {
        if (!first)
            skipFieldDelimiter();
        first = false;

        skipField();
    }
    while (!checkEndOfRow());

    skipRowEndDelimiter();
}

bool CustomSeparatedFormatReader::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool, const String &)
{
    skipSpaces();
    return deserializeFieldByEscapingRule(type, serialization, column, *buf, format_settings.custom.escaping_rule, format_settings);
}

bool CustomSeparatedFormatReader::checkForSuffixImpl(bool check_eof)
{
    skipSpaces();
    if (format_settings.custom.result_after_delimiter.empty())
    {
        if (!check_eof)
            return false;

        return buf->eof();
    }

    if (unlikely(checkString(format_settings.custom.result_after_delimiter, *buf)))
    {
        skipSpaces();
        if (!check_eof)
            return true;

        if (buf->eof())
            return true;
    }
    return false;
}

bool CustomSeparatedFormatReader::tryParseSuffixWithDiagnosticInfo(WriteBuffer & out)
{
    PeekableReadBufferCheckpoint checkpoint{*buf};
    if (checkForSuffixImpl(false))
    {
        if (buf->eof())
            out << "<End of stream>\n";
        else
            out << " There is some data after suffix\n";
        return false;
    }
    buf->rollbackToCheckpoint();
    return true;
}

bool CustomSeparatedFormatReader::checkForSuffix()
{
    PeekableReadBufferCheckpoint checkpoint{*buf};
    if (checkForSuffixImpl(true))
        return true;
    buf->rollbackToCheckpoint();
    return false;
}

bool CustomSeparatedFormatReader::parseRowStartWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, *buf, format_settings.custom.row_before_delimiter, "delimiter before first field", ignore_spaces);
}

bool CustomSeparatedFormatReader::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, *buf, format_settings.custom.field_delimiter, "delimiter between fields", ignore_spaces);
}

bool CustomSeparatedFormatReader::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, *buf, format_settings.custom.row_after_delimiter, "delimiter after last field", ignore_spaces);
}

bool CustomSeparatedFormatReader::parseRowBetweenDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, *buf, format_settings.custom.row_between_delimiter, "delimiter between rows", ignore_spaces);
}

void CustomSeparatedFormatReader::setReadBuffer(ReadBuffer & in_)
{
    buf = assert_cast<PeekableReadBuffer *>(&in_);
    FormatWithNamesAndTypesReader::setReadBuffer(in_);
}

CustomSeparatedSchemaReader::CustomSeparatedSchemaReader(
    ReadBuffer & in_, bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_setting_, ContextPtr context_)
    : FormatWithNamesAndTypesSchemaReader(
        buf,
        format_setting_.max_rows_to_read_for_schema_inference,
        with_names_,
        with_types_,
        &reader,
        getDefaultDataTypeForEscapingRule(format_setting_.custom.escaping_rule))
    , buf(in_)
    , reader(buf, ignore_spaces_, updateFormatSettings(format_setting_))
    , context(context_)
{
}

DataTypes CustomSeparatedSchemaReader::readRowAndGetDataTypes()
{
    if (reader.checkForSuffix())
        return {};

    if (!first_row || with_names || with_types)
        reader.skipRowBetweenDelimiter();

    if (first_row)
        first_row = false;

    auto fields = reader.readRow();
    return determineDataTypesByEscapingRule(fields, reader.getFormatSettings(), reader.getEscapingRule(), context);
}

void registerInputFormatCustomSeparated(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerInputFormat(format_name, [=](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
            {
                return std::make_shared<CustomSeparatedRowInputFormat>(sample, buf, params, with_names, with_types, ignore_spaces, settings);
            });
        };
        registerWithNamesAndTypes(ignore_spaces ? "CustomSeparatedIgnoreSpaces" : "CustomSeparated", register_func);
    }
}

void registerCustomSeparatedSchemaReader(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerSchemaReader(format_name, [with_names, with_types, ignore_spaces](ReadBuffer & buf, const FormatSettings & settings, ContextPtr context)
            {
                return std::make_shared<CustomSeparatedSchemaReader>(buf, with_names, with_types, ignore_spaces, settings, context);
            });
        };

        registerWithNamesAndTypes(ignore_spaces ? "CustomSeparatedIgnoreSpaces" : "CustomSeparated", register_func);
    }
}

}
