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
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    bool ignore_spaces_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(header_, buf, params_, with_names_, with_types_, updateFormatSettings(format_settings_))
    , buf(in_)
    , ignore_spaces(ignore_spaces_)
    , escaping_rule(format_settings_.custom.escaping_rule)
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

void CustomSeparatedRowInputFormat::skipPrefixBeforeHeader()
{
    skipSpaces();
    assertString(format_settings.custom.result_before_delimiter, buf);
}

void CustomSeparatedRowInputFormat::skipRowStartDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.row_before_delimiter, buf);
}

void CustomSeparatedRowInputFormat::skipFieldDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.field_delimiter, buf);
}

void CustomSeparatedRowInputFormat::skipRowEndDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.row_after_delimiter, buf);
}

void CustomSeparatedRowInputFormat::skipRowBetweenDelimiter()
{
    skipSpaces();
    assertString(format_settings.custom.row_between_delimiter, buf);
}

void CustomSeparatedRowInputFormat::skipField()
{
    skipSpaces();
    skipFieldByEscapingRule(buf, escaping_rule, format_settings);
}

bool CustomSeparatedRowInputFormat::checkEndOfRow()
{
    PeekableReadBufferCheckpoint checkpoint{buf, true};

    skipSpaces();
    if (!checkString(format_settings.custom.row_after_delimiter, buf))
        return false;

    skipSpaces();

    /// At the end of row after row_after_delimiter we expect result_after_delimiter or row_between_delimiter.

    if (checkString(format_settings.custom.row_between_delimiter, buf))
        return true;

    buf.rollbackToCheckpoint();
    skipSpaces();
    buf.ignore(format_settings.custom.row_after_delimiter.size());
    return checkForSuffixImpl(true);
}

std::vector<String> CustomSeparatedRowInputFormat::readHeaderRow()
{
    std::vector<String> values;
    skipRowStartDelimiter();
    do
    {
        if (!values.empty())
            skipFieldDelimiter();
        skipSpaces();
        values.push_back(readStringByEscapingRule(buf, escaping_rule, format_settings));
    }
    while (!checkEndOfRow());

    skipRowEndDelimiter();
    return values;
}

void CustomSeparatedRowInputFormat::skipHeaderRow()
{
    size_t columns = getPort().getHeader().columns();
    skipRowStartDelimiter();
    for (size_t i = 0; i != columns; ++i)
    {
        skipField();
        if (i + 1 != columns)
            skipFieldDelimiter();
    }
    skipRowEndDelimiter();
}

bool CustomSeparatedRowInputFormat::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool, const String &)
{
    skipSpaces();
    return deserializeFieldByEscapingRule(type, serialization, column, buf, escaping_rule, format_settings);
}

bool CustomSeparatedRowInputFormat::checkForSuffixImpl(bool check_eof)
{
    skipSpaces();
    if (format_settings.custom.result_after_delimiter.empty())
    {
        if (!check_eof)
            return false;

        return buf.eof();
    }

    if (unlikely(checkString(format_settings.custom.result_after_delimiter, buf)))
    {
        skipSpaces();
        if (!check_eof)
            return true;

        if (buf.eof())
            return true;
    }
    return false;
}

bool CustomSeparatedRowInputFormat::tryParseSuffixWithDiagnosticInfo(WriteBuffer & out)
{
    PeekableReadBufferCheckpoint checkpoint{buf};
    if (checkForSuffixImpl(false))
    {
        if (buf.eof())
            out << "<End of stream>\n";
        else
            out << " There is some data after suffix\n";
        return false;
    }
    buf.rollbackToCheckpoint();
    return true;
}

bool CustomSeparatedRowInputFormat::checkForSuffix()
{
    PeekableReadBufferCheckpoint checkpoint{buf};
    if (checkForSuffixImpl(true))
        return true;
    buf.rollbackToCheckpoint();
    return false;
}


bool CustomSeparatedRowInputFormat::allowSyncAfterError() const
{
    return !format_settings.custom.row_after_delimiter.empty() || !format_settings.custom.row_between_delimiter.empty();
}

void CustomSeparatedRowInputFormat::syncAfterError()
{
    skipToNextRowOrEof(buf, format_settings.custom.row_after_delimiter, format_settings.custom.row_between_delimiter, ignore_spaces);
    end_of_stream = buf.eof();
    /// It can happen that buf.position() is not at the beginning of row
    /// if some delimiters is similar to row_format.delimiters.back() and row_between_delimiter.
    /// It will cause another parsing error.
}

bool CustomSeparatedRowInputFormat::parseRowStartWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, buf, format_settings.custom.row_before_delimiter, "delimiter before first field", ignore_spaces);
}

bool CustomSeparatedRowInputFormat::parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, buf, format_settings.custom.field_delimiter, "delimiter between fields", ignore_spaces);
}

bool CustomSeparatedRowInputFormat::parseRowEndWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, buf, format_settings.custom.row_after_delimiter, "delimiter after last field", ignore_spaces);
}

bool CustomSeparatedRowInputFormat::parseRowBetweenDelimiterWithDiagnosticInfo(WriteBuffer & out)
{
    return parseDelimiterWithDiagnosticInfo(out, buf, format_settings.custom.row_between_delimiter, "delimiter between rows", ignore_spaces);
}

void CustomSeparatedRowInputFormat::resetParser()
{
    RowInputFormatWithNamesAndTypes::resetParser();
    buf.reset();
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

}
