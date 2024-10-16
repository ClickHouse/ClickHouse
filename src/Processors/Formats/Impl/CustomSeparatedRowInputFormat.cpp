#include <Processors/Formats/Impl/CustomSeparatedRowInputFormat.h>
#include <Processors/Formats/Impl/TemplateRowInputFormat.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
        header_, std::make_unique<PeekableReadBuffer>(in_buf_), params_, with_names_, with_types_, ignore_spaces_, format_settings_)
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
        false,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<CustomSeparatedFormatReader>(*buf_, ignore_spaces_, format_settings_),
        format_settings_.custom.try_detect_header)
    , buf(std::move(buf_)), ignore_spaces(ignore_spaces_)
{
    /// In case of CustomSeparatedWithNames(AndTypes) formats and enabled setting input_format_with_names_use_header we don't know
    /// the exact number of columns in data (because it can contain unknown columns). So, if field_delimiter and row_after_delimiter are
    /// the same and row_between_delimiter is empty, we won't be able to determine the end of row while reading column names or types.
    if ((with_types_ || with_names_) && format_settings_.with_names_use_header
        && format_settings_.custom.field_delimiter == format_settings_.custom.row_after_delimiter
        && format_settings_.custom.row_between_delimiter.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Input format CustomSeparatedWithNames(AndTypes) cannot work properly with enabled setting "
                        "input_format_with_names_use_header, when format_custom_field_delimiter and "
                        "format_custom_row_after_delimiter are the same "
                        "and format_custom_row_between_delimiter is empty.");
    }
}

void CustomSeparatedRowInputFormat::readPrefix()
{
    RowInputFormatWithNamesAndTypes::readPrefix();

    /// Provide better error message for unsupported delimiters
    for (const auto & column_index : column_mapping->column_indexes_for_input_fields)
    {
        if (column_index)
            checkSupportedDelimiterAfterField(format_settings.custom.escaping_rule, format_settings.custom.field_delimiter, data_types[*column_index]);
        else
            checkSupportedDelimiterAfterField(format_settings.custom.escaping_rule, format_settings.custom.field_delimiter, nullptr);
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

void CustomSeparatedRowInputFormat::resetReadBuffer()
{
    buf.reset();
    RowInputFormatWithNamesAndTypes::resetReadBuffer();
}

CustomSeparatedFormatReader::CustomSeparatedFormatReader(
    PeekableReadBuffer & buf_, bool ignore_spaces_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesReader(buf_, format_settings_), buf(&buf_), ignore_spaces(ignore_spaces_)
{
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
    if (format_settings.custom.escaping_rule == FormatSettings::EscapingRule::CSV)
        readCSVFieldWithTwoPossibleDelimiters(*buf, format_settings.csv, format_settings.custom.field_delimiter, format_settings.custom.row_after_delimiter);
    else
        skipFieldByEscapingRule(*buf, format_settings.custom.escaping_rule, format_settings);
}

bool CustomSeparatedFormatReader::checkForEndOfRow()
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

template <CustomSeparatedFormatReader::ReadFieldMode mode>
String CustomSeparatedFormatReader::readFieldIntoString(bool is_first, bool is_last, bool is_unknown)
{
    if (!is_first)
        skipFieldDelimiter();
    skipSpaces();
    updateFormatSettings(is_last);
    if constexpr (mode != ReadFieldMode::AS_FIELD)
    {
        /// If the number of columns is unknown and we use CSV escaping rule,
        /// we don't know what delimiter to expect after the value,
        /// so we should read until we meet field_delimiter or row_after_delimiter.
        if (is_unknown && format_settings.custom.escaping_rule == FormatSettings::EscapingRule::CSV)
            return readCSVStringWithTwoPossibleDelimiters(
                *buf, format_settings.csv, format_settings.custom.field_delimiter, format_settings.custom.row_after_delimiter);

        if constexpr (mode == ReadFieldMode::AS_STRING)
            return readStringByEscapingRule(*buf, format_settings.custom.escaping_rule, format_settings);
        else
            return readStringOrFieldByEscapingRule(*buf, format_settings.custom.escaping_rule, format_settings);
    }
    else
    {
        if (is_unknown && format_settings.custom.escaping_rule == FormatSettings::EscapingRule::CSV)
            return readCSVFieldWithTwoPossibleDelimiters(
                *buf, format_settings.csv, format_settings.custom.field_delimiter, format_settings.custom.row_after_delimiter);

        return readFieldByEscapingRule(*buf, format_settings.custom.escaping_rule, format_settings);
    }
}

template <CustomSeparatedFormatReader::ReadFieldMode mode>
std::vector<String> CustomSeparatedFormatReader::readRowImpl()
{
    std::vector<String> values;
    skipRowStartDelimiter();

    if (columns == 0 || allowVariableNumberOfColumns())
    {
        do
        {
            values.push_back(readFieldIntoString<mode>(values.empty(), false, true));
        } while (!checkForEndOfRow());
        columns = values.size();
    }
    else
    {
        for (size_t i = 0; i != columns; ++i)
            values.push_back(readFieldIntoString<mode>(i == 0, i + 1 == columns, false));
    }

    skipRowEndDelimiter();
    return values;
}

void CustomSeparatedFormatReader::skipRow()
{
    skipRowStartDelimiter();

    /// If the number of columns in row is unknown,
    /// we should check for end of row after each field.
    if (columns == 0 || allowVariableNumberOfColumns())
    {
        bool first = true;
        do
        {
            if (!first)
                skipFieldDelimiter();
            first = false;

            skipField();
        }
        while (!checkForEndOfRow());
    }
    else
    {
        for (size_t i = 0; i != columns; ++i)
        {
            if (i != 0)
                skipFieldDelimiter();
            skipField();
        }
    }

    skipRowEndDelimiter();
}

void CustomSeparatedFormatReader::updateFormatSettings(bool is_last_column)
{
    if (format_settings.custom.escaping_rule != FormatSettings::EscapingRule::CSV)
        return;

    /// Clean custom delimiter from previous delimiter.
    format_settings.csv.custom_delimiter.clear();

    /// If delimiter has length = 1, it will be more efficient to use csv.delimiter.
    /// If we have some complex delimiter, normal CSV reading will now work properly if we will
    /// use just the first character of delimiter (for example, if delimiter='||' and we have data 'abc|d||')
    /// We have special implementation for such case that uses custom delimiter, it's not so efficient,
    /// but works properly.

    if (is_last_column)
    {
        /// If field delimiter has length = 1, it will be more efficient to use csv.delimiter.
        if (format_settings.custom.row_after_delimiter.size() == 1)
            format_settings.csv.delimiter = format_settings.custom.row_after_delimiter.front();
        else
            format_settings.csv.custom_delimiter = format_settings.custom.row_after_delimiter;
    }
    else
    {
        if (format_settings.custom.field_delimiter.size() == 1)
            format_settings.csv.delimiter = format_settings.custom.field_delimiter.front();
        else
            format_settings.csv.custom_delimiter = format_settings.custom.field_delimiter;
    }
}

bool CustomSeparatedFormatReader::readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String &)
{
    skipSpaces();
    updateFormatSettings(is_last_file_column);
    return deserializeFieldByEscapingRule(type, serialization, column, *buf, format_settings.custom.escaping_rule, format_settings);
}

bool CustomSeparatedFormatReader::checkForSuffixImpl(bool check_eof)
{
    skipSpaces();
    if (format_settings.custom.result_after_delimiter.empty())
    {
        if (!check_eof)
            return false;

        /// Allow optional \n before eof.
        checkChar('\n', *buf);
        if (format_settings.custom.skip_trailing_empty_lines)
            while (checkChar('\n', *buf) || checkChar('\r', *buf));
        return buf->eof();
    }

    if (unlikely(checkString(format_settings.custom.result_after_delimiter, *buf)))
    {
        skipSpaces();
        if (!check_eof)
            return true;

        /// Allow optional \n before eof.
        checkChar('\n', *buf);
        if (format_settings.custom.skip_trailing_empty_lines)
            while (checkChar('\n', *buf) || checkChar('\r', *buf));
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
    FormatWithNamesAndTypesReader::setReadBuffer(*buf);
}

CustomSeparatedSchemaReader::CustomSeparatedSchemaReader(
    ReadBuffer & in_, bool with_names_, bool with_types_, bool ignore_spaces_, const FormatSettings & format_setting_)
    : FormatWithNamesAndTypesSchemaReader(
        buf,
        format_setting_,
        with_names_,
        with_types_,
        &reader,
        getDefaultDataTypeForEscapingRule(format_setting_.custom.escaping_rule),
        format_setting_.custom.try_detect_header)
    , buf(in_)
    , reader(buf, ignore_spaces_, format_setting_)
{
}

std::optional<std::pair<std::vector<String>, DataTypes>> CustomSeparatedSchemaReader::readRowAndGetFieldsAndDataTypes()
{
    if (no_more_data || reader.checkForSuffix())
    {
        no_more_data = true;
        return {};
    }

    if (!first_row || with_names || with_types)
        reader.skipRowBetweenDelimiter();

    if (first_row)
        first_row = false;

    auto fields = reader.readRow();
    auto data_types = tryInferDataTypesByEscapingRule(fields, reader.getFormatSettings(), reader.getEscapingRule(), &json_inference_info);
    return std::make_pair(std::move(fields), std::move(data_types));
}

std::optional<DataTypes> CustomSeparatedSchemaReader::readRowAndGetDataTypesImpl()
{
    auto fields_with_types = readRowAndGetFieldsAndDataTypes();
    if (!fields_with_types)
        return {};
    return std::move(fields_with_types->second);
}

void CustomSeparatedSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredTypesByEscapingRuleIfNeeded(type, new_type, format_settings, reader.getEscapingRule(), &json_inference_info);
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
        markFormatWithNamesAndTypesSupportsSamplingColumns(ignore_spaces ? "CustomSeparatedIgnoreSpaces" : "CustomSeparated", factory);
    }
}

void registerCustomSeparatedSchemaReader(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        auto register_func = [&](const String & format_name, bool with_names, bool with_types)
        {
            factory.registerSchemaReader(format_name, [with_names, with_types, ignore_spaces](ReadBuffer & buf, const FormatSettings & settings)
            {
                return std::make_shared<CustomSeparatedSchemaReader>(buf, with_names, with_types, ignore_spaces, settings);
            });
            if (!with_types)
            {
                factory.registerAdditionalInfoForSchemaCacheGetter(format_name, [with_names](const FormatSettings & settings)
                {
                    String result = getAdditionalFormatInfoByEscapingRule(settings, settings.custom.escaping_rule);
                    if (!with_names)
                        result += fmt::format(", column_names_for_schema_inference={}, try_detect_header={}", settings.column_names_for_schema_inference, settings.custom.try_detect_header);
                    return result + fmt::format(
                            ", result_before_delimiter={}, row_before_delimiter={}, field_delimiter={},"
                            " row_after_delimiter={}, row_between_delimiter={}, result_after_delimiter={}",
                            settings.custom.result_before_delimiter,
                            settings.custom.row_before_delimiter,
                            settings.custom.field_delimiter,
                            settings.custom.row_after_delimiter,
                            settings.custom.row_between_delimiter,
                            settings.custom.result_after_delimiter);
                });
            }
        };

        registerWithNamesAndTypes(ignore_spaces ? "CustomSeparatedIgnoreSpaces" : "CustomSeparated", register_func);
    }
}

}
