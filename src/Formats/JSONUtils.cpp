#include <IO/ReadHelpers.h>
#include <Formats/JSONUtils.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/assert_cast.h>

#include <base/find_symbols.h>
#include <base/scope_guard.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace JSONUtils
{
    template <const char opening_bracket, const char closing_bracket>
    static std::pair<bool, size_t>
    fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows)
    {
        skipWhitespaceIfAny(in);

        char * pos = in.position();
        size_t balance = 0;
        bool quotes = false;
        size_t number_of_rows = 0;
        bool need_more_data = true;

        if (max_rows && (max_rows < min_rows))
            max_rows = min_rows;

        while (loadAtPosition(in, memory, pos) && need_more_data)
        {
            const auto current_object_size = memory.size() + static_cast<size_t>(pos - in.position());
            if (min_bytes != 0 && current_object_size > 10 * min_bytes)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Size of JSON object at position {} is extremely large. Expected not greater than {} bytes, but current is {} bytes per row. "
                    "Increase the value setting 'min_chunk_bytes_for_parallel_parsing' or check your data manually, "
                    "most likely JSON is malformed", in.count(), min_bytes, current_object_size);

            if (quotes)
            {
                pos = find_first_symbols<'\\', '"'>(pos, in.buffer().end());

                if (pos > in.buffer().end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
                if (pos == in.buffer().end())
                    continue;

                if (*pos == '\\')
                {
                    ++pos;
                    if (loadAtPosition(in, memory, pos))
                        ++pos;
                }
                else if (*pos == '"')
                {
                    ++pos;
                    quotes = false;
                }
            }
            else
            {
                pos = find_first_symbols<opening_bracket, closing_bracket, '"'>(pos, in.buffer().end());

                if (pos > in.buffer().end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
                if (pos == in.buffer().end())
                    continue;

                if (*pos == opening_bracket)
                {
                    ++balance;
                    ++pos;
                }
                else if (*pos == closing_bracket)
                {
                    --balance;
                    ++pos;
                }
                else if (*pos == '"')
                {
                    quotes = true;
                    ++pos;
                }

                if (!quotes && balance == 0)
                {
                    ++number_of_rows;
                    if ((number_of_rows >= min_rows)
                        && ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows)))
                        need_more_data = false;
                }
            }
        }

        saveUpToPosition(in, memory, pos);
        return {loadAtPosition(in, memory, pos), number_of_rows};
    }

    std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(
        ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
    {
        return fileSegmentationEngineJSONEachRowImpl<'{', '}'>(in, memory, min_bytes, 1, max_rows);
    }

    std::pair<bool, size_t> fileSegmentationEngineJSONCompactEachRow(
        ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows)
    {
        return fileSegmentationEngineJSONEachRowImpl<'[', ']'>(in, memory, min_bytes, min_rows, max_rows);
    }

    template <const char opening_bracket, const char closing_bracket>
    void skipRowForJSONEachRowImpl(ReadBuffer & in)
    {
        size_t balance = 0;
        bool quotes = false;
        while (!in.eof())
        {
            if (quotes)
            {
                auto * pos = find_first_symbols<'\\', '"'>(in.position(), in.buffer().end());
                in.position() = pos;

                if (in.position() > in.buffer().end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
                if (in.position() == in.buffer().end())
                    continue;

                if (*in.position() == '\\')
                {
                    ++in.position();
                    if (!in.eof())
                        ++in.position();
                }
                else if (*in.position() == '"')
                {
                    ++in.position();
                    quotes = false;
                }
            }
            else
            {
                auto * pos = find_first_symbols<opening_bracket, closing_bracket, '\\', '"'>(in.position(), in.buffer().end());
                in.position() = pos;

                if (in.position() > in.buffer().end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
                if (in.position() == in.buffer().end())
                    continue;

                if (*in.position() == opening_bracket)
                {
                    ++balance;
                    ++in.position();
                }
                else if (*in.position() == closing_bracket)
                {
                    --balance;
                    ++in.position();
                }
                else if (*in.position() == '\\')
                {
                    ++in.position();
                    if (!in.eof())
                        ++in.position();
                }
                else if (*in.position() == '"')
                {
                    quotes = true;
                    ++in.position();
                }

                if (balance == 0)
                    return;
            }
        }

        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected eof");

    }

    void skipRowForJSONEachRow(ReadBuffer & in)
    {
        skipRowForJSONEachRowImpl<'{', '}'>(in);
    }

    void skipRowForJSONCompactEachRow(ReadBuffer & in)
    {
        skipRowForJSONEachRowImpl<'[', ']'>(in);
    }

    NamesAndTypesList readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, const FormatSettings & settings, JSONInferenceInfo * inference_info)
    {
        skipWhitespaceIfAny(in);
        assertChar('{', in);
        skipWhitespaceIfAny(in);
        bool first = true;
        NamesAndTypesList names_and_types;
        while (!in.eof() && *in.position() != '}')
        {
            if (!first)
                assertChar(',', in);
            else
                first = false;

            auto name = readFieldName(in, settings.json);
            auto type = tryInferDataTypeForSingleJSONField(in, settings, inference_info);
            names_and_types.emplace_back(name, type);
            skipWhitespaceIfAny(in);
        }

        if (in.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading JSON object");

        assertChar('}', in);
        return names_and_types;
    }

    DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, const FormatSettings & settings, JSONInferenceInfo * inference_info)
    {
        skipWhitespaceIfAny(in);
        assertChar('[', in);
        skipWhitespaceIfAny(in);
        bool first = true;
        DataTypes types;
        while (!in.eof() && *in.position() != ']')
        {
            if (!first)
                assertChar(',', in);
            else
                first = false;
            auto type = tryInferDataTypeForSingleJSONField(in, settings, inference_info);
            types.push_back(std::move(type));
            skipWhitespaceIfAny(in);
        }

        if (in.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading JSON array");

        assertChar(']', in);
        return types;
    }

    bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf)
    {
        /// For JSONEachRow we can safely skip whitespace characters
        skipWhitespaceIfAny(buf);
        return buf.eof() || *buf.position() == '[';
    }

    bool readField(
        ReadBuffer & in,
        IColumn & column,
        const DataTypePtr & type,
        const SerializationPtr & serialization,
        const String & column_name,
        const FormatSettings & format_settings,
        bool yield_strings)
    {
        try
        {
            bool as_nullable = format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type);

            if (yield_strings)
            {
                String str;
                readJSONString(str, in, format_settings.json);

                ReadBufferFromString buf(str);

                if (as_nullable)
                    return SerializationNullable::deserializeNullAsDefaultOrNestedWholeText(column, buf, format_settings, serialization);

                serialization->deserializeWholeText(column, buf, format_settings);
                return true;
            }

            auto deserialize = [as_nullable, &format_settings, &serialization](IColumn & column_, ReadBuffer & buf) -> bool
            {
                if (as_nullable)
                    return SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(column_, buf, format_settings, serialization);

                serialization->deserializeTextJSON(column_, buf, format_settings);
                return true;
            };

            if (format_settings.json.empty_as_default)
                return JSONUtils::deserializeEmpyStringAsDefaultOrNested<bool, false>(column, in, deserialize);
            return deserialize(column, in);
        }
        catch (Exception & e)
        {
            e.addMessage("(while reading the value of key " + column_name + ")");
            throw;
        }
    }

    void writeFieldDelimiter(WriteBuffer & out, size_t new_lines)
    {
        writeChar(',', out);
        writeChar('\n', new_lines, out);
    }

    void writeFieldCompactDelimiter(WriteBuffer & out) { writeCString(", ", out); }

    void writeTitle(const char * title, WriteBuffer & out, size_t indent, const char * after_delimiter)
    {
        writeChar('\t', indent, out);
        writeChar('"', out);
        writeCString(title, out);
        writeCString("\":", out);
        writeCString(after_delimiter, out);
    }

    void writeTitlePretty(const char * title, WriteBuffer & out, size_t indent, const char * after_delimiter)
    {
        writeChar(' ', indent * 4, out);
        writeChar('"', out);
        writeCString(title, out);
        writeCString("\": ", out);
        writeCString(after_delimiter, out);
    }

    void writeObjectStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, "\n");
        writeChar('\t', indent, out);
        writeCString("{\n", out);
    }

    void writeCompactObjectStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, " ");
        writeCString("{", out);
    }

    void writeCompactObjectEnd(WriteBuffer & out)
    {
        writeChar('}', out);
    }

    void writeObjectEnd(WriteBuffer & out, size_t indent)
    {
        writeChar('\n', out);
        writeChar('\t', indent, out);
        writeChar('}', out);
    }

    void writeArrayStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, "\n");
        writeChar('\t', indent, out);
        writeCString("[\n", out);
    }

    void writeCompactArrayStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, " ");
        else
            writeChar('\t', indent, out);
        writeCString("[", out);
    }

    void writeArrayEnd(WriteBuffer & out, size_t indent)
    {
        writeChar('\n', out);
        writeChar('\t', indent, out);
        writeChar(']', out);
    }

    void writeCompactArrayEnd(WriteBuffer & out) { writeChar(']', out); }

    void writeFieldFromColumn(
        const IColumn & column,
        const ISerialization & serialization,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out,
        const std::optional<String> & name,
        size_t indent,
        const char * title_after_delimiter,
        bool pretty_json)
    {
        if (name.has_value())
        {
            if (pretty_json)
            {
                writeTitlePretty(name->data(), out, indent, title_after_delimiter);
            }
            else
            {
                writeTitle(name->data(), out, indent, title_after_delimiter);
            }
        }

        if (yield_strings)
        {
            WriteBufferFromOwnString buf;

            serialization.serializeText(column, row_num, buf, settings);
            writeJSONString(buf.str(), out, settings);
        }
        else
        {
            if (pretty_json)
            {
                serialization.serializeTextJSONPretty(column, row_num, out, settings, indent);
            }
            else
            {
                serialization.serializeTextJSON(column, row_num, out, settings);
            }
        }
    }

    void writeColumns(
        const Columns & columns,
        const Names & names,
        const Serializations & serializations,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out,
        size_t indent)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (i != 0)
                writeFieldDelimiter(out);
            writeFieldFromColumn(*columns[i], *serializations[i], row_num, yield_strings, settings, out, names[i], indent);
        }
    }

    void writeCompactColumns(
        const Columns & columns,
        const Serializations & serializations,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (i != 0)
                writeFieldCompactDelimiter(out);
            writeFieldFromColumn(*columns[i], *serializations[i], row_num, yield_strings, settings, out);
        }
    }

    void writeMetadata(const Names & names, const DataTypes & types, const FormatSettings & settings, WriteBuffer & out)
    {
        writeArrayStart(out, 1, "meta");

        for (size_t i = 0; i < names.size(); ++i)
        {
            writeObjectStart(out, 2);

            writeTitle("name", out, 3, " ");

            /// The field names are pre-escaped to be put into JSON string literal.
            writeChar('"', out);
            writeString(names[i], out);
            writeChar('"', out);

            writeFieldDelimiter(out);
            writeTitle("type", out, 3, " ");
            writeJSONString(types[i]->getName(), out, settings);
            writeObjectEnd(out, 2);

            if (i + 1 < names.size())
                writeFieldDelimiter(out);
        }

        writeArrayEnd(out, 1);
    }

    void writeAdditionalInfo(
        size_t rows,
        size_t rows_before_limit,
        bool applied_limit,
        size_t rows_before_aggregation,
        bool applied_aggregation,
        const Stopwatch & watch,
        const Progress & progress,
        bool write_statistics,
        WriteBuffer & out)
    {
        writeFieldDelimiter(out, 2);
        writeTitle("rows", out, 1, " ");
        writeIntText(rows, out);

        if (applied_limit)
        {
            writeFieldDelimiter(out, 2);
            writeTitle("rows_before_limit_at_least", out, 1, " ");
            writeIntText(rows_before_limit, out);
        }
        if (applied_aggregation)
        {
            writeFieldDelimiter(out, 2);
            writeTitle("rows_before_aggregation", out, 1, " ");
            writeIntText(rows_before_aggregation, out);
        }
        if (write_statistics)
        {
            writeFieldDelimiter(out, 2);
            writeObjectStart(out, 1, "statistics");

            writeTitle("elapsed", out, 2, " ");
            writeText(watch.elapsedSeconds(), out);
            writeFieldDelimiter(out);

            writeTitle("rows_read", out, 2, " ");
            writeText(progress.read_rows.load(), out);
            writeFieldDelimiter(out);

            writeTitle("bytes_read", out, 2, " ");
            writeText(progress.read_bytes.load(), out);

            writeObjectEnd(out, 1);
        }
    }

    void writeException(const String & exception_message, WriteBuffer & out, const FormatSettings & settings, size_t indent)
    {
        writeTitle("exception", out, indent, " ");
        writeJSONString(exception_message, out, settings);
    }

    Strings makeNamesValidJSONStrings(const Strings & names, const FormatSettings & settings, bool validate_utf8)
    {
        Strings result;
        result.reserve(names.size());
        for (const auto & name : names)
        {
            WriteBufferFromOwnString buf;
            if (validate_utf8)
            {
                WriteBufferValidUTF8 validating_buf(buf);
                writeJSONString(name, validating_buf, settings);
            }
            else
                writeJSONString(name, buf, settings);

            result.push_back(buf.str().substr(1, buf.str().size() - 2));
        }
        return result;
    }

    void skipColon(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar(':', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipColon(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar(':', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    String readFieldName(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        skipWhitespaceIfAny(in);
        String field;
        readJSONString(field, in, settings);
        skipColon(in);
        return field;
    }

    bool tryReadFieldName(ReadBuffer & in, String & field, const FormatSettings::JSON & settings)
    {
        skipWhitespaceIfAny(in);
        return tryReadJSONStringInto(field, in, settings) && checkAndSkipColon(in);
    }

    String readStringField(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        skipWhitespaceIfAny(in);
        String value;
        readJSONString(value, in, settings);
        skipWhitespaceIfAny(in);
        return value;
    }

    bool tryReadStringField(ReadBuffer & in, String & value, const FormatSettings::JSON & settings)
    {
        skipWhitespaceIfAny(in);
        if (!tryReadJSONStringInto(value, in, settings))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    void skipArrayStart(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar('[', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipArrayStart(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar('[', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    void skipArrayEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar(']', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipArrayEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar(']', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    void skipObjectStart(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar('{', in);
        skipWhitespaceIfAny(in);
    }

    void skipObjectEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar('}', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipObjectStart(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar('{', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    bool checkAndSkipObjectEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar('}', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    void skipComma(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar(',', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipComma(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar(',', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    std::pair<String, String> readStringFieldNameAndValue(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        auto field_name = readFieldName(in, settings);
        auto field_value = readStringField(in, settings);
        return {field_name, field_value};
    }

    bool tryReadStringFieldNameAndValue(ReadBuffer & in, std::pair<String, String> & field_and_value, const FormatSettings::JSON & settings)
    {
        return tryReadFieldName(in, field_and_value.first, settings) && tryReadStringField(in, field_and_value.second, settings);
    }

    NameAndTypePair readObjectWithNameAndType(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        skipObjectStart(in);
        auto [first_field_name, first_field_value] = readStringFieldNameAndValue(in, settings);
        skipComma(in);
        auto [second_field_name, second_field_value] = readStringFieldNameAndValue(in, settings);

        NameAndTypePair name_and_type;
        if (first_field_name == "name" && second_field_name == "type")
            name_and_type = {first_field_value, DataTypeFactory::instance().get(second_field_value)};
        else if (second_field_name == "name" && first_field_name == "type")
            name_and_type = {second_field_value, DataTypeFactory::instance().get(first_field_value)};
        else
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                R"(Expected two fields "name" and "type" with column name and type, found fields "{}" and "{}")",
                first_field_name,
                second_field_name);
        skipObjectEnd(in);
        return name_and_type;
    }

    bool tryReadObjectWithNameAndType(ReadBuffer & in, NameAndTypePair & name_and_type, const FormatSettings::JSON & settings)
    {
        if (!checkAndSkipObjectStart(in))
            return false;

        std::pair<String, String> first_field_and_value;
        if (!tryReadStringFieldNameAndValue(in, first_field_and_value, settings))
            return false;

        if (!checkAndSkipComma(in))
            return false;

        std::pair<String, String> second_field_and_value;
        if (!tryReadStringFieldNameAndValue(in, second_field_and_value, settings))
            return false;

        if (first_field_and_value.first == "name" && second_field_and_value.first == "type")
        {
            auto type = DataTypeFactory::instance().tryGet(second_field_and_value.second);
            if (!type)
                return false;
            name_and_type = {first_field_and_value.second, type};
        }
        else if (second_field_and_value.first == "name" && first_field_and_value.first == "type")
        {
            auto type = DataTypeFactory::instance().tryGet(first_field_and_value.second);
            if (!type)
                return false;
            name_and_type = {second_field_and_value.second, type};
        }
        else
        {
            return false;
        }

        return checkAndSkipObjectEnd(in);
    }

    NamesAndTypesList readMetadata(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        auto field_name = readFieldName(in, settings);
        if (field_name != "meta")
            throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"meta\" with columns names and types, found field {}", field_name);
        skipArrayStart(in);
        NamesAndTypesList names_and_types;
        bool first = true;
        while (!checkAndSkipArrayEnd(in))
        {
            if (!first)
                skipComma(in);
            else
                first = false;

            names_and_types.push_back(readObjectWithNameAndType(in, settings));
        }
        return names_and_types;
    }

    bool tryReadMetadata(ReadBuffer & in, NamesAndTypesList & names_and_types, const FormatSettings::JSON & settings)
    {
        String field_name;
        if (!tryReadFieldName(in, field_name, settings) || field_name != "meta")
            return false;

        if (!checkAndSkipArrayStart(in))
            return false;

        bool first = true;
        while (!checkAndSkipArrayEnd(in))
        {
            if (!first)
            {
                if (!checkAndSkipComma(in))
                    return false;
            }
            else
            {
                first = false;
            }

            NameAndTypePair name_and_type;
            if (!tryReadObjectWithNameAndType(in, name_and_type, settings))
                return false;
            names_and_types.push_back(name_and_type);
        }

        return !names_and_types.empty();
    }

    void validateMetadataByHeader(const NamesAndTypesList & names_and_types_from_metadata, const Block & header)
    {
        for (const auto & [name, type] : names_and_types_from_metadata)
        {
            if (!header.has(name))
                continue;

            auto header_type = header.getByName(name).type;
            if (!type->equals(*header_type))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Type {} of column '{}' from metadata is not the same as type in header {}",
                    type->getName(), name, header_type->getName());
        }
    }

    NamesAndTypesList readMetadataAndValidateHeader(ReadBuffer & in, const Block & header, const FormatSettings::JSON & settings)
    {
        auto names_and_types = JSONUtils::readMetadata(in, settings);
        validateMetadataByHeader(names_and_types, header);
        return names_and_types;
    }

    bool skipUntilFieldInObject(ReadBuffer & in, const String & desired_field_name, const FormatSettings::JSON & settings)
    {
        while (!checkAndSkipObjectEnd(in))
        {
            auto field_name = JSONUtils::readFieldName(in, settings);
            if (field_name == desired_field_name)
                return true;
        }

        return false;
    }

    void skipTheRestOfObject(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        while (!checkAndSkipObjectEnd(in))
        {
            skipComma(in);
            auto name = readFieldName(in, settings);
            skipWhitespaceIfAny(in);
            skipJSONField(in, name, settings);
        }
    }

    template <typename ReturnType, bool default_column_return_value>
    ReturnType deserializeEmpyStringAsDefaultOrNested(IColumn & column, ReadBuffer & istr, const NestedDeserialize<ReturnType> & deserialize_nested)
    {
        static constexpr auto throw_exception = std::is_same_v<ReturnType, void>;

        static constexpr auto EMPTY_STRING = "\"\"";
        static constexpr auto EMPTY_STRING_LENGTH = std::string_view(EMPTY_STRING).length();

        if (istr.eof() || *istr.position() != EMPTY_STRING[0])
            return deserialize_nested(column, istr);

        auto do_deserialize = [](IColumn & column_, ReadBuffer & buf, auto && check_for_empty_string, auto && deserialize) -> ReturnType
        {
            if (check_for_empty_string(buf))
            {
                column_.insertDefault();
                return ReturnType(default_column_return_value);
            }
            return deserialize(column_, buf);
        };

        if (istr.available() >= EMPTY_STRING_LENGTH)
        {
            /// We have enough data in buffer to check if we have an empty string.
            auto check_for_empty_string = [](ReadBuffer & buf) -> bool
            {
                auto * pos = buf.position();
                if (checkString(EMPTY_STRING, buf))
                    return true;
                buf.position() = pos;
                return false;
            };

            return do_deserialize(column, istr, check_for_empty_string, deserialize_nested);
        }

        /// We don't have enough data in buffer to check if we have an empty string.
        /// Use PeekableReadBuffer to make a checkpoint before checking for an
        /// empty string and rollback if check was failed.

        auto check_for_empty_string = [](ReadBuffer & buf) -> bool
        {
            auto & peekable_buf = assert_cast<PeekableReadBuffer &>(buf);
            peekable_buf.setCheckpoint();
            SCOPE_EXIT(peekable_buf.dropCheckpoint());
            if (checkString(EMPTY_STRING, peekable_buf))
                return true;
            peekable_buf.rollbackToCheckpoint();
            return false;
        };

        auto deserialize_nested_with_check = [&deserialize_nested](IColumn & column_, ReadBuffer & buf) -> ReturnType
        {
            auto & peekable_buf = assert_cast<PeekableReadBuffer &>(buf);
            if constexpr (throw_exception)
                deserialize_nested(column_, peekable_buf);
            else if (!deserialize_nested(column_, peekable_buf))
                return ReturnType(false);

            if (unlikely(peekable_buf.hasUnreadData()))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect state while parsing JSON: PeekableReadBuffer has unread data in own memory: {}", String(peekable_buf.position(), peekable_buf.available()));

            return ReturnType(true);
        };

        PeekableReadBuffer peekable_buf(istr, true);
        return do_deserialize(column, peekable_buf, check_for_empty_string, deserialize_nested_with_check);
    }

    template void deserializeEmpyStringAsDefaultOrNested<void, true>(IColumn & column, ReadBuffer & istr, const NestedDeserialize<void> & deserialize_nested);
    template bool deserializeEmpyStringAsDefaultOrNested<bool, true>(IColumn & column, ReadBuffer & istr, const NestedDeserialize<bool> & deserialize_nested);
    template bool deserializeEmpyStringAsDefaultOrNested<bool, false>(IColumn & column, ReadBuffer & istr, const NestedDeserialize<bool> & deserialize_nested);
}

}
