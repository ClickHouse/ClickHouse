#include <IO/ReadHelpers.h>
#include <Formats/JSONUtils.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/JSONParsers/DummyJSONParser.h>

#include <base/find_symbols.h>

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
                throw ParsingException(
                    "Size of JSON object is extremely large. Expected not greater than " + std::to_string(min_bytes)
                        + " bytes, but current is " + std::to_string(current_object_size)
                        + " bytes per row. Increase the value setting 'min_chunk_bytes_for_parallel_parsing' or check your data manually, most likely JSON is malformed",
                    ErrorCodes::INCORRECT_DATA);

            if (quotes)
            {
                pos = find_first_symbols<'\\', '"'>(pos, in.buffer().end());

                if (pos > in.buffer().end())
                    throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
                else if (pos == in.buffer().end())
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
                pos = find_first_symbols<opening_bracket, closing_bracket, '\\', '"'>(pos, in.buffer().end());

                if (pos > in.buffer().end())
                    throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
                else if (pos == in.buffer().end())
                    continue;

                else if (*pos == opening_bracket)
                {
                    ++balance;
                    ++pos;
                }
                else if (*pos == closing_bracket)
                {
                    --balance;
                    ++pos;
                }
                else if (*pos == '\\')
                {
                    ++pos;
                    if (loadAtPosition(in, memory, pos))
                        ++pos;
                }
                else if (*pos == '"')
                {
                    quotes = true;
                    ++pos;
                }

                if (balance == 0)
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

    template <const char opening_bracket, const char closing_bracket>
    static String readJSONEachRowLineIntoStringImpl(ReadBuffer & in)
    {
        Memory memory;
        fileSegmentationEngineJSONEachRowImpl<opening_bracket, closing_bracket>(in, memory, 0, 1, 1);
        return String(memory.data(), memory.size());
    }

    template <class Element>
    DataTypePtr getDataTypeFromFieldImpl(const Element & field, const FormatSettings & settings, std::unordered_set<const IDataType *> & numbers_parsed_from_json_strings)
    {
        if (field.isNull())
            return nullptr;

        if (field.isBool())
            return DataTypeFactory::instance().get("Nullable(Bool)");

        if (field.isInt64() || field.isUInt64())
        {
            if (settings.try_infer_integers)
                return makeNullable(std::make_shared<DataTypeInt64>());

            return makeNullable(std::make_shared<DataTypeFloat64>());
        }

        if (field.isDouble())
            return makeNullable(std::make_shared<DataTypeFloat64>());

        if (field.isString())
        {
            if (auto date_type = tryInferDateOrDateTime(field.getString(), settings))
                return date_type;

            if (!settings.json.try_infer_numbers_from_strings)
                return makeNullable(std::make_shared<DataTypeString>());

            ReadBufferFromString buf(field.getString());

            if (settings.try_infer_integers)
            {
                Int64 tmp_int;
                if (tryReadIntText(tmp_int, buf) && buf.eof())
                {
                    auto type = std::make_shared<DataTypeInt64>();
                    numbers_parsed_from_json_strings.insert(type.get());
                    return makeNullable(type);
                }
            }

            Float64 tmp;
            if (tryReadFloatText(tmp, buf) && buf.eof())
            {
                auto type = std::make_shared<DataTypeFloat64>();
                numbers_parsed_from_json_strings.insert(type.get());
                return makeNullable(type);
            }

            return makeNullable(std::make_shared<DataTypeString>());
        }

        if (field.isArray())
        {
            auto array = field.getArray();

            /// Return nullptr in case of empty array because we cannot determine nested type.
            if (array.size() == 0)
                return nullptr;

            DataTypes nested_data_types;
            /// If this array contains fields with different types we will treat it as Tuple.
            bool are_types_the_same = true;
            for (const auto element : array)
            {
                auto type = getDataTypeFromFieldImpl(element, settings, numbers_parsed_from_json_strings);
                if (!type)
                    return nullptr;

                if (!nested_data_types.empty() && !type->equals(*nested_data_types.back()))
                    are_types_the_same = false;

                nested_data_types.push_back(std::move(type));
            }

            if (!are_types_the_same)
            {
                auto nested_types_copy = nested_data_types;
                transformInferredJSONTypesIfNeeded(nested_types_copy, settings, &numbers_parsed_from_json_strings);
                are_types_the_same = true;
                for (size_t i = 1; i < nested_types_copy.size(); ++i)
                    are_types_the_same &= nested_types_copy[i]->equals(*nested_types_copy[i - 1]);

                if (are_types_the_same)
                    nested_data_types = std::move(nested_types_copy);
            }

            if (!are_types_the_same)
                return std::make_shared<DataTypeTuple>(nested_data_types);

            return std::make_shared<DataTypeArray>(nested_data_types.back());
        }

        if (field.isObject())
        {
            auto object = field.getObject();
            DataTypes value_types;
            for (const auto key_value_pair : object)
            {
                auto type = getDataTypeFromFieldImpl(key_value_pair.second, settings, numbers_parsed_from_json_strings);
                if (!type)
                {
                    /// If we couldn't infer nested type and Object type is not enabled,
                    /// we can't determine the type of this JSON field.
                    if (!settings.json.try_infer_objects)
                    {
                        /// If read_objects_as_strings is enabled, we can read objects into strings.
                        if (settings.json.read_objects_as_strings)
                            return makeNullable(std::make_shared<DataTypeString>());
                        return nullptr;
                    }

                    continue;
                }

                if (settings.json.try_infer_objects && isObject(type))
                    return std::make_shared<DataTypeObject>("json", true);

                value_types.push_back(type);
            }

            if (value_types.empty())
                return nullptr;

            transformInferredJSONTypesIfNeeded(value_types, settings, &numbers_parsed_from_json_strings);
            bool are_types_equal = true;
            for (size_t i = 1; i < value_types.size(); ++i)
                are_types_equal &= value_types[i]->equals(*value_types[0]);

            if (!are_types_equal)
            {
                if (!settings.json.try_infer_objects)
                {
                    /// If read_objects_as_strings is enabled, we can read objects into strings.
                    if (settings.json.read_objects_as_strings)
                        return makeNullable(std::make_shared<DataTypeString>());
                    return nullptr;
                }
                return std::make_shared<DataTypeObject>("json", true);
            }

            return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), value_types[0]);
        }

        throw Exception{ErrorCodes::INCORRECT_DATA, "Unexpected JSON type"};
    }

    auto getJSONParserAndElement()
    {
#if USE_SIMDJSON
        return std::pair<SimdJSONParser, SimdJSONParser::Element>();
#elif USE_RAPIDJSON
        return std::pair<RapidJSONParser, RapidJSONParser::Element>();
#else
        return std::pair<DummyJSONParser, DummyJSONParser::Element>();
#endif
    }

    DataTypePtr getDataTypeFromField(const String & field, const FormatSettings & settings)
    {
        auto [parser, element] = getJSONParserAndElement();
        bool parsed = parser.parse(field, element);
        if (!parsed)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON object here: {}", field);

        std::unordered_set<const IDataType *> numbers_parsed_from_json_strings;
        return getDataTypeFromFieldImpl(element, settings, numbers_parsed_from_json_strings);
    }

    template <class Extractor, const char opening_bracket, const char closing_bracket>
    static DataTypes determineColumnDataTypesFromJSONEachRowDataImpl(ReadBuffer & in, const FormatSettings & settings, bool /*json_strings*/, Extractor & extractor)
    {
        String line = readJSONEachRowLineIntoStringImpl<opening_bracket, closing_bracket>(in);
        auto [parser, element] = getJSONParserAndElement();
        bool parsed = parser.parse(line, element);
        if (!parsed)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON object here: {}", line);

        auto fields = extractor.extract(element);

        DataTypes data_types;
        data_types.reserve(fields.size());
        std::unordered_set<const IDataType *> numbers_parsed_from_json_strings;
        for (const auto & field : fields)
            data_types.push_back(getDataTypeFromFieldImpl(field, settings, numbers_parsed_from_json_strings));

        /// TODO: For JSONStringsEachRow/JSONCompactStringsEach all types will be strings.
        ///       Should we try to parse data inside strings somehow in this case?

        return data_types;
    }

    std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
    {
        return fileSegmentationEngineJSONEachRowImpl<'{', '}'>(in, memory, min_bytes, 1, max_rows);
    }

    std::pair<bool, size_t>
    fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t min_rows, size_t max_rows)
    {
        return fileSegmentationEngineJSONEachRowImpl<'[', ']'>(in, memory, min_bytes, min_rows, max_rows);
    }

    struct JSONEachRowFieldsExtractor
    {
        template <class Element>
        std::vector<Element> extract(const Element & element)
        {
            /// {..., "<column_name>" : <value>, ...}

            if (!element.isObject())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Root JSON value is not an object");

            auto object = element.getObject();
            std::vector<Element> fields;
            fields.reserve(object.size());
            column_names.reserve(object.size());
            for (const auto & key_value_pair : object)
            {
                column_names.emplace_back(key_value_pair.first);
                fields.push_back(key_value_pair.second);
            }

            return fields;
        }

        std::vector<String> column_names;
    };

    NamesAndTypesList readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, const FormatSettings & settings, bool json_strings)
    {
        JSONEachRowFieldsExtractor extractor;
        auto data_types
            = determineColumnDataTypesFromJSONEachRowDataImpl<JSONEachRowFieldsExtractor, '{', '}'>(in, settings, json_strings, extractor);
        NamesAndTypesList result;
        for (size_t i = 0; i != extractor.column_names.size(); ++i)
            result.emplace_back(extractor.column_names[i], data_types[i]);
        return result;
    }

    struct JSONCompactEachRowFieldsExtractor
    {
        template <class Element>
        std::vector<Element> extract(const Element & element)
        {
            /// [..., <value>, ...]
            if (!element.isArray())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Root JSON value is not an array");

            auto array = element.getArray();
            std::vector<Element> fields;
            fields.reserve(array.size());
            for (size_t i = 0; i != array.size(); ++i)
                fields.push_back(array[i]);
            return fields;
        }
    };

    DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, const FormatSettings & settings, bool json_strings)
    {
        JSONCompactEachRowFieldsExtractor extractor;
        return determineColumnDataTypesFromJSONEachRowDataImpl<JSONCompactEachRowFieldsExtractor, '[', ']'>(in, settings, json_strings, extractor);
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
            bool as_nullable = format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable();

            if (yield_strings)
            {
                String str;
                readJSONString(str, in);

                ReadBufferFromString buf(str);

                if (as_nullable)
                    return SerializationNullable::deserializeWholeTextImpl(column, buf, format_settings, serialization);

                serialization->deserializeWholeText(column, buf, format_settings);
                return true;
            }

            if (as_nullable)
                return SerializationNullable::deserializeTextJSONImpl(column, in, format_settings, serialization);

            serialization->deserializeTextJSON(column, in, format_settings);
            return true;
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
        const char * title_after_delimiter)
    {
        if (name.has_value())
            writeTitle(name->data(), out, indent, title_after_delimiter);

        if (yield_strings)
        {
            WriteBufferFromOwnString buf;

            serialization.serializeText(column, row_num, buf, settings);
            writeJSONString(buf.str(), out, settings);
        }
        else
            serialization.serializeTextJSON(column, row_num, out, settings);
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

    String readFieldName(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        String field;
        readJSONString(field, in);
        skipColon(in);
        return field;
    }

    String readStringField(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        String value;
        readJSONString(value, in);
        skipWhitespaceIfAny(in);
        return value;
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

    std::pair<String, String> readStringFieldNameAndValue(ReadBuffer & in)
    {
        auto field_name = readFieldName(in);
        auto field_value = readStringField(in);
        return {field_name, field_value};
    }

    NameAndTypePair readObjectWithNameAndType(ReadBuffer & in)
    {
        skipObjectStart(in);
        auto [first_field_name, first_field_value] = readStringFieldNameAndValue(in);
        skipComma(in);
        auto [second_field_name, second_field_value] = readStringFieldNameAndValue(in);

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

    NamesAndTypesList readMetadata(ReadBuffer & in)
    {
        auto field_name = readFieldName(in);
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

            names_and_types.push_back(readObjectWithNameAndType(in));
        }
        return names_and_types;
    }

    NamesAndTypesList readMetadataAndValidateHeader(ReadBuffer & in, const Block & header)
    {
        auto names_and_types = JSONUtils::readMetadata(in);
        for (const auto & [name, type] : names_and_types)
        {
            auto header_type = header.getByName(name).type;
            if (header.has(name) && !type->equals(*header_type))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Type {} of column '{}' from metadata is not the same as type in header {}", type->getName(), name, header_type->getName());
        }
        return names_and_types;
    }

    bool skipUntilFieldInObject(ReadBuffer & in, const String & desired_field_name)
    {
        while (!checkAndSkipObjectEnd(in))
        {
            auto field_name = JSONUtils::readFieldName(in);
            if (field_name == desired_field_name)
                return true;
        }

        return false;
    }

    void skipTheRestOfObject(ReadBuffer & in)
    {
        while (!checkAndSkipObjectEnd(in))
        {
            skipComma(in);
            auto name = readFieldName(in);
            skipWhitespaceIfAny(in);
            skipJSONField(in, name);
        }
    }

}

}
