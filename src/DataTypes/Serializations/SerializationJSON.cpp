#include <DataTypes/Serializations/SerializationJSON.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#if USE_SIMDJSON
#include <Common/JSONParsers/SimdJSONParser.h>
#endif
#if USE_RAPIDJSON
#include <Common/JSONParsers/RapidJSONParser.h>
#endif
#include <Common/JSONParsers/DummyJSONParser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

template <typename Parser>
SerializationJSON<Parser>::SerializationJSON(
    std::unordered_map<String, SerializationPtr> typed_paths_serializations_,
    const std::unordered_set<String> & paths_to_skip_,
    const std::vector<String> & path_regexps_to_skip_,
    std::unique_ptr<JSONExtractTreeNode<Parser>> json_extract_tree_)
    : SerializationObject(std::move(typed_paths_serializations_), paths_to_skip_, path_regexps_to_skip_)
    , json_extract_tree(std::move(json_extract_tree_))
{
}

namespace
{

/// Struct that represents elements of the JSON path.
/// "a.b.c" -> ["a", "b", "c"]
struct PathElements
{
    explicit PathElements(const String & path)
    {
        const char * start = path.data();
        const char * end = start + path.size();
        const char * pos = start;
        const char * last_dot_pos = pos - 1;
        for (pos = start; pos != end; ++pos)
        {
            if (*pos == '.')
            {
                elements.emplace_back(last_dot_pos + 1, size_t(pos - last_dot_pos - 1));
                last_dot_pos = pos;
            }
        }

        elements.emplace_back(last_dot_pos + 1, size_t(pos - last_dot_pos - 1));
    }

    size_t size() const { return elements.size(); }

    std::vector<std::string_view> elements;
};

/// Struct that represents a prefix of a JSON path. Used during output of the JSON object.
struct Prefix
{
    /// Shrink current prefix to the common prefix of current prefix and specified path.
    /// For example, if current prefix is a.b.c.d and path is a.b.e, then shrink the prefix to a.b.
    void shrinkToCommonPrefix(const PathElements & path_elements)
    {
        /// Don't include last element in path_elements in the prefix.
        size_t i = 0;
        while (i != elements.size() && i != (path_elements.elements.size() - 1) && elements[i].first == path_elements.elements[i])
            ++i;
        elements.resize(i);
    }

    /// Check is_first flag in current object.
    bool isFirstInCurrentObject() const
    {
        if (elements.empty())
            return root_is_first_flag;
        return elements.back().second;
    }

    /// Set flag is_first = false in current object.
    void setNotFirstInCurrentObject()
    {
        if (elements.empty())
            root_is_first_flag = false;
        else
            elements.back().second = false;
    }

    size_t size() const { return elements.size(); }

    /// Elements of the prefix: (path element, is_first flag in this prefix).
    /// is_first flag indicates if we already serialized some key in the object with such prefix.
    std::vector<std::pair<std::string_view, bool>> elements;
    bool root_is_first_flag = true;
};

}

template <typename Parser>
void SerializationJSON<Parser>::serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, bool pretty, size_t indent) const
{
    const auto & column_object = assert_cast<const ColumnObject &>(column);
    const auto & typed_paths = column_object.getTypedPaths();
    const auto & dynamic_paths = column_object.getDynamicPaths();
    const auto & shared_data_offsets = column_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
    size_t shared_data_offset = shared_data_offsets[static_cast<ssize_t>(row_num) - 1];
    size_t shared_data_end = shared_data_offsets[static_cast<ssize_t>(row_num)];

    /// We need to convert the set of paths in this row to a JSON object.
    /// To do it, we first collect all the paths from current row, then we sort them
    /// and construct the resulting JSON object by iterating over sorted list of paths.
    /// For example:
    /// b.c, a.b, a.a, b.e, g, h.u.t -> a.a, a.b, b.c, b.e, g, h.u.t -> {"a" : {"a" : ..., "b" : ...}, "b" : {"c" : ..., "e" : ...}, "g" : ..., "h" : {"u" : {"t" : ...}}}.
    std::vector<String> sorted_paths;
    sorted_paths.reserve(typed_paths.size() + dynamic_paths.size() + (shared_data_end - shared_data_offset));
    for (const auto & [path, _] : typed_paths)
        sorted_paths.emplace_back(path);
    for (const auto & [path, dynamic_column] : dynamic_paths)
    {
        /// We consider null value and absence of the path in a row as equivalent cases, because we cannot actually distinguish them.
        /// So, we don't output null values at all.
        if (!dynamic_column->isNullAt(row_num))
            sorted_paths.emplace_back(path);
    }
    for (size_t i = shared_data_offset; i != shared_data_end; ++i)
    {
        auto path = shared_data_paths->getDataAt(i).toString();
        sorted_paths.emplace_back(path);
    }

    std::sort(sorted_paths.begin(), sorted_paths.end());

    if (pretty)
        writeCString("{\n", ostr);
    else
        writeChar('{', ostr);
    size_t index_in_shared_data_values = shared_data_offset;
    /// current_prefix represents the path of the object we are currently serializing keys in.
    Prefix current_prefix;
    for (const auto & path : sorted_paths)
    {
        PathElements path_elements(path);
        /// Change prefix to common prefix between current prefix and current path.
        /// If prefix changed (it can only decrease), close all finished objects.
        /// For example:
        /// Current prefix: a.b.c.d
        /// Current path: a.b.e.f
        /// It means now we have : {..., "a" : {"b" : {"c" : {"d" : ...
        /// Common prefix will be a.b, so it means we should close objects a.b.c.d and a.b.c: {..., "a" : {"b" : {"c" : {"d" : ...}}
        /// and continue serializing keys in object a.b
        size_t prev_prefix_size = current_prefix.size();
        current_prefix.shrinkToCommonPrefix(path_elements);
        size_t prefix_size = current_prefix.size();
        if (prefix_size != prev_prefix_size)
        {
            size_t objects_to_close = prev_prefix_size - prefix_size;
            if (pretty)
            {
                writeChar('\n', ostr);
                for (size_t i = 0; i != objects_to_close; ++i)
                {
                    writeChar(' ', (indent + prefix_size + objects_to_close - i) * 4, ostr);
                    if (i != objects_to_close - 1)
                        writeCString("}\n", ostr);
                    else
                        writeChar('}', ostr);
                }
            }
            else
            {
                for (size_t i = 0; i != objects_to_close; ++i)
                    writeChar('}', ostr);
            }
        }

        /// Now we are inside object that has common prefix with current path.
        /// We should go inside all objects in current path.
        /// From the example above we should open object a.b.e:
        ///  {..., "a" : {"b" : {"c" : {"d" : ...}}, "e" : {
        if (prefix_size + 1 < path_elements.size())
        {
            for (size_t i = prefix_size; i != path_elements.size() - 1; ++i)
            {
                /// Write comma before the key if it's not the first key in this prefix.
                if (!current_prefix.isFirstInCurrentObject())
                {
                    if (pretty)
                        writeCString(",\n", ostr);
                    else
                        writeChar(',', ostr);
                }
                else
                {
                    current_prefix.setNotFirstInCurrentObject();
                }

                if (pretty)
                {
                    writeChar(' ', (indent + i + 1) * 4, ostr);
                    writeJSONString(path_elements.elements[i], ostr, settings);
                    writeCString(" : {\n", ostr);
                }
                else
                {
                    writeJSONString(path_elements.elements[i], ostr, settings);
                    writeCString(":{", ostr);
                }

                /// Update current prefix.
                current_prefix.elements.emplace_back(path_elements.elements[i], true);
            }
        }

        /// Write comma before the key if it's not the first key in this prefix.
        if (!current_prefix.isFirstInCurrentObject())
        {
            if (pretty)
                writeCString(",\n", ostr);
            else
                writeChar(',', ostr);
        }
        else
        {
            current_prefix.setNotFirstInCurrentObject();
        }

        if (pretty)
        {
            writeChar(' ', (indent + current_prefix.size() + 1) * 4, ostr);
            writeJSONString(path_elements.elements.back(), ostr, settings);
            writeCString(" : ", ostr);
        }
        else
        {
            writeJSONString(path_elements.elements.back(), ostr, settings);
            writeCString(":", ostr);
        }

        /// Serialize value of current path.
        if (auto typed_it = typed_paths.find(path); typed_it != typed_paths.end())
        {
            if (pretty)
                typed_path_serializations.at(path)->serializeTextJSONPretty(*typed_it->second, row_num, ostr, settings, indent + current_prefix.size() + 1);
            else
                typed_path_serializations.at(path)->serializeTextJSON(*typed_it->second, row_num, ostr, settings);
        }
        else if (auto dynamic_it = dynamic_paths.find(path); dynamic_it != dynamic_paths.end())
        {
            if (pretty)
                dynamic_serialization->serializeTextJSONPretty(*dynamic_it->second, row_num, ostr, settings, indent + current_prefix.size() + 1);
            else
                dynamic_serialization->serializeTextJSON(*dynamic_it->second, row_num, ostr, settings);
        }
        else
        {
            /// To serialize value stored in shared data we should first deserialize it from binary format.
            auto tmp_dynamic_column = ColumnDynamic::create();
            tmp_dynamic_column->reserve(1);
            column_object.deserializeValueFromSharedData(shared_data_values, index_in_shared_data_values++, *tmp_dynamic_column);

            if (pretty)
                dynamic_serialization->serializeTextJSONPretty(*tmp_dynamic_column, 0, ostr, settings, indent + current_prefix.size() + 1);
            else
                dynamic_serialization->serializeTextJSON(*tmp_dynamic_column, 0, ostr, settings);
        }
    }

    /// Close all remaining open objects.
    if (pretty)
    {
        writeChar('\n', ostr);
        for (size_t i = 0; i != current_prefix.elements.size(); ++i)
        {
            writeChar(' ', (indent + current_prefix.size() - i) * 4, ostr);
            writeCString("}\n", ostr);
        }
        writeChar(' ', indent * 4, ostr);
        writeChar('}', ostr);
    }
    else
    {
        for (size_t i = 0; i != current_prefix.elements.size(); ++i)
            writeChar('}', ostr);
        writeChar('}', ostr);
    }
}

template <typename Parser>
void SerializationJSON<Parser>::deserializeObject(IColumn & column, std::string_view object, const FormatSettings & settings) const
{
    typename Parser::Element document;
    auto parser = parsers_pool.get([] { return new Parser; });
    if (!parser->parse(object, document))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON object here: {}", object);

    String error;
    if (!json_extract_tree->insertResultToColumn(column, document, insert_settings, settings, error))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot insert data into JSON column: {}", error);
}

template <typename Parser>
void SerializationJSON<Parser>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

template <typename Parser>
void SerializationJSON<Parser>::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    readStringUntilEOF(object, istr);
    deserializeObject(column, object, settings);
}

template <typename Parser>
void SerializationJSON<Parser>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    writeEscapedString(buf.str(), ostr);
}

template <typename Parser>
void SerializationJSON<Parser>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    readEscapedString(object, istr);
    deserializeObject(column, object, settings);
}

template <typename Parser>
void SerializationJSON<Parser>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    writeQuotedString(buf.str(), ostr);
}

template <typename Parser>
void SerializationJSON<Parser>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    readQuotedString(object, istr);
    deserializeObject(column, object, settings);
}

template <typename Parser>
void SerializationJSON<Parser>::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    writeCSVString(buf.str(), ostr);
}

template <typename Parser>
void SerializationJSON<Parser>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    readCSVString(object, istr, settings.csv);
    deserializeObject(column, object, settings);
}

template <typename Parser>
void SerializationJSON<Parser>::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    writeXMLStringForTextElement(buf.str(), ostr);
}

template <typename Parser>
void SerializationJSON<Parser>::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

template <typename Parser>
void SerializationJSON<Parser>::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    serializeTextImpl(column, row_num, ostr, settings, true, indent);
}

template <typename Parser>
void SerializationJSON<Parser>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object_buffer;
    auto object_view = readJSONObjectAsViewPossiblyInvalid(istr, object_buffer);
    deserializeObject(column, object_view, settings);
}

#if USE_SIMDJSON
template class SerializationJSON<SimdJSONParser>;
#endif
#if USE_RAPIDJSON
template class SerializationJSON<RapidJSONParser>;
#else
template class SerializationJSON<DummyJSONParser>;
#endif

}
