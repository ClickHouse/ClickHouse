#include <algorithm>
#include <unordered_map>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesCache.h>
#include <Formats/JSONExtractTree.h>
#include <DataTypes/Serializations/SerializationJSON.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <base/scope_guard.h>

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

SerializationJSON::SerializationJSON(
    const std::unordered_map<String, DataTypePtr> & typed_paths_types_,
    const std::unordered_map<String, SerializationPtr> & typed_paths_serializations_,
    const std::unordered_set<String> & paths_to_skip_,
    const std::vector<String> & path_regexps_to_skip_,
    const DataTypePtr & dynamic_type_,
    const SerializationPtr & dynamic_serialization_,
    size_t max_dynamic_paths_)
    : SerializationObject(typed_paths_types_, typed_paths_serializations_, paths_to_skip_, path_regexps_to_skip_, dynamic_type_, dynamic_serialization_)
    , max_dynamic_paths(max_dynamic_paths_)
    , supports_pooling(dynamic_serialization_->supportsPooling()
        && std::ranges::all_of(typed_paths_serializations_, [](const auto & path) { return path.second->supportsPooling(); }))
{
}

SerializationPtr SerializationJSON::create(
    const std::unordered_map<String, DataTypePtr> & typed_paths_types_,
    const std::unordered_map<String, SerializationPtr> & typed_paths_serializations_,
    const std::unordered_set<String> & paths_to_skip_,
    const std::vector<String> & path_regexps_to_skip_,
    const DataTypePtr & dynamic_type_,
    const SerializationPtr & dynamic_serialization_,
    size_t max_dynamic_paths_,
    const SerializationInfoSettings & settings)
{
    auto creator = [&]
    {
        return new SerializationJSON(typed_paths_types_, typed_paths_serializations_, paths_to_skip_,
            path_regexps_to_skip_, dynamic_type_, dynamic_serialization_, max_dynamic_paths_);
    };
    if (!dynamic_serialization_->supportsPooling()
        || !std::ranges::all_of(typed_paths_serializations_, [](const auto & path) { return path.second->supportsPooling(); }))
        return SerializationPtr(creator());

    SipHash hash;
    auto hash_string = [&](const String & value)
    {
        hash.update(value.size());
        hash.update(value);
    };
    hash.update("JSON");
    settings.updateHash(hash);
    hash.update(max_dynamic_paths_);
    hash_string(dynamic_type_->getName());
    hash.update(dynamic_serialization_->getHash());
    Strings sorted_paths;
    sorted_paths.reserve(typed_paths_types_.size());
    for (const auto & [path, _] : typed_paths_types_)
        sorted_paths.push_back(path);
    std::ranges::sort(sorted_paths);
    hash.update(sorted_paths.size());
    for (const auto & path : sorted_paths)
    {
        hash_string(path);
        hash_string(typed_paths_types_.at(path)->getName());
        hash.update(typed_paths_serializations_.at(path)->getHash());
    }
    sorted_paths.assign(paths_to_skip_.begin(), paths_to_skip_.end());
    std::ranges::sort(sorted_paths);
    hash.update(sorted_paths.size());
    for (const auto & path : sorted_paths)
        hash_string(path);
    hash.update(path_regexps_to_skip_.size());
    for (const auto & regexp : path_regexps_to_skip_)
        hash_string(regexp);
    return pooled(hash.get128(), creator);
}

namespace
{

#if USE_RAPIDJSON
using FallbackJSONParser = RapidJSONParser;
#else
using FallbackJSONParser = DummyJSONParser;
#endif

template <typename Parser>
struct JSONParserState
{
    Parser parser;
    std::unique_ptr<JSONExtractTreeNode<Parser>> tree;

    explicit JSONParserState(const DataTypePtr & type) : tree(buildJSONExtractTree<Parser>(type, "JSON serialization")) {}
};

/// Parsers and extraction trees are mutable and expensive to build, so they stay out of the immutable,
/// pooled serialization and are cached per thread instead. A thread-local cache needs no locking.
///
/// Extraction trees capture the query's `session_timezone` and the parser follows `allow_simdjson`,
/// so the cache has the same lifetime as `DataTypesCache`: it is released when the thread starts serving
/// another query context or `session_timezone` changes. Entries are also released when a map reaches
/// `MAX_ELEMENTS` schemas or an object larger than `DBMS_DEFAULT_BUFFER_SIZE` is parsed.
/// An idle thread keeps the state of its last query until then.
class JSONParserStateCache
{
public:
    template <typename Parser>
    struct Entry
    {
        /// Owning the serialization guarantees that its address is not reused by another schema while cached.
        SerializationPtr owner;
        std::unique_ptr<JSONParserState<Parser>> state;
    };

    template <typename Parser>
    struct Pools
    {
        void clear()
        {
            entries.clear();
            last_serialization = nullptr;
            last_state = nullptr;
        }

        std::unordered_map<const ISerialization *, Entry<Parser>> entries;
        const ISerialization * last_serialization = nullptr;
        JSONParserState<Parser> * last_state = nullptr;
    };

    /// Returns whether the current query allows `SimdJSON`.
    bool clearIfQueryContextChanged()
    {
        auto & data_types_cache = getDataTypesCache();
        UInt64 current_version = data_types_cache.getQueryContextVersion();
        if (query_context_version != current_version)
        {
#if USE_SIMDJSON
            simdjson_pools.clear();
#endif
            fallback_pools.clear();
            query_context_version = current_version;
        }
        return data_types_cache.allowSimdJSON();
    }

    template <typename Parser, typename Factory>
    JSONParserState<Parser> & get(Pools<Parser> & pools, const ISerialization & serialization, Factory && factory)
    {
        if (pools.last_serialization == &serialization)
            return *pools.last_state;

        auto it = pools.entries.find(&serialization);
        if (it == pools.entries.end())
            return add(pools, serialization, std::forward<Factory>(factory));
        pools.last_serialization = &serialization;
        pools.last_state = it->second.state.get();
        return *pools.last_state;
    }

    template <typename Parser>
    Pools<Parser> & getPools()
    {
#if USE_SIMDJSON
        if constexpr (std::is_same_v<Parser, SimdJSONParser>)
            return simdjson_pools;
        else
            return fallback_pools;
#else
        return fallback_pools;
#endif
    }

#if USE_SIMDJSON
    Pools<SimdJSONParser> simdjson_pools;
#endif
    Pools<FallbackJSONParser> fallback_pools;

private:
    static constexpr size_t MAX_ELEMENTS = 64;

    template <typename Parser, typename Factory>
    NO_INLINE JSONParserState<Parser> & add(Pools<Parser> & pools, const ISerialization & serialization, Factory && factory)
    {
        if (pools.entries.size() >= MAX_ELEMENTS)
            pools.clear();
        auto [it, _] = pools.entries.emplace(
            &serialization, Entry<Parser>{serialization.shared_from_this(), factory()});
        pools.last_serialization = &serialization;
        pools.last_state = it->second.state.get();
        return *pools.last_state;
    }

    UInt64 query_context_version = 0;
};

JSONParserStateCache & getJSONParserStateCache()
{
    static thread_local JSONParserStateCache cache;
    return cache;
}

}

namespace
{

/// Struct that represents elements of the JSON path.
/// "a.b.c" -> ["a", "b", "c"]
struct PathElements
{
    explicit PathElements(std::string_view path)
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

void writeJSONKey(std::string_view key, WriteBuffer & ostr, const FormatSettings & settings)
{
    if (settings.json.json_type_escape_dots_in_keys)
        writeJSONString(unescapeDotInJSONKey(String(key)), ostr, settings);
    else
        writeJSONString(key, ostr, settings);
}

}

void SerializationJSON::serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, bool pretty, size_t indent) const
{
    const auto & column_object = assert_cast<const ColumnObject &>(column);

    /// We need to convert the set of paths in this row to a JSON object.
    /// To do it, we construct the resulting JSON object by iterating over sorted list of paths in current row.
    /// For example:
    /// b.c, a.b, a.a, b.e, g, h.u.t -> a.a, a.b, b.c, b.e, g, h.u.t -> {"a" : {"a" : ..., "b" : ...}, "b" : {"c" : ..., "e" : ...}, "g" : ..., "h" : {"u" : {"t" : ...}}}.

    if (pretty)
        writeCString("{\n", ostr);
    else
        writeChar('{', ostr);

    /// current_prefix represents the path of the object we are currently serializing keys in.
    Prefix current_prefix;
    for (auto it = ColumnObject::SortedPathsIterator(column_object, row_num); !it.end(); it.next())
    {
        auto path_info = it.getCurrentPathInfo();

        /// When type_json_skip_null_typed_paths is enabled, treat NULL in typed paths
        /// as absence of the path, matching the behavior of dynamic paths.
        if (settings.json.type_json_skip_null_typed_paths
            && path_info.type == ColumnObject::SortedPathsIterator::PathType::TYPED
            && path_info.column->isNullAt(path_info.row))
            continue;

        PathElements path_elements(path_info.path);
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
                    writeChar(settings.json.pretty_print_indent, (indent + prefix_size + objects_to_close - i) * settings.json.pretty_print_indent_multiplier, ostr);
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
                    writeChar(settings.json.pretty_print_indent, (indent + i + 1) * settings.json.pretty_print_indent_multiplier, ostr);
                    writeJSONKey(path_elements.elements[i], ostr, settings);
                    writeCString(": {\n", ostr);
                }
                else
                {
                    writeJSONKey(path_elements.elements[i], ostr, settings);
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
            writeChar(settings.json.pretty_print_indent, (indent + current_prefix.size() + 1) * settings.json.pretty_print_indent_multiplier, ostr);
            writeJSONKey(path_elements.elements.back(), ostr, settings);
            writeCString(": ", ostr);
        }
        else
        {
            writeJSONKey(path_elements.elements.back(), ostr, settings);
            writeCString(":", ostr);
        }

        /// Serialize value of current path.
        if (path_info.type == ColumnObject::SortedPathsIterator::PathType::TYPED)
        {
            if (pretty)
                typed_paths_serializations.at(String(path_info.path))->serializeTextJSONPretty(*path_info.column, path_info.row, ostr, settings, indent + current_prefix.size() + 1);
            else
                typed_paths_serializations.at(String(path_info.path))->serializeTextJSON(*path_info.column, path_info.row, ostr, settings);
        }
        else
        {
            if (pretty)
                dynamic_serialization->serializeTextJSONPretty(*path_info.column, path_info.row, ostr, settings, indent + current_prefix.size() + 1);
            else
                dynamic_serialization->serializeTextJSON(*path_info.column, path_info.row, ostr, settings);
        }
    }

    /// Close all remaining open objects.
    if (pretty)
    {
        writeChar('\n', ostr);
        for (size_t i = 0; i != current_prefix.elements.size(); ++i)
        {
            writeChar(settings.json.pretty_print_indent, (indent + current_prefix.size() - i) * settings.json.pretty_print_indent_multiplier, ostr);
            writeCString("}\n", ostr);
        }
        writeChar(settings.json.pretty_print_indent, indent * settings.json.pretty_print_indent_multiplier, ostr);
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
NO_INLINE void SerializationJSON::deserializeObjectWithParser(IColumn & column, std::string_view object, const FormatSettings & settings) const
{
    auto & cache = getJSONParserStateCache();
    auto & state = cache.get(cache.getPools<Parser>(), *this, [&]
    {
        /// The tree is rebuilt from the type instead of keeping a reference to it: a strong
        /// reference would form a cycle with the serialization cached inside `DataTypeObject`.
        Strings regexps;
        for (const auto & regexp : path_regexps_to_skip)
            regexps.push_back(regexp.pattern());
        auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
            typed_paths_types, paths_to_skip, std::move(regexps), max_dynamic_paths,
            assert_cast<const DataTypeDynamic &>(*dynamic_type).getMaxDynamicTypes());
        return std::make_unique<JSONParserState<Parser>>(type);
    });
    SCOPE_EXIT(
        if (unlikely(object.size() > DBMS_DEFAULT_BUFFER_SIZE))
            cache.getPools<Parser>().clear();
    );
    typename Parser::Element document;
    if (!state.parser.parse(object, document))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON object here: {}{}", object.substr(0, std::min(object.size(), 1000uz)), object.size() > 1000 ? "... (JSON object is too long to display as a whole)" : "");

    String error;
    JSONExtractInsertSettings insert_settings;
    insert_settings.escape_dots_in_json_keys = settings.json.json_type_escape_dots_in_keys;
    insert_settings.skip_invalid_typed_paths = settings.json.type_json_skip_invalid_typed_paths;
    insert_settings.use_partial_match_to_skip_paths_by_regexp = settings.json.type_json_use_partial_match_to_skip_paths_by_regexp;
    if (!state.tree->insertResultToColumn(column, document, insert_settings, settings, error))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot insert data into JSON column: {}", error);
}

void SerializationJSON::deserializeObject(IColumn & column, std::string_view object, const FormatSettings & settings) const
{
    [[maybe_unused]] bool allow_simdjson = getJSONParserStateCache().clearIfQueryContextChanged();
#if USE_SIMDJSON
    if (allow_simdjson)
    {
        deserializeObjectWithParser<SimdJSONParser>(column, object, settings);
        return;
    }
#endif
    deserializeObjectWithParser<FallbackJSONParser>(column, object, settings);
}

void SerializationJSON::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

void SerializationJSON::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    readStringUntilEOF(object, istr);
    deserializeObject(column, object, settings);
}

void SerializationJSON::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    writeEscapedString(buf.str(), ostr);
}

void SerializationJSON::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    readEscapedString(object, istr);
    deserializeObject(column, object, settings);
}

void SerializationJSON::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    if (settings.values.escape_quote_with_quote)
        writeQuotedStringPostgreSQL(buf.str(), ostr);
    else
        writeQuotedString(buf.str(), ostr);
}

void SerializationJSON::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    /// Use SQL-style quoted reader so we accept both `\'` and the SQL-standard `''` apostrophe escapes.
    /// `serializeTextQuoted` above can emit either form depending on `output_format_values_escape_quote_with_quote`,
    /// and a JSON column written by us via `Values` must be parseable back by the same path.
    readQuotedStringWithSQLStyle(object, istr);
    deserializeObject(column, object, settings);
}

void SerializationJSON::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    writeCSVString(buf.str(), ostr);
}

void SerializationJSON::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object;
    readCSVString(object, istr, settings.csv);
    deserializeObject(column, object, settings);
}

void SerializationJSON::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString buf;
    serializeTextImpl(column, row_num, buf, settings);
    writeXMLStringForTextElement(buf.str(), ostr);
}

void SerializationJSON::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr, settings);
}

void SerializationJSON::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    serializeTextImpl(column, row_num, ostr, settings, true, indent);
}

void SerializationJSON::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String object_buffer;
    auto object_view = readJSONObjectAsViewPossiblyInvalid(istr, object_buffer, settings.json.max_row_size_for_json_each_row);
    deserializeObject(column, object_view, settings);
}

}
