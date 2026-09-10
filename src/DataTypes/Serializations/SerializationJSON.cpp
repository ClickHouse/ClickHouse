#include <algorithm>
#include <unordered_map>
#include <Poco/Mutex.h>
#include <base/getThreadId.h>
#include <Common/CacheLine.h>
#include <Common/SipHash.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/ObjectPool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeObject.h>
#include <Formats/JSONExtractTree.h>
#include <Interpreters/Context.h>
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

template <typename Parser>
struct JSONParserState
{
    Parser parser;
    std::unique_ptr<JSONExtractTreeNode<Parser>> tree;

    explicit JSONParserState(const DataTypePtr & type) : tree(buildJSONExtractTree<Parser>(type, "JSON serialization")) {}
};

}

struct JSONParsingPools
{
    /// The pool key owns the serialization, so an address cannot be reused for a different schema.
    /// Each backend has its own pool; the timezone also isolates dynamically inferred types.
    struct Lookup
    {
        const ISerialization * serialization;
        std::string_view session_timezone;
    };
    struct Key
    {
        SerializationPtr serialization;
        String session_timezone;

        explicit Key(Lookup lookup) : serialization(lookup.serialization->shared_from_this()), session_timezone(lookup.session_timezone)
        {
        }
    };
    struct Compare
    {
        using is_transparent = void;
        bool operator()(const auto & lhs, const auto & rhs) const
        {
            auto * lhs_serialization = std::to_address(lhs.serialization);
            auto * rhs_serialization = std::to_address(rhs.serialization);
            if (lhs_serialization != rhs_serialization)
                return std::less<const ISerialization *>{}(lhs_serialization, rhs_serialization);
            return std::string_view(lhs.session_timezone) < std::string_view(rhs.session_timezone);
        }
    };
    /// Exact thread IDs give each OS thread a private shard. Parsing must not suspend between acquisition and lease return.
    /// Restore real pool mutexes if parsing can yield or shards become shared between threads.
    struct alignas(CH_CACHE_LINE_SIZE) Shard
    {
#if USE_SIMDJSON
        ObjectPoolMap<JSONParserState<SimdJSONParser>, Key, Compare, Poco::NullMutex> simdjson;
#endif
#if USE_RAPIDJSON
        ObjectPoolMap<JSONParserState<RapidJSONParser>, Key, Compare, Poco::NullMutex> rapidjson;
#else
        ObjectPoolMap<JSONParserState<DummyJSONParser>, Key, Compare, Poco::NullMutex> dummy;
#endif
    };

    std::mutex mutex;
    /// Shards are never erased while these pools are alive.
    std::unordered_map<UInt64, std::unique_ptr<Shard>> shards;

    static Shard & getShard(const std::shared_ptr<JSONParsingState> & holder)
    {
        /// The caller owns the holder; matching owners keep the cached pointer valid without extending its lifetime.
        static thread_local std::weak_ptr<JSONParsingState> cached_holder;
        static thread_local Shard * cached_shard = nullptr;
        if (cached_holder.owner_before(holder) || holder.owner_before(cached_holder))
        {
            std::call_once(holder->initialization_flag, [&] { holder->pools = std::make_shared<JSONParsingPools>(); });
            auto & pools = holder->pools;
            std::lock_guard lock(pools->mutex);
            auto & shard = pools->shards[getThreadId()];
            if (!shard)
                shard = std::make_unique<Shard>();
            cached_shard = shard.get();
            cached_holder = holder;
        }
        chassert(cached_shard);
        return *cached_shard;
    }
};

namespace Setting
{
    extern const SettingsBool allow_simdjson;
    extern const SettingsTimezone session_timezone;
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

void SerializationJSON::deserializeObject(IColumn & column, std::string_view object, const FormatSettings & settings) const
{
    /// Resolve the context once for both the parser backend and the effective timezone.
    const auto * context = CurrentThread::retainQueryContext();
    ContextPtr global_context;
    if (!context)
    {
        global_context = Context::getGlobalContextInstance();
        context = global_context.get();
    }
    std::string_view session_timezone_name;
    if (context)
        session_timezone_name = context->getSettingsRef()[Setting::session_timezone].value;
    if (session_timezone_name.empty())
        session_timezone_name = DateLUT::serverTimezoneInstance().getTimeZone();

    auto & shard = JSONParsingPools::getShard(settings.json_parsing_state.get());
    JSONParsingPools::Lookup key{this, session_timezone_name};
    auto deserialize = [&]<typename Parser>(ObjectPoolMap<JSONParserState<Parser>, JSONParsingPools::Key, JSONParsingPools::Compare, Poco::NullMutex> & pool)
    {
        auto lease = pool.get(key, [&]
        {
            Strings regexps;
            for (const auto & regexp : path_regexps_to_skip)
                regexps.push_back(regexp.pattern());
            auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON,
                typed_paths_types, paths_to_skip, std::move(regexps), max_dynamic_paths,
                assert_cast<const DataTypeDynamic &>(*dynamic_type).getMaxDynamicTypes());
            return new JSONParserState<Parser>(type);
        });
        typename Parser::Element document;
        if (!lease->parser.parse(object, document))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON object here: {}{}", object.substr(0, std::min(object.size(), 1000uz)), object.size() > 1000 ? "... (JSON object is too long to display as a whole)" : "");

        String error;
        JSONExtractInsertSettings insert_settings;
        insert_settings.escape_dots_in_json_keys = settings.json.json_type_escape_dots_in_keys;
        insert_settings.skip_invalid_typed_paths = settings.json.type_json_skip_invalid_typed_paths;
        insert_settings.use_partial_match_to_skip_paths_by_regexp = settings.json.type_json_use_partial_match_to_skip_paths_by_regexp;
        if (!lease->tree->insertResultToColumn(column, document, insert_settings, settings, error))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot insert data into JSON column: {}", error);
    };
#if USE_SIMDJSON
    if (context->getSettingsRef()[Setting::allow_simdjson])
    {
        deserialize(shard.simdjson);
        return;
    }
#endif
#if USE_RAPIDJSON
    deserialize(shard.rapidjson);
#else
    deserialize(shard.dummy);
#endif
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
