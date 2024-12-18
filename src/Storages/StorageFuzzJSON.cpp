#include <Storages/StorageFuzzJSON.h>

#if USE_SIMDJSON || USE_RAPIDJSON

#include <optional>
#include <random>
#include <string_view>
#include <unordered_set>
#include <Columns/ColumnString.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/checkStackSize.h>
#include <Common/escapeString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
extern const int INCORRECT_DATA;
}

namespace
{

using uniform = std::uniform_int_distribution<size_t>;

struct JSONNode;
using JSONNodeList = std::list<std::shared_ptr<JSONNode>>;

struct JSONValue
{
    enum class Type : size_t
    {
        Fixed = 0,
        Array = 1,
        Object = 2,
    };

    static Type getType(const JSONValue & v);

    // The node value must be one of the following:
    // Examples: 5, true, "abc"
    std::optional<Field> fixed;
    // Examples: [], ["a"], [1, true]
    std::optional<JSONNodeList> array;
    // Examples: {}, {"a": [1,2], "b": "c"}
    std::optional<JSONNodeList> object;
};

JSONValue::Type JSONValue::getType(const JSONValue & v)
{
    if (v.fixed)
    {
        assert(!v.array);
        assert(!v.object);
        return JSONValue::Type::Fixed;
    }
    if (v.array)
    {
        assert(!v.fixed);
        assert(!v.object);
        return JSONValue::Type::Array;
    }
    if (v.object)
    {
        assert(!v.fixed);
        assert(!v.array);
        return JSONValue::Type::Object;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to determine JSON node type.");
}

// A node represents either a JSON field (a key-value pair) or a JSON value.
// The key is not set for the JSON root and for the array items.
struct JSONNode
{
    std::optional<String> key;
    JSONValue value;
};

#if USE_SIMDJSON
using ParserImpl = DB::SimdJSONParser;
#elif USE_RAPIDJSON
using ParserImpl = DB::RapidJSONParser;
#endif

std::optional<Field> getFixedValue(const ParserImpl::Element & e)
{
    return e.isBool()  ? e.getBool()
        : e.isInt64()  ? e.getInt64()
        : e.isUInt64() ? e.getUInt64()
        : e.isDouble() ? e.getDouble()
        : e.isString() ? e.getString()
        : e.isNull()   ? Field()
                       : std::optional<Field>();
}

void traverse(const ParserImpl::Element & e, std::shared_ptr<JSONNode> node)
{
    checkStackSize();

    assert(node);

    auto & val = node->value;
    if (e.isObject())
    {
        const auto & obj = e.getObject();
        if (!val.object)
            val.object = JSONNodeList{};

        for (const auto [k, v] : obj)
        {
            auto child = std::make_shared<JSONNode>();
            child->key = k;
            traverse(v, child);
            val.object->push_back(child);
        }
    }
    else if (e.isArray())
    {
        if (!val.array)
            val.array = JSONNodeList{};

        const auto arr = e.getArray();
        for (const auto a : arr)
        {
            auto child = std::make_shared<JSONNode>();
            traverse(a, child);
            val.array->push_back(child);
        }
    }
    else
    {
        auto field = getFixedValue(e);
        if (!field)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to parse a fixed JSON value '{}'.", escapeString(e.getString()));

        val.fixed = std::move(field);
    }
}

std::shared_ptr<JSONNode> parseJSON(const String & json)
{
    ParserImpl::Element document;
    ParserImpl p;

    if (!p.parse(json, document))
    {
        constexpr size_t max_length = 128LU;
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Failed to parse JSON from the string '{}'{}.",
            escapeString(json.substr(0, max_length)),
            json.size() >= max_length ? "... (truncated)" : "");
    }

    auto root = std::make_shared<JSONNode>();
    traverse(document, root);
    return root;
}

char generateRandomCharacter(pcg64 & rnd, const std::string_view & charset)
{
    assert(!charset.empty());
    auto idx = uniform(0, charset.size() - 1)(rnd);
    return charset[idx];
}

char generateRandomKeyCharacter(pcg64 & rnd)
{
    static constexpr std::string_view charset = "0123456789"
                                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                                "abcdefghijklmnopqrstuvwxyz";
    return generateRandomCharacter(rnd, charset);
}

char generateRandomStringValueCharacter(pcg64 & rnd)
{
    static constexpr std::string_view charset = "0123456789"
                                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                                "abcdefghijklmnopqrstuvwxyz"
                                                "!@#$%^&*-+_";
    return generateRandomCharacter(rnd, charset);
}

String generateRandomStringValue(UInt64 min_length, UInt64 max_length, pcg64 & rnd)
{
    size_t size = min_length + rnd() % (max_length - min_length + 1);
    String res;
    res.reserve(size);
    for (size_t i = 0; i < size; ++i)
        res.push_back(generateRandomStringValueCharacter(rnd));

    return res;
}

String generateRandomKey(UInt64 min_length, UInt64 max_length, pcg64 & rnd)
{
    size_t size = min_length + rnd() % (max_length - min_length + 1);
    String res;
    res.reserve(size);
    for (size_t i = 0; i < size; ++i)
        res.push_back(generateRandomKeyCharacter(rnd));

    return res;
}

enum class FuzzAction : size_t
{
    Skip = 0,
    Edit = 1,
    Add = 2,
    Delete = 3,
};

Field generateRandomFixedValue(const StorageFuzzJSON::Configuration & config, pcg64 & rnd)
{
    // TODO (@jkartseva): support more field types.
    static std::array<Field::Types::Which, 3> possible_types{
        Field::Types::Which::UInt64, Field::Types::Which::String, Field::Types::Which::Bool};

    Field f;
    auto idx = rnd() % possible_types.size();
    switch (possible_types[idx])
    {
        case Field::Types::Which::UInt64: {
            f = rnd();
            break;
        }
        case Field::Types::Which::String:
            f = generateRandomStringValue(/*min_length*/ 0, config.max_string_value_length, rnd);
            break;
        case Field::Types::Which::Bool:
            f = bool(rnd() % 2);
            break;
        default:
    }
    return f;
}

String fuzzString(UInt64 min_length, UInt64 max_length, pcg64 & rnd, const String & source, std::function<char(pcg64 &)> charGen)
{
    String result;
    result.reserve(max_length);

    using FA = FuzzAction;
    auto get_action = [&]() -> FuzzAction
    {
        static constexpr std::array<FuzzAction, 4> actions{FA::Skip, FA::Edit, FA::Add, FA::Delete};
        return actions[uniform(0, 3)(rnd)];
    };

    size_t i = 0;
    while (i < source.size() && result.size() < max_length)
    {
        auto action = get_action();
        switch (action)
        {
            case FA::Skip: {
                result.push_back(source[i++]);
            }
            break;
            case FA::Edit: {
                result.push_back(charGen(rnd));
                ++i;
            }
            break;
            case FA::Add: {
                result.push_back(charGen(rnd));
            }
            break;
            default:
                ++i;
        }
    }

    while (result.size() < min_length)
        result.push_back(charGen(rnd));

    return result;
}

String fuzzJSONKey(const StorageFuzzJSON::Configuration & config, pcg64 & rnd, const String & key)
{
    return fuzzString(config.min_key_length, config.max_key_length, rnd, key, generateRandomKeyCharacter);
}

// Randomly modify structural characters (e.g. '{', '}', '[', ']', ':', '"') to generate output that cannot be parsed as JSON.
String fuzzJSONStructure(const StorageFuzzJSON::Configuration & config, pcg64 & rnd, const String & s)
{
    return config.should_malform_output ? fuzzString(/*min_length*/ 0, /*max_length*/ s.size(), rnd, s, generateRandomStringValueCharacter)
                                        : s;
}

std::shared_ptr<JSONNode>
generateRandomJSONNode(const StorageFuzzJSON::Configuration & config, pcg64 & rnd, bool with_key, JSONValue::Type type)
{
    auto node = std::make_shared<JSONNode>();

    if (with_key)
        node->key = generateRandomKey(config.min_key_length, config.max_key_length, rnd);

    auto & val = node->value;
    switch (type)

    {
        case JSONValue::Type::Fixed: {
            val.fixed = generateRandomFixedValue(config, rnd);
            break;
        }
        case JSONValue::Type::Array: {
            val.array = JSONNodeList{};
            break;
        }
        case JSONValue::Type::Object: {
            val.object = JSONNodeList{};
            break;
        }
    }
    return node;
}

template <size_t n>
std::shared_ptr<JSONNode> generateRandomJSONNode(
    const StorageFuzzJSON::Configuration & config, pcg64 & rnd, bool with_key, const std::array<JSONValue::Type, n> & possible_types)
{
    auto type = possible_types[uniform(0, possible_types.size() - 1)(rnd)];
    return generateRandomJSONNode(config, rnd, with_key, type);
}

std::shared_ptr<JSONNode> generateRandomJSONNode(const StorageFuzzJSON::Configuration & config, pcg64 & rnd, bool with_key, size_t depth)
{
    if (depth >= config.max_nesting_level)
        return generateRandomJSONNode(config, rnd, with_key, JSONValue::Type::Fixed);

    static constexpr std::array<JSONValue::Type, 3> possible_types
        = {JSONValue::Type::Fixed, JSONValue::Type::Array, JSONValue::Type::Object};
    return generateRandomJSONNode(config, rnd, with_key, possible_types);
}

JSONNode & fuzzSingleJSONNode(JSONNode & n, const StorageFuzzJSON::Configuration & config, pcg64 & rnd, size_t depth, size_t & node_count)
{
    auto & val = n.value;

    static constexpr size_t update_key = 1;
    static constexpr size_t update_value = 2;

    auto action = 1 + rnd() % static_cast<size_t>(update_key | update_value);
    if (n.key && (action & update_key))
        n.key = fuzzJSONKey(config, rnd, *n.key);

    if ((action & update_value) == 0)
        return n;

    if (val.fixed)
        val.fixed = generateRandomFixedValue(config, rnd);
    else if (val.array && val.array->size() < config.max_array_size && node_count + val.array->size() < StorageFuzzJSON::Configuration::value_number_limit)
    {
        if (val.array->empty())
            val.array->push_back(generateRandomJSONNode(config, rnd, /*with_key*/ false, depth));
        else
        {
            // Use the type of the preceding element.
            const auto & prev = val.array->back();
            auto value_type = JSONValue::getType(prev->value);
            val.array->push_back(generateRandomJSONNode(config, rnd, /*with_key*/ false, value_type));
        }
        ++node_count;
    }
    else if (val.object && val.object->size() < config.max_object_size && node_count + val.object->size() < StorageFuzzJSON::Configuration::value_number_limit)
    {
        val.object->push_back(generateRandomJSONNode(config, rnd, /*with_key*/ true, depth));
        ++node_count;
    }

    return n;
}


void fuzzJSONObject(
    const std::shared_ptr<JSONNode> & node,
    WriteBuffer & out,
    const StorageFuzzJSON::Configuration & config,
    pcg64 & rnd,
    size_t depth,
    size_t & node_count)
{
    checkStackSize();

    ++node_count;

    bool should_fuzz = rnd() % 100 < 100 * config.probability;

    const auto & next_node = should_fuzz && !config.should_reuse_output ? std::make_shared<JSONNode>(*node) : node;

    if (should_fuzz)
        fuzzSingleJSONNode(*next_node, config, rnd, depth, node_count);

    if (next_node->key)
    {
        writeDoubleQuoted(*next_node->key, out);
        out << fuzzJSONStructure(config, rnd, ":");
    }

    auto & val = next_node->value;

    if (val.fixed)
    {
        if (val.fixed->getType() == Field::Types::Which::String)
        {
            out << fuzzJSONStructure(config, rnd, "\"");
            writeText(val.fixed->safeGet<String>(), out);
            out << fuzzJSONStructure(config, rnd, "\"");
        }
        else
            writeFieldText(*val.fixed, out);
    }
    else
    {
        if (!val.array && !val.object)
            return;

        const auto & [op, cl, node_list] = val.array ? std::make_tuple("[", "]", *val.array) : std::make_tuple("{", "}", *val.object);

        out << fuzzJSONStructure(config, rnd, op);

        bool first = true;
        for (const auto & ptr : node_list)
        {
            if (node_count >= StorageFuzzJSON::Configuration::value_number_limit)
                break;

            WriteBufferFromOwnString child_out;
            if (!first)
                child_out << fuzzJSONStructure(config, rnd, ", ");
            first = false;

            fuzzJSONObject(ptr, child_out, config, rnd, depth + 1, node_count);
            // Should not exceed the maximum length of the output string.
            if (out.count() + child_out.count() >= config.max_output_length)
                break;
            out << child_out.str();
        }
        out << fuzzJSONStructure(config, rnd, cl);
    }
}

void fuzzJSONObject(std::shared_ptr<JSONNode> n, WriteBuffer & out, const StorageFuzzJSON::Configuration & config, pcg64 & rnd)
{
    size_t node_count = 0;
    fuzzJSONObject(n, out, config, rnd, /*depth*/ 0, node_count);
}

class FuzzJSONSource : public ISource
{
public:
    FuzzJSONSource(
        UInt64 block_size_, Block block_header_, const StorageFuzzJSON::Configuration & config_, std::shared_ptr<JSONNode> json_root_)
        : ISource(block_header_)
        , block_size(block_size_)
        , block_header(std::move(block_header_))
        , config(config_)
        , rnd(config.random_seed)
        , json_root(json_root_)
    {
    }
    String getName() const override { return "FuzzJSON"; }

protected:
    Chunk generate() override
    {
        Columns columns;
        columns.reserve(block_header.columns());
        for (const auto & col : block_header)
        {
            chassert(col.type->getTypeId() == TypeIndex::String);
            columns.emplace_back(createColumn());
        }

        return {std::move(columns), block_size};
    }

private:
    ColumnPtr createColumn();

    UInt64 block_size;
    Block block_header;

    StorageFuzzJSON::Configuration config;
    pcg64 rnd;

    std::shared_ptr<JSONNode> json_root;
};

ColumnPtr FuzzJSONSource::createColumn()
{
    auto column = ColumnString::create();
    ColumnString::Chars & data_to = column->getChars();
    ColumnString::Offsets & offsets_to = column->getOffsets();

    offsets_to.resize(block_size);
    IColumn::Offset offset = 0;

    for (size_t row_num = 0; row_num < block_size; ++row_num)
    {
        WriteBufferFromOwnString out;
        fuzzJSONObject(json_root, out, config, rnd);

        auto data = out.str();
        size_t data_len = data.size();

        IColumn::Offset next_offset = offset + data_len + 1;
        data_to.resize(next_offset);

        std::copy(data.begin(), data.end(), &data_to[offset]);

        data_to[offset + data_len] = 0;
        offsets_to[row_num] = next_offset;

        offset = next_offset;
    }

    return column;
}

}

StorageFuzzJSON::StorageFuzzJSON(
    const StorageID & table_id_, const ColumnsDescription & columns_, const String & comment_, const Configuration & config_)
    : IStorage(table_id_), config(config_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageFuzzJSON::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    Pipes pipes;
    pipes.reserve(num_streams);

    const ColumnsDescription & our_columns = storage_snapshot->metadata->getColumns();
    Block block_header;
    for (const auto & name : column_names)
    {
        const auto & name_type = our_columns.get(name);
        MutableColumnPtr column = name_type.type->createColumn();
        block_header.insert({std::move(column), name_type.type, name_type.name});
    }

    for (UInt64 i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<FuzzJSONSource>(max_block_size, block_header, config, parseJSON(config.json_str)));

    return Pipe::unitePipes(std::move(pipes));
}

static constexpr std::array<std::string_view, 14> optional_configuration_keys
    = {"json_str",
       "random_seed",
       "reuse_output",
       "malform_output",
       "probability",
       "max_output_length",
       "max_nesting_level",
       "max_array_size",
       "max_object_size",
       "max_string_value_length",
       "min_key_length",
       "max_key_length"};

void StorageFuzzJSON::processNamedCollectionResult(Configuration & configuration, const NamedCollection & collection)
{
    validateNamedCollection(
        collection,
        std::unordered_set<std::string>(),
        std::unordered_set<std::string>(optional_configuration_keys.begin(), optional_configuration_keys.end()));

    if (collection.has("json_str"))
        configuration.json_str = collection.get<String>("json_str");

    if (collection.has("random_seed"))
        configuration.random_seed = collection.get<UInt64>("random_seed");

    if (collection.has("reuse_output"))
        configuration.should_reuse_output = static_cast<bool>(collection.get<UInt64>("reuse_output"));

    if (collection.has("malform_output"))
        configuration.should_malform_output = static_cast<bool>(collection.get<UInt64>("malform_output"));

    if (collection.has("probability"))
    {
        configuration.probability = collection.get<Float64>("probability");

        if (configuration.probability < 0.0 || configuration.probability > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of the 'probability' argument must be within the interval [0, 1].");
    }

    if (collection.has("max_output_length"))
    {
        configuration.max_output_length = collection.get<UInt64>("max_output_length");

        if (configuration.max_output_length < 2 || configuration.max_output_length > StorageFuzzJSON::Configuration::output_length_limit)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The value of the 'max_output_length' argument must be within the interval [2, {}.]",
                StorageFuzzJSON::Configuration::output_length_limit);
    }

    if (collection.has("max_nesting_level"))
        configuration.max_nesting_level = collection.get<UInt64>("max_nesting_level");

    if (collection.has("max_array_size"))
        configuration.max_array_size = collection.get<UInt64>("max_array_size");

    if (collection.has("max_object_size"))
        configuration.max_object_size = collection.get<UInt64>("max_object_size");

    if (collection.has("max_string_value_length"))
    {
        auto max_string_value_length = collection.get<UInt64>("max_string_value_length");
        if (max_string_value_length > StorageFuzzJSON::Configuration::output_length_limit)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The value of the 'max_string_value_length' argument must be at most {}.",
                StorageFuzzJSON::Configuration::output_length_limit);

        configuration.max_string_value_length = std::min(max_string_value_length, configuration.max_output_length);
    }

    if (collection.has("max_key_length"))
    {
        auto max_key_length = collection.get<UInt64>("max_key_length");
        if (max_key_length > StorageFuzzJSON::Configuration::output_length_limit)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The value of the 'max_key_length' argument must be less or equal than {}.",
                StorageFuzzJSON::Configuration::output_length_limit);
        configuration.max_key_length = std::min(max_key_length, configuration.max_output_length);
        configuration.min_key_length = std::min(configuration.min_key_length, configuration.max_key_length);
    }

    if (collection.has("min_key_length"))
    {
        auto min_key_length = collection.get<UInt64>("min_key_length");
        if (min_key_length == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of the 'min_key_length' argument must be at least 1.");

        if (collection.has("max_key_length") && collection.get<UInt64>("max_key_length") < min_key_length)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The value of the 'min_key_length' argument must be less or equal than "
                "the value of the 'max_key_lenght' argument.");

        configuration.min_key_length = min_key_length;
        configuration.max_key_length = std::max(configuration.max_key_length, configuration.min_key_length);
    }
}

StorageFuzzJSON::Configuration StorageFuzzJSON::getConfiguration(ASTs & engine_args, ContextPtr local_context)
{
    StorageFuzzJSON::Configuration configuration{};

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
    {
        StorageFuzzJSON::processNamedCollectionResult(configuration, *named_collection);
    }
    else
    {
        // Supported signatures:
        //
        // FuzzJSON('json_str')
        // FuzzJSON('json_str', 'random_seed')
        if (engine_args.empty() || engine_args.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "FuzzJSON requires 1 to 2 arguments: "
                "json_str, random_seed");
        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

        auto first_arg = checkAndGetLiteralArgument<String>(engine_args[0], "json_str");
        configuration.json_str = std::move(first_arg);

        if (engine_args.size() == 2)
        {
            const auto & literal = engine_args[1]->as<const ASTLiteral &>();
            if (!literal.value.isNull())
                configuration.random_seed = checkAndGetLiteralArgument<UInt64>(literal, "random_seed");
        }
    }
    return configuration;
}

void registerStorageFuzzJSON(StorageFactory & factory)
{
    factory.registerStorage(
        "FuzzJSON",
        [](const StorageFactory::Arguments & args) -> std::shared_ptr<StorageFuzzJSON>
        {
            ASTs & engine_args = args.engine_args;

            if (engine_args.empty())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage FuzzJSON must have arguments.");

            StorageFuzzJSON::Configuration configuration = StorageFuzzJSON::getConfiguration(engine_args, args.getLocalContext());

            for (const auto& col : args.columns)
                if (col.type->getTypeId() != TypeIndex::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "'StorageFuzzJSON' supports only columns of String type, got {}.", col.type->getName());

            return std::make_shared<StorageFuzzJSON>(args.table_id, args.columns, args.comment, configuration);
        });
}

}
#endif
