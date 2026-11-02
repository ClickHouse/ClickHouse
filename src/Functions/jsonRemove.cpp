#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathMemberAccess.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathRange.h>
#include <Functions/JSONPath/ASTs/ASTJSONPathRoot.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Interpreters/Context.h>
#include <Parsers/Lexer.h>
#include <Common/JSONParsers/RapidJSONMemoryTrackerAllocator.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include "config.h"

#include <string_view>
#include <utility>
#include <variant>

#if USE_RAPIDJSON

#define RAPIDJSON_PARSE_DEFAULT_FLAGS (kParseIterativeFlag)

#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
using TrackedReader = rapidjson::GenericReader<rapidjson::UTF8<char>, rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;

struct PathStep
{
    enum class Type
    {
        Member,
        Index,
    };

    Type type;
    String member_name;
    UInt32 index = 0;
};

using ParsedPath = VectorWithMemoryTracking<PathStep>;
using ParsedPaths = VectorWithMemoryTracking<ParsedPath>;
using MemberNameLookup = UnorderedMapWithMemoryTracking<std::string_view, const String *>;

struct JSONSlice
{
    size_t offset = 0;
    size_t length = 0;
};

using JSONNodeIndex = size_t;

struct JSONObjectMember
{
    /// Points to a constant JSONPath name, or null if no path selects this key.
    const String * name = nullptr;
    JSONSlice raw_name;
    JSONNodeIndex value;
};

using JSONObjectMembers = VectorWithMemoryTracking<JSONObjectMember>;
using JSONArrayElements = VectorWithMemoryTracking<JSONNodeIndex>;

/// A node holds only the contents of its JSON type, avoiding two empty vectors for every scalar.
struct JSONNode
{
    using Value = std::variant<JSONSlice, JSONObjectMembers, JSONArrayElements>;
    Value value;
};

using JSONNodes = VectorWithMemoryTracking<JSONNode>;
using JSONNodeIndices = VectorWithMemoryTracking<JSONNodeIndex>;

struct JSONPathTraversalScratch
{
    JSONNodeIndices current;
    JSONNodeIndices next;
};

/// RapidJSON keeps an optimized local copy of MemoryStream while parsing strings and numbers,
/// so a SAX handler cannot observe its live cursor. This custom stream uses RapidJSON's default
/// reference semantics and remembers the start of each string token, letting the handler point
/// nodes directly at slices of the original JSON.
class JSONInputStream
{
public:
    using Ch = char;

    explicit JSONInputStream(std::string_view data_)
        : data(data_)
    {
    }

    void reset(std::string_view data_)
    {
        data = data_;
        position = 0;
        last_string_start = 0;
        in_string = false;
        escaped = false;
        in_exponent = false;
    }

    /// Even with kParseNumbersAsStringsFlag, RapidJSON rejects valid exponents above
    /// the double range. Give its parser zero exponent digits, while retaining the
    /// original bytes and offsets for JSONTreeBuilder's raw-number slices.
    Ch Peek() const
    {
        if (position == data.size())
            return '\0';

        const Ch value = data[position];
        return maskExponentDigit(value);
    }

    Ch Take()
    {
        if (position == data.size())
            return '\0';

        const size_t offset = position;
        const Ch value = data[position++];
        const Ch masked_value = maskExponentDigit(value);
        if (in_string)
        {
            if (escaped)
                escaped = false;
            else if (value == '\\')
                escaped = true;
            else if (value == '"')
                in_string = false;
        }
        else if (value == '"')
        {
            in_string = true;
            in_exponent = false;
            last_string_start = offset;
        }
        else if (in_exponent)
        {
            if (!isDigit(value) && value != '+' && value != '-')
                in_exponent = false;
        }
        else if (value == 'e' || value == 'E')
        {
            in_exponent = true;
        }

        return masked_value;
    }

    size_t Tell() const { return position; }
    size_t getLastStringStart() const { return last_string_start; }

    Ch * PutBegin() { return nullptr; }
    void Put(Ch) { }
    size_t PutEnd(Ch *) { return 0; }

private:
    static bool isDigit(Ch value) { return value >= '0' && value <= '9'; }
    Ch maskExponentDigit(Ch value) const { return in_exponent && isDigit(value) ? '0' : value; }

    std::string_view data;
    size_t position = 0;
    size_t last_string_start = 0;
    bool in_string = false;
    bool escaped = false;
    bool in_exponent = false;
};

class JSONTreeBuilder : public rapidjson::BaseReaderHandler<rapidjson::UTF8<char>, JSONTreeBuilder>
{
public:
    JSONTreeBuilder(JSONInputStream & stream_, const MemberNameLookup & member_names_)
        : stream(stream_), member_names(member_names_)
    {
    }

    void reset()
    {
        stack.clear();
        nodes.clear();
        root = 0;
    }

    bool Null()
    {
        addScalarFromEnd(4);
        return true;
    }

    bool Bool(bool value)
    {
        addScalarFromEnd(value ? 4 : 5);
        return true;
    }

    bool RawNumber(const char *, rapidjson::SizeType length, bool)
    {
        addScalarFromEnd(length);
        return true;
    }

    bool String(const char *, rapidjson::SizeType, bool)
    {
        addNode(getRawStringSlice());
        return true;
    }

    bool StartObject()
    {
        const auto node = addNode(JSONObjectMembers{});
        stack.push_back({node, {}, {}});
        return true;
    }

    bool Key(const char * value, rapidjson::SizeType length, bool)
    {
        auto & frame = stack.back();
        /// RapidJSON already decoded the key; intern only keys referenced by JSONPaths.
        const auto it = member_names.find(std::string_view(value, length));
        frame.member_name = it == member_names.end() ? nullptr : it->second;
        frame.raw_member_name = getRawStringSlice();
        return true;
    }

    bool EndObject(rapidjson::SizeType)
    {
        stack.pop_back();
        return true;
    }

    bool StartArray()
    {
        const auto node = addNode(JSONArrayElements{});
        stack.push_back({node, {}, {}});
        return true;
    }

    bool EndArray(rapidjson::SizeType)
    {
        stack.pop_back();
        return true;
    }

    JSONNodes & getNodes() { return nodes; }
    JSONNodeIndex getRoot() const { return root; }

private:
    struct ContainerFrame
    {
        JSONNodeIndex node;
        const ::String * member_name = nullptr;
        JSONSlice raw_member_name;
    };

    using ContainerStack = VectorWithMemoryTracking<ContainerFrame>;

    JSONSlice getRawStringSlice() const
    {
        const size_t end_offset = stream.Tell();
        const size_t start_offset = stream.getLastStringStart();
        return {start_offset, end_offset - start_offset};
    }

    void addScalarFromEnd(size_t length)
    {
        const size_t end_offset = stream.Tell();
        addNode(JSONSlice{end_offset - length, length});
    }

    JSONNodeIndex addNode(JSONNode::Value value)
    {
        nodes.push_back({std::move(value)});

        const auto node_index = nodes.size() - 1;
        if (stack.empty())
        {
            root = node_index;
            return node_index;
        }

        auto & frame = stack.back();
        auto & parent = nodes[frame.node];
        if (auto * members = std::get_if<JSONObjectMembers>(&parent.value))
        {
            members->push_back({frame.member_name, frame.raw_member_name, node_index});
            frame.member_name = nullptr;
        }
        else
        {
            std::get<JSONArrayElements>(parent.value).push_back(node_index);
        }

        return node_index;
    }

    JSONInputStream & stream;
    const MemberNameLookup & member_names;
    JSONNodes nodes;
    ContainerStack stack;
    JSONNodeIndex root = 0;
};

ParsedPath parseJSONPath(const String & path, uint32_t parse_depth, uint32_t parse_backtracks)
{
    Tokens tokens(path.data(), path.data() + path.size());
    IParser::Pos token_iterator(tokens, parse_depth, parse_backtracks);

    Expected expected;
    ASTPtr ast;
    ParserJSONPath parser;
    if (!parser.parse(token_iterator, ast, expected))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse JSONPath '{}' for function JSONRemove", path);

    const auto * json_path = ast ? ast->as<ASTJSONPath>() : nullptr;
    if (!json_path || !json_path->jsonpath_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid JSONPath for function JSONRemove: {}", path);

    const auto & children = json_path->jsonpath_query->children;
    if (children.empty() || !typeid_cast<const ASTJSONPathRoot *>(children.front().get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid JSONPath for function JSONRemove: {}", path);

    ParsedPath result;
    result.reserve(children.size() - 1);

    for (size_t i = 1; i < children.size(); ++i)
    {
        const auto * member = typeid_cast<const ASTJSONPathMemberAccess *>(children[i].get());
        if (member)
        {
            result.push_back({PathStep::Type::Member, member->member_name, 0});
            continue;
        }

        const auto * range = typeid_cast<const ASTJSONPathRange *>(children[i].get());
        if (range && !range->is_star && range->ranges.size() == 1)
        {
            const auto [begin, end] = range->ranges.front();
            if (end > begin && end - begin == 1)
            {
                result.push_back({PathStep::Type::Index, {}, begin});
                continue;
            }
        }

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "JSONPath '{}' must select one object member or one array "
            "element for function JSONRemove",
            path);
    }

    if (result.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSONPath '{}' cannot remove the root value", path);

    return result;
}

void collectNextNodes(
    const JSONNodes & nodes,
    const JSONNodeIndices & parent_indices,
    const PathStep & step,
    JSONNodeIndices & next_parent_indices)
{
    next_parent_indices.clear();

    for (const auto parent_index : parent_indices)
    {
        const auto & parent = nodes[parent_index];
        if (step.type == PathStep::Type::Member)
        {
            if (const auto * members = std::get_if<JSONObjectMembers>(&parent.value))
            {
                for (const auto & member : *members)
                {
                    if (member.name && *member.name == step.member_name)
                        next_parent_indices.push_back(member.value);
                }
            }
        }
        else if (const auto * elements = std::get_if<JSONArrayElements>(&parent.value);
                 elements && step.index < elements->size())
        {
            next_parent_indices.push_back((*elements)[step.index]);
        }
    }
}

void removePathStep(JSONNode & parent, const PathStep & step)
{
    if (step.type == PathStep::Type::Member)
    {
        if (auto * members = std::get_if<JSONObjectMembers>(&parent.value))
            std::erase_if(*members, [&](const auto & member) { return member.name && *member.name == step.member_name; });
        return;
    }

    auto * elements = std::get_if<JSONArrayElements>(&parent.value);
    if (elements && step.index < elements->size())
        elements->erase(elements->begin() + step.index);
}

void removeAtPath(
    JSONNodes & nodes,
    JSONNodeIndex root,
    const ParsedPath & path,
    JSONPathTraversalScratch & scratch)
{
    auto & parent_indices = scratch.current;
    auto & next_parent_indices = scratch.next;
    parent_indices.clear();
    next_parent_indices.clear();
    parent_indices.push_back(root);

    for (size_t i = 0; i + 1 < path.size(); ++i)
    {
        collectNextNodes(nodes, parent_indices, path[i], next_parent_indices);
        if (next_parent_indices.empty())
            return;

        parent_indices.swap(next_parent_indices);
    }

    for (const auto parent_index : parent_indices)
        removePathStep(nodes[parent_index], path.back());
}

void appendSlice(ColumnString::Chars & output, std::string_view json, JSONSlice slice)
{
    const auto * begin = reinterpret_cast<const UInt8 *>(json.data() + slice.offset);
    output.insert(begin, begin + slice.length);
}

struct JSONSerializationFrame
{
    JSONNodeIndex node;
    size_t next_child = 0;
    bool started = false;
};

using JSONSerializationStack = VectorWithMemoryTracking<JSONSerializationFrame>;

void serializeJSON(
    const JSONNodes & nodes,
    JSONNodeIndex node_index,
    std::string_view json,
    ColumnString::Chars & output,
    JSONSerializationStack & stack)
{
    /// Use an explicit stack so deeply nested JSON cannot overflow the C++ call stack.
    stack.clear();
    stack.push_back({node_index});

    while (!stack.empty())
    {
        auto & frame = stack.back();
        const auto & node = nodes[frame.node];

        if (const auto * slice = std::get_if<JSONSlice>(&node.value))
        {
            appendSlice(output, json, *slice);
            stack.pop_back();
            continue;
        }

        const auto * members = std::get_if<JSONObjectMembers>(&node.value);
        if (!frame.started)
        {
            output.push_back(members ? '{' : '[');
            frame.started = true;
        }

        if (members)
        {
            if (frame.next_child == members->size())
            {
                output.push_back('}');
                stack.pop_back();
                continue;
            }

            if (frame.next_child)
                output.push_back(',');

            const auto & member = (*members)[frame.next_child++];
            appendSlice(output, json, member.raw_name);
            output.push_back(':');
            stack.push_back({member.value});
            continue;
        }

        const auto & elements = std::get<JSONArrayElements>(node.value);
        if (frame.next_child == elements.size())
        {
            output.push_back(']');
            stack.pop_back();
            continue;
        }

        if (frame.next_child)
            output.push_back(',');

        stack.push_back({elements[frame.next_child++]});
    }
}

/// Retains parser, tree, traversal, and serialization buffers across all rows in a block.
class JSONRemoveProcessor
{
public:
    explicit JSONRemoveProcessor(const ParsedPaths & paths_)
        : paths(paths_)
        , stream(std::string_view{})
        , builder(stream, member_names)
    {
        /// Constant paths outlive the processor, so matched member names need no per-row copies.
        for (const auto & path : paths)
            for (const auto & step : path)
                if (step.type == PathStep::Type::Member)
                    member_names.emplace(step.member_name, &step.member_name);
    }

    /// The builder references the stream and lookup, so the processor must not move or copy.
    JSONRemoveProcessor(const JSONRemoveProcessor &) = delete;
    JSONRemoveProcessor & operator=(const JSONRemoveProcessor &) = delete;

    void appendResult(std::string_view json, ColumnString::Chars & output)
    {
        /// RapidJSON treats '\0' as end-of-stream and would silently ignore a suffix.
        if (json.contains('\0'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid JSON string in function JSONRemove: embedded NULL byte");

        stream.reset(json);
        builder.reset();
        const auto parse_result
            = reader.Parse<rapidjson::kParseIterativeFlag | rapidjson::kParseNumbersAsStringsFlag>(stream, builder);
        if (parse_result.IsError())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Wrong JSON string passed to function JSONRemove: {}",
                rapidjson::GetParseError_En(parse_result.Code()));

        const auto root = builder.getRoot();
        auto & nodes = builder.getNodes();
        for (const auto & path : paths)
            removeAtPath(nodes, root, path, traversal_scratch);

        serializeJSON(nodes, root, json, output, serialization_stack);
    }

private:
    const ParsedPaths & paths;
    MemberNameLookup member_names;
    JSONInputStream stream;
    JSONTreeBuilder builder;
    TrackedReader reader;
    JSONPathTraversalScratch traversal_scratch;
    JSONSerializationStack serialization_stack;
};

class FunctionJSONRemove final : public IFunction
{
public:
    static constexpr auto name = "JSONRemove";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionJSONRemove>(context); }

    explicit FunctionJSONRemove(ContextPtr context)
        : max_parser_depth(context->getSettingsRef()[Setting::max_parser_depth])
        , max_parser_backtracks(context->getSettingsRef()[Setting::max_parser_backtracks])
    {
    }

    String getName() const override { return name; }

    /// The captured parser settings decide whether constant JSONPaths are parsed or rejected,
    /// see `IFunctionBase::updateHash`.
    void updateHash(SipHash & hash) const override
    {
        hash.update(max_parser_depth);
        hash.update(max_parser_backtracks);
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto string_validator = static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString);
        FunctionArgumentDescriptors mandatory_args{
            {"json", string_validator, nullptr, "String"},
            {"path", string_validator, nullptr, "String"},
        };
        FunctionArgumentDescriptor path_arg{"path", string_validator, nullptr, "String"};
        validateFunctionArgumentsWithVariadics(*this, arguments, mandatory_args, path_arg);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto paths = parseConstantPaths(arguments);

        const bool json_is_const = isColumnConst(*arguments[0].column);
        const auto * json_column = json_is_const ? checkAndGetColumnConstData<ColumnString>(arguments[0].column.get())
                                                 : checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!json_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a String", getName());

        auto result = ColumnString::create();
        result->reserve(json_is_const ? 1 : input_rows_count);
        if (!input_rows_count && !json_is_const)
            return result;

        auto & result_chars = result->getChars();
        auto & result_offsets = result->getOffsets();
        result_chars.reserve_exact(json_is_const ? json_column->getDataAt(0).size() : json_column->getChars().size());

        JSONRemoveProcessor processor(paths);

        const size_t rows_to_process = json_is_const ? 1 : input_rows_count;
        for (size_t row = 0; row < rows_to_process; ++row)
        {
            const auto json = json_column->getDataAt(json_is_const ? 0 : row);
            processor.appendResult(json, result_chars);
            result_offsets.push_back(result_chars.size());
        }

        if (json_is_const)
            return ColumnConst::create(std::move(result), input_rows_count);

        return result;
    }

private:
    ParsedPaths parseConstantPaths(const ColumnsWithTypeAndName & arguments) const
    {
        ParsedPaths paths;
        paths.reserve(arguments.size() - 1);

        const auto parse_depth = static_cast<uint32_t>(max_parser_depth);
        const auto parse_backtracks = static_cast<uint32_t>(max_parser_backtracks);
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (!isColumnConst(*arguments[i].column))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Path arguments of function {} must be constant Strings", getName());

            const auto * path_column = checkAndGetColumnConstData<ColumnString>(arguments[i].column.get());
            if (!path_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Path arguments of function {} must be Strings", getName());

            paths.push_back(parseJSONPath(String{path_column->getDataAt(0)}, parse_depth, parse_backtracks));
        }
        return paths;
    }

    const UInt64 max_parser_depth;
    const UInt64 max_parser_backtracks;
};
}

REGISTER_FUNCTION(JSONRemove)
{
    FunctionDocumentation::Description description = R"(
Removes one or more object members or array elements from a JSON string using JSONPath.
Each path must resolve to one object member name or array position. Paths are applied from left to right.
A JSONPath range that resolves to one array position, such as `$[1 to 2]`, is treated as that position and is equivalent to `$[1]`.
Object member steps follow all members with matching names, including duplicates.
A final object member step removes all matching members. Missing paths do not remove any values.
The result is compacted. Invalid JSON causes an exception.
        )";
    FunctionDocumentation::Syntax syntax = "JSONRemove(json, path[, path ...])";
    FunctionDocumentation::Arguments arguments
        = {{"json", "A string containing valid JSON.", {"String"}},
           {"path[, path ...]",
            "One or more constant strings containing JSONPath expressions. Each "
            "path must resolve to one object member name or array position. A range that resolves "
            "to one array position, such as $[1 to 2], is accepted as $[1].",
            {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the JSON document as a compact string.", {"String"}};
    FunctionDocumentation::Examples examples
        = {{"Usage example",
            R"(
SELECT JSONRemove('{"a":1,"b":2}', '$.a');
SELECT JSON_REMOVE('[0,1,2]', '$[0]', '$[1]');
            )",
            R"(
{"b":2}
[1]
            )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionJSONRemove>(documentation);
    factory.registerAlias("JSON_REMOVE", "JSONRemove", FunctionFactory::Case::Insensitive);
}

}

#endif
