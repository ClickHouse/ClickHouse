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
#include <Common/VectorWithMemoryTracking.h>
#include "config.h"

#if USE_RAPIDJSON

#define RAPIDJSON_PARSE_DEFAULT_FLAGS (kParseIterativeFlag)

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
} // namespace Setting

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_DEEP_RECURSION;
} // namespace ErrorCodes

namespace
{
using TrackedPoolAllocator = rapidjson::MemoryPoolAllocator<RapidJSONMemoryTrackerAllocator>;
using TrackedValue = rapidjson::GenericValue<rapidjson::UTF8<char>, TrackedPoolAllocator>;
using TrackedDocument = rapidjson::GenericDocument<rapidjson::UTF8<char>, TrackedPoolAllocator, RapidJSONMemoryTrackerAllocator>;
using TrackedStringBuffer = rapidjson::GenericStringBuffer<rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;
using TrackedWriter = rapidjson::Writer<TrackedStringBuffer, rapidjson::UTF8<char>, rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;
using TrackedReader = rapidjson::GenericReader<rapidjson::UTF8<char>, rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;

constexpr size_t max_json_remove_depth = 1000;

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

using JSONMetadataRef = UInt32;
constexpr JSONMetadataRef json_number_ref_flag = JSONMetadataRef{1} << 31;

struct JSONMetadataNode
{
    VectorWithMemoryTracking<JSONMetadataRef> children;
};

struct JSONMetadata
{
    VectorWithMemoryTracking<JSONMetadataNode> containers;
};

bool isContainerMetadataRef(JSONMetadataRef ref, const JSONMetadata & metadata)
{
    return ref && !(ref & json_number_ref_flag) && ref <= metadata.containers.size();
}

struct NumberLexeme
{
    size_t offset;
    rapidjson::SizeType length;
};

struct NumberLexemes
{
    VectorWithMemoryTracking<char> data;
    VectorWithMemoryTracking<NumberLexeme> entries;
};

class NumberCollector : public rapidjson::BaseReaderHandler<rapidjson::UTF8<char>, NumberCollector>
{
public:
    NumberLexemes number_lexemes;

    bool RawNumber(const char * value, rapidjson::SizeType length, bool)
    {
        const size_t offset = number_lexemes.data.size();
        number_lexemes.data.insert(number_lexemes.data.end(), value, value + length);
        number_lexemes.entries.push_back({offset, length});
        return true;
    }
};

NumberLexemes collectNumberLexemes(const StringRef & json)
{
    NumberCollector collector;
    TrackedReader reader;
    rapidjson::MemoryStream stream(json.data(), json.size());
    const auto parse_result = reader.Parse<kParseIterativeFlag | kParseNumbersAsStringsFlag>(stream, collector);

    if (parse_result.IsError())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Wrong JSON string passed to function JSONRemove: {}",
            rapidjson::GetParseError_En(parse_result.Code()));

    return std::move(collector.number_lexemes);
}

void buildJSONMetadata(
    const TrackedValue & value,
    JSONMetadata & metadata,
    const NumberLexemes & number_lexemes,
    size_t & number_index,
    JSONMetadataRef & metadata_ref)
{
    if (value.IsObject())
    {
        if (metadata.containers.size() >= json_number_ref_flag - 1)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Too many JSON containers in function JSONRemove");

        metadata.containers.emplace_back();
        metadata_ref = static_cast<JSONMetadataRef>(metadata.containers.size());
        metadata.containers.back().children.reserve(value.MemberCount());
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
        {
            JSONMetadataRef child_ref = 0;
            buildJSONMetadata(it->value, metadata, number_lexemes, number_index, child_ref);
            metadata.containers[metadata_ref - 1].children.emplace_back(child_ref);
        }
    }
    else if (value.IsArray())
    {
        if (metadata.containers.size() >= json_number_ref_flag - 1)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Too many JSON containers in function JSONRemove");

        metadata.containers.emplace_back();
        metadata_ref = static_cast<JSONMetadataRef>(metadata.containers.size());
        metadata.containers.back().children.reserve(value.Size());
        for (const auto * it = value.Begin(); it != value.End(); ++it)
        {
            JSONMetadataRef child_ref = 0;
            buildJSONMetadata(*it, metadata, number_lexemes, number_index, child_ref);
            metadata.containers[metadata_ref - 1].children.emplace_back(child_ref);
        }
    }
    else if (value.IsNumber())
    {
        if (number_index >= number_lexemes.entries.size())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unable to track JSON numbers in function JSONRemove");
        if (number_index >= json_number_ref_flag - 1)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Too many JSON numbers in function JSONRemove");

        metadata_ref = json_number_ref_flag | static_cast<JSONMetadataRef>(++number_index);
    }
}

void checkJSONDepth(const TrackedValue & root)
{
    VectorWithMemoryTracking<std::pair<const TrackedValue *, size_t>> to_visit;
    to_visit.emplace_back(&root, 1);

    while (!to_visit.empty())
    {
        const auto [value, depth] = to_visit.back();
        to_visit.pop_back();

        if (depth > max_json_remove_depth)
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "Too deep nesting in a JSON document passed to function "
                "JSONRemove: the limit is {}",
                max_json_remove_depth);

        if (value->IsObject())
        {
            for (auto it = value->MemberBegin(); it != value->MemberEnd(); ++it)
                to_visit.emplace_back(&it->value, depth + 1);
        }
        else if (value->IsArray())
        {
            for (const auto * it = value->Begin(); it != value->End(); ++it)
                to_visit.emplace_back(&*it, depth + 1);
        }
    }
}

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

bool removeAtPath(TrackedValue & document, JSONMetadata & metadata, JSONMetadataRef root_metadata_ref, const ParsedPath & path)
{
    TrackedValue * parent = &document;
    JSONMetadataRef parent_metadata_ref = root_metadata_ref;

    for (size_t i = 0; i + 1 < path.size(); ++i)
    {
        const auto & step = path[i];
        if (step.type == PathStep::Type::Member)
        {
            if (!parent->IsObject())
                return false;
            if (!isContainerMetadataRef(parent_metadata_ref, metadata))
                return false;

            TrackedValue key(rapidjson::StringRef(step.member_name.data(), step.member_name.size()));
            auto member = parent->FindMember(key);
            if (member == parent->MemberEnd())
                return false;

            const size_t child_index = static_cast<size_t>(member - parent->MemberBegin());

            parent = &member->value;
            parent_metadata_ref = metadata.containers[parent_metadata_ref - 1].children[child_index];
        }
        else
        {
            if (!parent->IsArray() || step.index >= parent->Size())
                return false;
            if (!isContainerMetadataRef(parent_metadata_ref, metadata))
                return false;

            parent = &(*parent)[step.index];
            parent_metadata_ref = metadata.containers[parent_metadata_ref - 1].children[step.index];
        }
    }

    const auto & target = path.back();

    if (target.type == PathStep::Type::Member)
    {
        if (!parent->IsObject())
            return false;
        if (!isContainerMetadataRef(parent_metadata_ref, metadata))
            return false;

        TrackedValue key(rapidjson::StringRef(target.member_name.data(), target.member_name.size()));
        auto member = parent->FindMember(key);
        if (member == parent->MemberEnd())
            return false;

        const size_t child_index = static_cast<size_t>(member - parent->MemberBegin());

        parent->EraseMember(member);
        metadata.containers[parent_metadata_ref - 1].children.erase(
            metadata.containers[parent_metadata_ref - 1].children.begin() + child_index);
        return true;
    }

    if (!parent->IsArray() || target.index >= parent->Size())
        return false;
    if (!isContainerMetadataRef(parent_metadata_ref, metadata))
        return false;

    parent->Erase(parent->Begin() + target.index);
    metadata.containers[parent_metadata_ref - 1].children.erase(
        metadata.containers[parent_metadata_ref - 1].children.begin() + target.index);
    return true;
}

bool serializeJSON(
    const TrackedValue & value,
    JSONMetadataRef metadata_ref,
    const JSONMetadata & metadata,
    const NumberLexemes & number_lexemes,
    TrackedWriter & writer)
{
    if (metadata_ref & json_number_ref_flag)
    {
        if (!value.IsNumber())
            return false;

        const auto number_index = (metadata_ref & ~json_number_ref_flag) - 1;
        if (number_index >= number_lexemes.entries.size())
            return false;

        const auto & number = number_lexemes.entries[number_index];
        if (number.offset > number_lexemes.data.size() || number.length > number_lexemes.data.size() - number.offset)
            return false;

        return writer.RawValue(number_lexemes.data.data() + number.offset, number.length, rapidjson::kNumberType);
    }

    if (value.IsObject())
    {
        if (!isContainerMetadataRef(metadata_ref, metadata))
            return false;

        const auto & node = metadata.containers[metadata_ref - 1];
        if (node.children.size() != value.MemberCount() || !writer.StartObject())
            return false;

        size_t child_index = 0;
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it, ++child_index)
        {
            if (!writer.Key(it->name.GetString(), it->name.GetStringLength())
                || !serializeJSON(it->value, node.children[child_index], metadata, number_lexemes, writer))
                return false;
        }

        return writer.EndObject(value.MemberCount());
    }

    if (value.IsArray())
    {
        if (!isContainerMetadataRef(metadata_ref, metadata))
            return false;

        const auto & node = metadata.containers[metadata_ref - 1];
        if (node.children.size() != value.Size() || !writer.StartArray())
            return false;

        size_t child_index = 0;
        for (const auto * it = value.Begin(); it != value.End(); ++it, ++child_index)
        {
            if (!serializeJSON(*it, node.children[child_index], metadata, number_lexemes, writer))
                return false;
        }

        return writer.EndArray(value.Size());
    }

    if (value.IsNumber() || metadata_ref)
        return false;

    return value.Accept(writer);
}

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

        const bool json_is_const = isColumnConst(*arguments[0].column);
        const auto * json_column = json_is_const ? checkAndGetColumnConstData<ColumnString>(arguments[0].column.get())
                                                 : checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!json_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a String", getName());

        auto result = ColumnString::create();
        result->reserve(json_is_const ? 1 : input_rows_count);
        if (!input_rows_count)
            return result;

        const size_t rows_to_process = json_is_const ? 1 : input_rows_count;
        for (size_t row = 0; row < rows_to_process; ++row)
        {
            const auto json = json_column->getDataAt(json_is_const ? 0 : row);
            auto number_lexemes = collectNumberLexemes(json);

            TrackedPoolAllocator allocator;
            TrackedDocument document(&allocator);
            document.Parse(json.data, json.size);

            if (document.HasParseError())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Wrong JSON string passed to function JSONRemove: {}",
                    rapidjson::GetParseError_En(document.GetParseError()));

            checkJSONDepth(document);
            JSONMetadata metadata;
            size_t number_index = 0;
            JSONMetadataRef root_metadata_ref = 0;
            buildJSONMetadata(document, metadata, number_lexemes, number_index, root_metadata_ref);

            if (number_index != number_lexemes.entries.size())
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unable to track JSON numbers in function JSONRemove");

            for (const auto & path : paths)
                removeAtPath(document, metadata, root_metadata_ref, path);

            TrackedStringBuffer buffer;
            TrackedWriter writer(buffer);
            if (!serializeJSON(document, root_metadata_ref, metadata, number_lexemes, writer))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to serialize JSON string in function JSONRemove");

            result->insertData(buffer.GetString(), buffer.GetSize());
        }

        if (json_is_const)
            return ColumnConst::create(std::move(result), input_rows_count);

        return result;
    }

private:
    const UInt64 max_parser_depth;
    const UInt64 max_parser_backtracks;
};
} // namespace

REGISTER_FUNCTION(JSONRemove)
{
    FunctionDocumentation::Description description = R"(
Removes one or more object members or array elements from a JSON string using JSONPath.
Each path must select exactly one member or element. Paths are applied from left to right. Missing paths do not change the JSON document.
        )";
    FunctionDocumentation::Syntax syntax = "JSONRemove(json, path[, path ...])";
    FunctionDocumentation::Arguments arguments
        = {{"json", "A string containing valid JSON.", {"String"}},
           {"path[, path ...]",
            "One or more constant strings containing JSONPath expressions. Each "
            "path must select one object member or array element.",
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

} // namespace DB

#endif
