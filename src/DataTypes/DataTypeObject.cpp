#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationJSON.h>
#include <DataTypes/Serializations/SerializationObjectTypedPath.h>
#include <DataTypes/Serializations/SerializationObjectDynamicPath.h>
#include <DataTypes/Serializations/SerializationSubObject.h>
#include <Columns/ColumnObject.h>
#include <Common/CurrentThread.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTNameTypePair.h>
#include <Formats/JSONExtractTree.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <IO/Operators.h>

#include "config.h"

#if USE_SIMDJSON
#  include <Common/JSONParsers/SimdJSONParser.h>
#endif
#if USE_RAPIDJSON
#  include <Common/JSONParsers/RapidJSONParser.h>
#else
#  include <Common/JSONParsers/DummyJSONParser.h>
#endif

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_object_type;
    extern const SettingsBool use_json_alias_for_old_object_type;
    extern const SettingsBool allow_simdjson;
}

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPILE_REGEXP;
}

DataTypeObject::DataTypeObject(
    const SchemaFormat & schema_format_,
    std::unordered_map<String, DataTypePtr> typed_paths_,
    std::unordered_set<String> paths_to_skip_,
    std::vector<String> path_regexps_to_skip_,
    size_t max_dynamic_paths_,
    size_t max_dynamic_types_)
    : schema_format(schema_format_)
    , typed_paths(std::move(typed_paths_))
    , paths_to_skip(std::move(paths_to_skip_))
    , path_regexps_to_skip(std::move(path_regexps_to_skip_))
    , max_dynamic_paths(max_dynamic_paths_)
    , max_dynamic_types(max_dynamic_types_)
{
    /// Check if regular expressions are valid.
    for (const auto & regexp_str : path_regexps_to_skip)
    {
        re2::RE2::Options options;
        /// Don't log errors to stderr.
        options.set_log_errors(false);
        auto regexp = re2::RE2(regexp_str, options);
        if (!regexp.ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "Invalid regexp '{}': {}", regexp_str, regexp.error());
    }

    for (const auto & [typed_path, type] : typed_paths)
    {
        for (const auto & path_to_skip : paths_to_skip)
        {
            if (typed_path.starts_with(path_to_skip))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path '{}' is specified with the data type ('{}') and matches the SKIP path prefix '{}'", typed_path, type->getName(), path_to_skip);
        }

        for (const auto & path_regex_to_skip : path_regexps_to_skip)
        {
            if (re2::RE2::FullMatch(typed_path, re2::RE2(path_regex_to_skip)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path '{}' is specified with the data type ('{}') and matches the SKIP REGEXP '{}'", typed_path, type->getName(), path_regex_to_skip);
        }
    }
}

DataTypeObject::DataTypeObject(const DB::DataTypeObject::SchemaFormat & schema_format_, size_t max_dynamic_paths_, size_t max_dynamic_types_)
    : schema_format(schema_format_)
    , max_dynamic_paths(max_dynamic_paths_)
    , max_dynamic_types(max_dynamic_types_)
{
}

bool DataTypeObject::equals(const IDataType & rhs) const
{
    if (const auto * object = typeid_cast<const DataTypeObject *>(&rhs))
    {
        if (typed_paths.size() != object->typed_paths.size())
            return false;

        for (const auto & [path, type] : typed_paths)
        {
            auto it = object->typed_paths.find(path);
            if (it == object->typed_paths.end())
                return false;
            if (!type->equals(*it->second))
                return false;
        }

        return schema_format == object->schema_format && paths_to_skip == object->paths_to_skip && path_regexps_to_skip == object->path_regexps_to_skip
            && max_dynamic_types == object->max_dynamic_types && max_dynamic_paths == object->max_dynamic_paths;
    }

    return false;
}

SerializationPtr DataTypeObject::doGetDefaultSerialization() const
{
    std::unordered_map<String, SerializationPtr> typed_path_serializations;
    typed_path_serializations.reserve(typed_paths.size());
    for (const auto & [path, type] : typed_paths)
        typed_path_serializations[path] = type->getDefaultSerialization();

    switch (schema_format)
    {
        case SchemaFormat::JSON:
#if USE_SIMDJSON
            auto context = CurrentThread::getQueryContext();
            if (!context)
                context = Context::getGlobalContextInstance();
            if (context->getSettingsRef()[Setting::allow_simdjson])
                return std::make_shared<SerializationJSON<SimdJSONParser>>(
                    std::move(typed_path_serializations),
                    paths_to_skip,
                    path_regexps_to_skip,
                    buildJSONExtractTree<SimdJSONParser>(getPtr(), "JSON serialization"));
#endif

#if USE_RAPIDJSON
            return std::make_shared<SerializationJSON<RapidJSONParser>>(
                std::move(typed_path_serializations),
                paths_to_skip,
                path_regexps_to_skip,
                buildJSONExtractTree<RapidJSONParser>(getPtr(), "JSON serialization"));
#else
            return std::make_shared<SerializationJSON<DummyJSONParser>>(
                std::move(typed_path_serializations),
                paths_to_skip,
                path_regexps_to_skip,
                buildJSONExtractTree<DummyJSONParser>(getPtr(), "JSON serialization"));
#endif
    }
}

String DataTypeObject::doGetName() const
{
    WriteBufferFromOwnString out;
    out << magic_enum::enum_name(schema_format);
    bool first = true;
    auto write_separator = [&]()
    {
        if (!first)
        {
            out << ", ";
        }
        else
        {
            out << "(";
            first = false;
        }
    };

    if (max_dynamic_types != DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES)
    {
        write_separator();
        out << "max_dynamic_types=" << max_dynamic_types;
    }

    if (max_dynamic_paths != DEFAULT_MAX_SEPARATELY_STORED_PATHS)
    {
        write_separator();
        out << "max_dynamic_paths=" << max_dynamic_paths;
    }

    std::vector<String> sorted_typed_paths;
    sorted_typed_paths.reserve(typed_paths.size());
    for (const auto & [path, _] : typed_paths)
        sorted_typed_paths.push_back(path);
    std::sort(sorted_typed_paths.begin(), sorted_typed_paths.end());
    for (const auto & path : sorted_typed_paths)
    {
        write_separator();
        out << backQuoteIfNeed(path) << " " << typed_paths.at(path)->getName();
    }

    std::vector<String> sorted_skip_paths;
    sorted_skip_paths.reserve(paths_to_skip.size());
    for (const auto & skip_path : paths_to_skip)
        sorted_skip_paths.push_back(skip_path);
    std::sort(sorted_skip_paths.begin(), sorted_skip_paths.end());
    for (const auto & skip_path : sorted_skip_paths)
    {
        write_separator();
        out << "SKIP " << backQuoteIfNeed(skip_path);
    }

    for (const auto & skip_regexp : path_regexps_to_skip)
    {
        write_separator();
        out << "SKIP REGEXP " << quoteString(skip_regexp);
    }

    if (!first)
        out << ")";

    return out.str();
}

MutableColumnPtr DataTypeObject::createColumn() const
{
    std::unordered_map<String, MutableColumnPtr> typed_path_columns;
    typed_path_columns.reserve(typed_paths.size());
    for (const auto & [path, type] : typed_paths)
        typed_path_columns[path] = type->createColumn();

    return ColumnObject::create(std::move(typed_path_columns), max_dynamic_paths, max_dynamic_types);
}

void DataTypeObject::forEachChild(const ChildCallback & callback) const
{
    for (const auto & [path, type] : typed_paths)
    {
        callback(*type);
        type->forEachChild(callback);
    }
}

namespace
{

/// It is possible to have nested JSON object inside Dynamic. For example when we have an array of JSON objects.
/// During type inference in parsing in case of creating nested JSON objects, we reduce max_dynamic_paths/max_dynamic_types by factors
/// NESTED_OBJECT_MAX_DYNAMIC_PATHS_REDUCE_FACTOR/NESTED_OBJECT_MAX_DYNAMIC_TYPES_REDUCE_FACTOR.
/// So the type name will actually be JSON(max_dynamic_paths=N, max_dynamic_types=M). But we want the user to be able to query it
/// using json.array.:`Array(JSON)`.some.path without specifying max_dynamic_paths/max_dynamic_types.
/// To support it, we do a trick - we replace JSON name in subcolumn to JSON(max_dynamic_paths=N, max_dynamic_types=M), because we know
/// the exact values of max_dynamic_paths/max_dynamic_types for it.
void replaceJSONTypeNameIfNeeded(String & type_name, const String & nested_json_type_name)
{
    auto pos = type_name.find("JSON");
    while (pos != String::npos)
    {
        /// Replace only if we don't already have parameters in JSON type declaration.
        if (pos + 4 == type_name.size() || type_name[pos + 4] != '(')
            type_name.replace(pos, 4, nested_json_type_name);
        pos = type_name.find("JSON", pos + 4);
    }
}

/// JSON subcolumn name with Dynamic type subcolumn looks like this:
/// "json.some.path.:`Type_name`.some.subcolumn".
/// We back quoted type name during identifier parsing so we can distinguish type subcolumn and path element ":TypeName".
std::pair<String, String> splitPathAndDynamicTypeSubcolumn(std::string_view subcolumn_name, const String & nested_json_type_name)
{
    /// Try to find dynamic type subcolumn in a form .:`Type`.
    auto pos = subcolumn_name.find(".:`");
    if (pos == std::string_view::npos)
        return {String(subcolumn_name), ""};

    ReadBufferFromMemory buf(subcolumn_name.substr(pos + 2));
    String dynamic_subcolumn;
    /// Try to read back quoted type name.
    if (!tryReadBackQuotedString(dynamic_subcolumn, buf))
        return {String(subcolumn_name), ""};

    replaceJSONTypeNameIfNeeded(dynamic_subcolumn, nested_json_type_name);

    /// If there is more data in the buffer - it's subcolumn of a type, append it to the type name.
    if (!buf.eof())
        dynamic_subcolumn += String(buf.position(), buf.available());

    return {String(subcolumn_name.substr(0, pos)), dynamic_subcolumn};
}

/// Sub-object subcolumn in JSON path always looks like "^`some`.path.path".
/// We back quote first path element after `^` so we can distinguish sub-object subcolumn and path element "^path".
std::optional<String> tryGetSubObjectSubcolumn(std::string_view subcolumn_name)
{
    if (!subcolumn_name.starts_with("^`"))
        return std::nullopt;

    ReadBufferFromMemory buf(subcolumn_name.data() + 1);
    String path;
    /// Try to read back-quoted first path element.
    if (!tryReadBackQuotedString(path, buf))
        return std::nullopt;

    /// Add remaining path elements if any.
    return path + String(buf.position(), buf.available());
}

}

std::unique_ptr<ISerialization::SubstreamData> DataTypeObject::getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, bool throw_if_null) const
{
    /// Check if it's sub-object subcolumn.
    /// In this case we should return JSON column with all paths that are inside specified object prefix.
    /// For example, if we have {"a" : {"b" : {"c" : {"d" : 10, "e" : "Hello"}, "f" : [1, 2, 3]}}} and subcolumn ^a.b
    /// we should return JSON column with data {"c" : {"d" : 10, "e" : Hello}, "f" : [1, 2, 3]}
    if (auto sub_object_subcolumn = tryGetSubObjectSubcolumn(subcolumn_name))
    {
        const String & prefix = *sub_object_subcolumn + ".";
        /// Collect new typed paths.
        std::unordered_map<String, DataTypePtr> typed_sub_paths;
        /// Collect serializations for typed paths. They will be needed for sub-object subcolumn deserialization.
        std::unordered_map<String, SerializationPtr> typed_paths_serializations;
        for (const auto & [path, type] : typed_paths)
        {
            if (path.starts_with(prefix))
            {
                typed_sub_paths[path.substr(prefix.size())] = type;
                typed_paths_serializations[path] = type->getDefaultSerialization();
            }
        }

        std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(std::make_shared<SerializationSubObject>(prefix, typed_paths_serializations));
        /// Keep all current constraints like limits and skip paths/prefixes/regexps.
        res->type = std::make_shared<DataTypeObject>(schema_format, typed_sub_paths, paths_to_skip, path_regexps_to_skip, max_dynamic_paths, max_dynamic_types);
        /// If column was provided, we should create a column for the requested subcolumn.
        if (data.column)
        {
            const auto & object_column = assert_cast<const ColumnObject &>(*data.column);

            auto result_column = res->type->createColumn();
            auto & result_object_column = assert_cast<ColumnObject &>(*result_column);

            /// Iterate over all typed/dynamic/shared data paths and collect all paths with specified prefix.
            auto & result_typed_columns = result_object_column.getTypedPaths();
            for (const auto & [path, column] : object_column.getTypedPaths())
            {
                if (path.starts_with(prefix))
                    result_typed_columns[path.substr(prefix.size())] = column;
            }

            std::vector<std::pair<String, ColumnPtr>> result_dynamic_paths;
            for (const auto & [path, column] :  object_column.getDynamicPaths())
            {
                if (path.starts_with(prefix))
                    result_dynamic_paths.emplace_back(path.substr(prefix.size()), column);
            }
            result_object_column.setDynamicPaths(result_dynamic_paths);

            const auto & shared_data_offsets = object_column.getSharedDataOffsets();
            const auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
            auto & result_shared_data_offsets = result_object_column.getSharedDataOffsets();
            result_shared_data_offsets.reserve(shared_data_offsets.size());
            auto [result_shared_data_paths, result_shared_data_values] = result_object_column.getSharedDataPathsAndValues();
            for (size_t i = 0; i != shared_data_offsets.size(); ++i)
            {
                size_t start = shared_data_offsets[ssize_t(i) - 1];
                size_t end = shared_data_offsets[ssize_t(i)];
                size_t lower_bound_index = ColumnObject::findPathLowerBoundInSharedData(prefix, *shared_data_paths, start, end);
                for (; lower_bound_index != end; ++lower_bound_index)
                {
                    auto path = shared_data_paths->getDataAt(lower_bound_index).toView();
                    if (!path.starts_with(prefix))
                        break;

                    auto sub_path = path.substr(prefix.size());
                    result_shared_data_paths->insertData(sub_path.data(), sub_path.size());
                    result_shared_data_values->insertFrom(*shared_data_values, lower_bound_index);
                }
                result_shared_data_offsets.push_back(result_shared_data_paths->size());
            }

            res->column = std::move(result_column);
        }

        return res;
    }

    /// Split requested subcolumn to the JSON path and Dynamic type subcolumn.
    auto [path, path_subcolumn] = splitPathAndDynamicTypeSubcolumn(subcolumn_name, getTypeOfNestedObjects()->getName());
    std::unique_ptr<SubstreamData> res;
    if (auto it = typed_paths.find(path); it != typed_paths.end())
    {
        res = std::make_unique<SubstreamData>(it->second->getDefaultSerialization());
        res->type = it->second;
    }
    else
    {
        res = std::make_unique<SubstreamData>(std::make_shared<SerializationDynamic>());
        res->type = std::make_shared<DataTypeDynamic>(max_dynamic_types);
    }

    /// If column was provided, we should create a column for requested subcolumn.
    if (data.column)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(*data.column);
        /// Try to find requested path in typed paths.
        if (auto typed_it = object_column.getTypedPaths().find(path); typed_it != object_column.getTypedPaths().end())
        {
            res->column = typed_it->second;
        }
        /// Try to find requested path in dynamic paths.
        else if (auto dynamic_it = object_column.getDynamicPaths().find(path); dynamic_it != object_column.getDynamicPaths().end())
        {
            res->column = dynamic_it->second;
        }
        /// Extract values of requested path from shared data.
        else
        {
            auto dynamic_column = ColumnDynamic::create(max_dynamic_types);
            dynamic_column->reserve(object_column.size());
            ColumnObject::fillPathColumnFromSharedData(*dynamic_column, path, object_column.getSharedDataPtr(), 0, object_column.size());
            res->column = std::move(dynamic_column);
        }
    }

    /// Get subcolumn for Dynamic type if needed.
    if (!path_subcolumn.empty())
    {
        res = DB::IDataType::getSubcolumnData(path_subcolumn, *res, throw_if_null);
        if (!res)
            return nullptr;
    }

    if (typed_paths.contains(path))
        res->serialization = std::make_shared<SerializationObjectTypedPath>(res->serialization, path);
    else
        res->serialization = std::make_shared<SerializationObjectDynamicPath>(res->serialization, path, path_subcolumn, max_dynamic_types);

    return res;
}

static DataTypePtr createObject(const ASTPtr & arguments, const DataTypeObject::SchemaFormat & schema_format)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeObject>(schema_format);

    std::unordered_map<String, DataTypePtr> typed_paths;
    std::unordered_set<String> paths_to_skip;
    std::vector<String> path_regexps_to_skip;

    size_t max_dynamic_types = DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES;
    size_t max_dynamic_paths = DataTypeObject::DEFAULT_MAX_SEPARATELY_STORED_PATHS;

    for (const auto & argument : arguments->children)
    {
        const auto * object_type_argument = argument->as<ASTObjectTypeArgument>();
        if (object_type_argument->parameter)
        {
            const auto * function = object_type_argument->parameter->as<ASTFunction>();

            if (!function || function->name != "equals")
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected parameter in {} type arguments: {}", magic_enum::enum_name(schema_format), function->formatForErrorMessage());

            const auto * identifier = function->arguments->children[0]->as<ASTIdentifier>();
            if (!identifier)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected {} type argument: {}. Expected expression 'max_dynamic_types=N' or 'max_dynamic_paths=N'", magic_enum::enum_name(schema_format), function->formatForErrorMessage());

            auto identifier_name = identifier->name();
            if (identifier_name != "max_dynamic_types" && identifier_name != "max_dynamic_paths")
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected parameter in {} type arguments: {}. Expected 'max_dynamic_types' or `max_dynamic_paths`", magic_enum::enum_name(schema_format), identifier_name);

            auto * literal = function->arguments->children[1]->as<ASTLiteral>();
            /// Is 1000000 a good maximum for max paths?
            size_t max_value = identifier_name == "max_dynamic_types" ? ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT : 1000000;
            if (!literal || literal->value.getType() != Field::Types::UInt64 || literal->value.safeGet<UInt64>() > max_value)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "'{}' parameter for {} type should be a positive integer between 0 and {}. Got {}", identifier_name, magic_enum::enum_name(schema_format), max_value, function->arguments->children[1]->formatForErrorMessage());

            if (identifier_name == "max_dynamic_types")
                max_dynamic_types = literal->value.safeGet<UInt64>();
            else
                max_dynamic_paths = literal->value.safeGet<UInt64>();
        }
        else if (object_type_argument->path_with_type)
        {
            const auto * path_with_type = object_type_argument->path_with_type->as<ASTNameTypePair>();
            auto data_type = DataTypeFactory::instance().get(path_with_type->type);
            if (typed_paths.contains(path_with_type->name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Found duplicated path with type: {}", path_with_type->name);
            typed_paths.emplace(path_with_type->name, data_type);
        }
        else if (object_type_argument->skip_path)
        {
            const auto * identifier = object_type_argument->skip_path->as<ASTIdentifier>();
            if (!identifier)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST in SKIP section of {} type arguments: {}. Expected identifier with path name", magic_enum::enum_name(schema_format), object_type_argument->skip_path->formatForErrorMessage());

            paths_to_skip.insert(identifier->name());
        }
        else if (object_type_argument->skip_path_regexp)
        {
            const auto * literal = object_type_argument->skip_path_regexp->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST in SKIP section of {} type arguments: {}. Expected identifier with path name", magic_enum::enum_name(schema_format), object_type_argument->skip_path->formatForErrorMessage());

            path_regexps_to_skip.push_back(literal->value.safeGet<String>());
        }
    }

    std::sort(path_regexps_to_skip.begin(), path_regexps_to_skip.end());
    return std::make_shared<DataTypeObject>(schema_format, std::move(typed_paths), std::move(paths_to_skip), std::move(path_regexps_to_skip), max_dynamic_paths, max_dynamic_types);
}

const DataTypePtr & DataTypeObject::getTypeOfSharedData()
{
    /// Array(Tuple(String, String))
    static const DataTypePtr type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}, Names{"paths", "values"}));
    return type;
}

void DataTypeObject::updateHashImpl(SipHash & hash) const
{
    hash.update(static_cast<UInt8>(schema_format));
    hash.update(max_dynamic_paths);
    hash.update(max_dynamic_types);

    // Include the sorted paths in the hash for deterministic ordering
    std::vector<String> sorted_paths;
    for (const auto & [path, type] : typed_paths)
        sorted_paths.push_back(path);
    std::sort(sorted_paths.begin(), sorted_paths.end());

    hash.update(sorted_paths.size());
    for (const auto & path : sorted_paths)
    {
        hash.update(path);
        typed_paths.at(path)->updateHash(hash);
    }

    // Include paths to skip in the hash
    hash.update(paths_to_skip.size());
    for (const auto & path : paths_to_skip)
        hash.update(path);

    // Include path regexps to skip in the hash
    hash.update(path_regexps_to_skip.size());
    for (const auto & regexp : path_regexps_to_skip)
        hash.update(regexp);
}

DataTypePtr DataTypeObject::getTypeOfNestedObjects() const
{
    return std::make_shared<DataTypeObject>(schema_format, max_dynamic_paths / NESTED_OBJECT_MAX_DYNAMIC_PATHS_REDUCE_FACTOR, max_dynamic_types / NESTED_OBJECT_MAX_DYNAMIC_TYPES_REDUCE_FACTOR);
}

static DataTypePtr createJSON(const ASTPtr & arguments)
{
    auto context = CurrentThread::getQueryContext();
    if (!context)
        context = Context::getGlobalContextInstance();

    if (context->getSettingsRef()[Setting::allow_experimental_object_type] && context->getSettingsRef()[Setting::use_json_alias_for_old_object_type])
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Experimental Object type doesn't support any arguments. If you want to use new JSON type, set settings enable_json_type = 1 and use_json_alias_for_old_object_type = 0");

        return std::make_shared<DataTypeObjectDeprecated>("JSON", false);
    }

    return createObject(arguments, DataTypeObject::SchemaFormat::JSON);
}

void registerDataTypeJSON(DataTypeFactory & factory)
{
    factory.registerDataType("JSON", createJSON, DataTypeFactory::Case::Insensitive);
}

}
