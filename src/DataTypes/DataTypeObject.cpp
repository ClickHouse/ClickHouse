#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationJSON.h>
#include <DataTypes/Serializations/SerializationObjectTypedPath.h>
#include <DataTypes/Serializations/SerializationObjectDynamicPath.h>
#include <DataTypes/Serializations/SerializationSubObject.h>
#include <DataTypes/Serializations/SerializationObjectDistinctPaths.h>
#include <DataTypes/Serializations/SerializationObjectCombinedPath.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnDynamic.h>
#include <Interpreters/castColumn.h>
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
#include <boost/algorithm/string.hpp>

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

SerializationPtr DataTypeObject::doGetSerialization(const SerializationInfoSettings & settings) const
{
    std::unordered_map<String, SerializationPtr> typed_paths_serializations;
    typed_paths_serializations.reserve(typed_paths.size());
    for (const auto & [path, type] : typed_paths)
        typed_paths_serializations[path] = settings.propagate_types_serialization_versions_to_nested_types ? type->getSerialization(settings) : type->getDefaultSerialization();

    SerializationPtr dynamic_serialization = settings.propagate_types_serialization_versions_to_nested_types
        ? getDynamicType()->getSerialization(settings)
        : getDynamicType()->getDefaultSerialization();

    switch (schema_format)
    {
        case SchemaFormat::JSON:
#if USE_SIMDJSON
            auto context = CurrentThread::tryGetQueryContext();
            if (!context)
                context = Context::getGlobalContextInstance();
            if (context->getSettingsRef()[Setting::allow_simdjson])
                return SerializationJSON<SimdJSONParser>::create(
                    typed_paths,
                    typed_paths_serializations,
                    paths_to_skip,
                    path_regexps_to_skip,
                    getDynamicType(),
                    dynamic_serialization,
                    buildJSONExtractTree<SimdJSONParser>(getPtr(), "JSON serialization"));
#endif

#if USE_RAPIDJSON
            return SerializationJSON<RapidJSONParser>::create(
                typed_paths,
                typed_paths_serializations,
                paths_to_skip,
                path_regexps_to_skip,
                getDynamicType(),
                dynamic_serialization,
                buildJSONExtractTree<RapidJSONParser>(getPtr(), "JSON serialization"));
#else
            return SerializationJSON<DummyJSONParser>::create(
                typed_paths,
                typed_paths_serializations,
                paths_to_skip,
                path_regexps_to_skip,
                getDynamicType(),
                dynamic_serialization,
                buildJSONExtractTree<DummyJSONParser>(getPtr(), "JSON serialization"));
#endif
    }
}

String DataTypeObject::getSchemaFormatString() const
{
    return String{magic_enum::enum_name(schema_format)};
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

    if (max_dynamic_paths != DEFAULT_MAX_DYNAMIC_PATHS)
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
        /// We must quote path "SKIP" to avoid its confusion with SKIP keyword.
        if (boost::to_upper_copy(path) == "SKIP")
            out << backQuote(path) << " " << typed_paths.at(path)->getName();
        else
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
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_path_columns;
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

/// Strip JSON type parameters from a type name string for comparison purposes.
/// E.g. "Array(JSON(max_dynamic_paths=4, max_dynamic_types=2))" -> "Array(JSON)"
///      "JSON(max_dynamic_paths=16, max_dynamic_types=8)" -> "JSON"
///      "Int64" -> "Int64" (no change)
/// When scanning for the matching ')' we skip over back-quoted strings
/// (which may contain unbalanced parentheses, e.g. JSON(SKIP `some()path`))
/// using tryReadBackQuotedString to correctly handle escaped backticks.
String removeJSONTypeParameters(const String & type_name)
{
    String result = type_name;
    auto pos = result.find("JSON(");
    while (pos != String::npos)
    {
        /// Find matching ')' starting after "JSON("
        size_t start = pos + 4; /// position of '('
        size_t depth = 1;
        size_t i = start + 1;
        while (i < result.size() && depth > 0)
        {
            if (result[i] == '`')
            {
                /// Use tryReadBackQuotedString to properly skip back-quoted strings,
                /// handling escaped backticks (doubled: ``).
                ReadBufferFromMemory buf(result.data() + i, result.size() - i);
                String tmp;
                if (tryReadBackQuotedString(tmp, buf))
                    i += buf.count();
                else
                    ++i;
                continue;
            }
            if (result[i] == '\'')
            {
                /// Use tryReadQuotedString to properly skip single-quoted strings,
                /// so that parentheses inside them (e.g. in SKIP REGEXP '...')
                /// don't affect the nesting depth.
                ReadBufferFromMemory buf(result.data() + i, result.size() - i);
                String tmp;
                if (tryReadQuotedString(tmp, buf))
                    i += buf.count();
                else
                    ++i;
                continue;
            }
            if (result[i] == '(')
                ++depth;
            else if (result[i] == ')')
                --depth;
            ++i;
        }
        /// Remove from '(' to matching ')'
        result.erase(start, i - start);
        pos = result.find("JSON(", pos + 4);
    }
    return result;
}

/// JSON subcolumn name with Dynamic type subcolumn looks like this:
/// "json.some.path.:`Type_name`.some.subcolumn".
/// We back quoted type name during identifier parsing so we can distinguish type subcolumn and path element ":TypeName".
struct PathAndSubcolumn
{
    String path;
    String type_hint;
    /// Remaining subcolumn after the type hint, without a leading dot separator.
    String remaining;

    /// Reconstruct the full subcolumn (type_hint + "." + remaining).
    String fullSubcolumn() const
    {
        if (type_hint.empty())
            return {};
        if (remaining.empty())
            return type_hint;
        return type_hint + "." + remaining;
    }
};

PathAndSubcolumn splitPathAndDynamicTypeSubcolumn(std::string_view subcolumn_name, const String & nested_json_type_name)
{
    /// Try to find dynamic type subcolumn in a form .:`Type`.
    auto pos = subcolumn_name.find(".:`");
    if (pos == std::string_view::npos)
        return {String(subcolumn_name), {}, {}};

    ReadBufferFromMemory buf(subcolumn_name.substr(pos + 2));
    String type_hint;
    /// Try to read back quoted type name.
    if (!tryReadBackQuotedString(type_hint, buf))
        return {String(subcolumn_name), {}, {}};

    replaceJSONTypeNameIfNeeded(type_hint, nested_json_type_name);

    /// If there is more data in the buffer - it's the remaining subcolumn after the type hint.
    /// Strip the leading dot separator if present.
    String remaining;
    if (!buf.eof())
    {
        remaining = String(buf.position(), buf.available());
        if (!remaining.empty() && remaining[0] == '.')
            remaining = remaining.substr(1);
    }

    return {String(subcolumn_name.substr(0, pos)), std::move(type_hint), std::move(remaining)};
}

/// Prefixed subcolumn in JSON path always looks like "<prefix>`some`.path.path"
/// (e.g. "^`some`.path" for sub-object, "@`some`.path" for combined).
/// We back-quote the first path element after the prefix so we can distinguish
/// prefixed subcolumns from regular path elements like "^path" or "$path".
std::optional<String> tryGetPrefixedSubcolumn(std::string_view subcolumn_name, char prefix)
{
    if (subcolumn_name.size() < 2 || subcolumn_name[0] != prefix || subcolumn_name[1] != '`')
        return std::nullopt;

    ReadBufferFromMemory buf(subcolumn_name.substr(1));
    String path;
    /// Try to read back-quoted first path element.
    if (!tryReadBackQuotedString(path, buf))
        return std::nullopt;

    /// Add remaining path elements if any.
    return path + String(buf.position(), buf.available());
}

/// Extracts the literal (Dynamic) column for the given path from a ColumnObject.
/// Checks typed paths first, then dynamic paths, then falls back to shared data.
ColumnPtr extractLiteralColumn(const ColumnObject & object_column, const String & path, size_t max_dynamic_types)
{
    if (auto typed_it = object_column.getTypedPaths().find(path); typed_it != object_column.getTypedPaths().end())
        return typed_it->second;

    if (auto dynamic_it = object_column.getDynamicPaths().find(path); dynamic_it != object_column.getDynamicPaths().end())
        return dynamic_it->second;

    auto dynamic_column = ColumnDynamic::create(max_dynamic_types);
    dynamic_column->reserve(object_column.size());
    ColumnObject::fillPathColumnFromSharedData(*dynamic_column, path, object_column.getSharedDataPtr(), 0, object_column.size());
    return dynamic_column;
}

/// Extracts the sub-object column for the given path prefix from a ColumnObject.
/// Returns a new ColumnObject containing only the paths that start with the prefix,
/// with the prefix stripped from the path names.
ColumnPtr extractSubObjectColumn(const ColumnObject & object_column, const String & prefix, const DataTypePtr & sub_object_type)
{
    auto result_column = sub_object_type->createColumn();
    auto & result_object_column = assert_cast<ColumnObject &>(*result_column);

    auto & result_typed_columns = result_object_column.getTypedPaths();
    for (const auto & [path, column] : object_column.getTypedPaths())
    {
        if (path.starts_with(prefix))
            result_typed_columns[path.substr(prefix.size())] = column;
    }

    VectorWithMemoryTracking<std::pair<String, ColumnPtr>> result_dynamic_paths;
    for (const auto & [path, column] : object_column.getDynamicPaths())
    {
        if (path.starts_with(prefix))
            result_dynamic_paths.emplace_back(path.substr(prefix.size()), column);
    }
    result_object_column.setMaxDynamicPaths(result_dynamic_paths.size());
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
            auto path = shared_data_paths->getDataAt(lower_bound_index);
            if (!path.starts_with(prefix))
                break;

            auto sub_path = path.substr(prefix.size());
            result_shared_data_paths->insertData(sub_path.data(), sub_path.size());
            result_shared_data_values->insertFrom(*shared_data_values, lower_bound_index);
        }
        result_shared_data_offsets.push_back(result_shared_data_paths->size());
    }

    return result_column;
}

/// Merges literal and sub-object columns into a single Dynamic column.
/// Prefers the literal value if present; falls back to the sub-object cast to Dynamic; otherwise NULL.
ColumnPtr extractCombinedColumn(
    const ColumnObject & object_column,
    const String & path,
    const String & prefix,
    const DataTypePtr & sub_object_type,
    const DataTypePtr & dynamic_result_type,
    size_t max_dynamic_types)
{
    auto literal_column = extractLiteralColumn(object_column, path, max_dynamic_types);
    auto sub_object_column = extractSubObjectColumn(object_column, prefix, sub_object_type);

    /// If sub-object is all defaults, just use literal.
    if (sub_object_column->getNumberOfDefaultRows() == sub_object_column->size())
        return literal_column;

    /// Cast sub-object to Dynamic.
    auto casted_sub_object = castColumn({sub_object_column, sub_object_type, ""}, dynamic_result_type);

    /// Merge row-by-row: prefer literal, then sub-object, then NULL.
    auto merged = dynamic_result_type->createColumn();
    merged->reserve(object_column.size());
    for (size_t i = 0; i < object_column.size(); ++i)
    {
        if (!literal_column->isDefaultAt(i))
            merged->insertFrom(*literal_column, i);
        else if (!sub_object_column->isDefaultAt(i))
            merged->insertFrom(*casted_sub_object, i);
        else
            merged->insertDefault();
    }
    return merged;
}

/// Builds the sub-object type and serialization for a given path prefix.
/// Returns a pair of (sub_object_type, sub_object_serialization), shared between
/// the sub-object subcolumn (^) and the combined subcolumn (@) branches.
std::pair<DataTypePtr, SerializationPtr> buildSubObjectTypeAndSerialization(
    const String & prefix,
    const std::unordered_map<String, DataTypePtr> & typed_paths,
    const std::unordered_map<String, SerializationPtr> & all_typed_paths_serializations,
    const DataTypeObject::SchemaFormat & schema_format,
    const std::unordered_set<String> & paths_to_skip,
    const std::vector<String> & path_regexps_to_skip,
    size_t max_dynamic_paths,
    size_t max_dynamic_types,
    const DataTypePtr & dynamic_type,
    const SerializationPtr & dynamic_serialization)
{
    std::unordered_map<String, DataTypePtr> typed_sub_paths;
    std::unordered_map<String, SerializationPtr> typed_sub_paths_serializations;
    for (const auto & [path, type] : typed_paths)
    {
        if (path.starts_with(prefix))
        {
            typed_sub_paths[path.substr(prefix.size())] = type;
            typed_sub_paths_serializations[path] = all_typed_paths_serializations.at(path);
        }
    }

    auto sub_object_type = std::make_shared<DataTypeObject>(schema_format, typed_sub_paths, paths_to_skip, path_regexps_to_skip, max_dynamic_paths, max_dynamic_types);
    auto sub_object_serialization = SerializationSubObject::create(prefix, typed_sub_paths_serializations, dynamic_type, dynamic_serialization);
    return {std::move(sub_object_type), std::move(sub_object_serialization)};
}

}

std::unique_ptr<ISerialization::SubstreamData> DataTypeObject::getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const
{
    /// Check if it's a special subcolumn used for distinct paths calculation.
    if (subcolumn_name == SPECIAL_SUBCOLUMN_NAME_FOR_DISTINCT_PATHS_CALCULATION)
    {
        std::vector<String> typed_path_names;
        typed_path_names.reserve(typed_paths.size());
        for (const auto & [path, _] : typed_paths)
            typed_path_names.push_back(path);

        std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(SerializationObjectDistinctPaths::create(typed_path_names));
        res->type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        /// If column was provided, we should create a column for the requested subcolumn.
        if (data.column)
        {
            const auto & object_column = assert_cast<const ColumnObject &>(*data.column);
            auto result_column = res->type->createColumn();
            if (!object_column.empty())
            {
                auto & result_array_column = assert_cast<ColumnArray &>(*result_column);
                auto & result_paths_column = assert_cast<ColumnString &>(result_array_column.getData());
                for (const auto & path : typed_path_names)
                    result_paths_column.insertData(path.data(), path.size());
                for (const auto & [path, _] : object_column.getDynamicPaths())
                    result_paths_column.insertData(path.data(), path.size());
                const auto [shared_data_paths, _] = object_column.getSharedDataPathsAndValues();
                result_paths_column.insertRangeFrom(*shared_data_paths, 0, shared_data_paths->size());
                result_array_column.getOffsets().push_back(result_paths_column.size());
                result_array_column.insertManyDefaults(object_column.size() - 1);
            }
            res->column = std::move(result_column);
        }

        return res;
    }

    const auto & object_serialization = dynamic_cast<const SerializationObject &>(*removeNamedSerialization(data.serialization));
    const auto & typed_paths_serializations = object_serialization.getTypedPathsSerializations();
    const auto & dynamic_path_serialization = object_serialization.getDynamicPathSerialization();

    /// Check if it's sub-object subcolumn.
    /// In this case we should return JSON column with all paths that are inside specified object prefix.
    /// For example, if we have {"a" : {"b" : {"c" : {"d" : 10, "e" : "Hello"}, "f" : [1, 2, 3]}}} and subcolumn ^a.b
    /// we should return JSON column with data {"c" : {"d" : 10, "e" : Hello}, "f" : [1, 2, 3]}
    if (auto sub_object_subcolumn = tryGetPrefixedSubcolumn(subcolumn_name, SUB_OBJECT_SUBCOLUMN_PREFIX))
    {
        const String prefix = *sub_object_subcolumn + ".";
        auto [sub_object_type, sub_object_serialization] = buildSubObjectTypeAndSerialization(
            prefix, typed_paths, typed_paths_serializations, schema_format, paths_to_skip, path_regexps_to_skip,
            max_dynamic_paths, max_dynamic_types, getDynamicType(), dynamic_path_serialization);

        std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(sub_object_serialization);
        res->type = sub_object_type;

        if (data.column)
        {
            const auto & object_column = assert_cast<const ColumnObject &>(*data.column);
            res->column = extractSubObjectColumn(object_column, prefix, sub_object_type);
        }

        return res;
    }

    /// Check if it's a combined literal+sub-object subcolumn.
    /// In this case we should return Dynamic column that contains:
    /// - literal value if the path has one
    /// - sub-object as JSON if the path has a non-empty sub-object but no literal
    /// - NULL if literal is null and sub-object is empty
    if (auto combined_subcolumn = tryGetPrefixedSubcolumn(subcolumn_name, COMBINED_SUBCOLUMN_PREFIX))
    {
        const String & combined_path = *combined_subcolumn;

        /// For typed paths, return literal value only (typed paths are always considered present).
        if (auto it = typed_paths.find(combined_path); it != typed_paths.end())
        {
            auto res = std::make_unique<SubstreamData>(typed_paths_serializations.at(combined_path));
            res->type = it->second;
            if (data.column)
            {
                const auto & object_column = assert_cast<const ColumnObject &>(*data.column);
                res->column = object_column.getTypedPaths().at(combined_path);
            }
            res->serialization = SerializationObjectTypedPath::create(res->serialization, combined_path);
            return res;
        }

        /// Non-typed path: merge literal + sub-object.
        const String prefix = combined_path + ".";
        auto dynamic_result_type = getDynamicType();
        auto [sub_object_type, sub_object_serialization] = buildSubObjectTypeAndSerialization(
            prefix, typed_paths, typed_paths_serializations, schema_format, paths_to_skip, path_regexps_to_skip,
            max_dynamic_paths, max_dynamic_types, dynamic_result_type, dynamic_path_serialization);

        auto literal_serialization = SerializationObjectDynamicPath::create(dynamic_path_serialization, combined_path, /*path_subcolumn=*/"", dynamic_result_type, dynamic_path_serialization, dynamic_result_type);

        auto res = std::make_unique<SubstreamData>(SerializationObjectCombinedPath::create(
            literal_serialization, sub_object_serialization, dynamic_result_type, sub_object_type));
        res->type = dynamic_result_type;

        if (data.column)
        {
            const auto & object_column = assert_cast<const ColumnObject &>(*data.column);
            res->column = extractCombinedColumn(object_column, combined_path, prefix, sub_object_type, dynamic_result_type, max_dynamic_types);
        }

        return res;
    }

    /// If the subcolumn starts with a type hint (.:`Type`), it means this getDynamicSubcolumnData
    /// was reached from IDataType::getSubcolumnData after a typed path prefix match in enumerateStreams.
    /// E.g. for json.a.:`Array(JSON)`.x where a is a typed Array(JSON) path, enumerateStreams found "a"
    /// as a static subcolumn, then tried to resolve the remaining ":`Array(JSON)`.x" via the typed path's
    /// type chain, which eventually called this method. We return nullptr here so that the resolution falls
    /// through to the outer DataTypeObject::getDynamicSubcolumnData with the full subcolumn name, where
    /// the type hint can be properly detected and stripped.
    if (subcolumn_name.starts_with(":`"))
        return nullptr;

    /// Split requested subcolumn to the JSON path, type hint, and remaining subcolumn.
    auto split = splitPathAndDynamicTypeSubcolumn(subcolumn_name, getTypeOfNestedObjects()->getName());
    const auto & path = split.path;
    String path_subcolumn;
    std::unique_ptr<SubstreamData> res;
    if (auto it = typed_paths.find(path); it != typed_paths.end())
    {
        /// If there is a type hint subcolumn and it matches the typed path's type
        /// (after normalizing JSON parameters), the hint is redundant — strip it.
        if (!split.type_hint.empty() && removeJSONTypeParameters(split.type_hint) == removeJSONTypeParameters(it->second->getName()))
            path_subcolumn = split.remaining;
        else
            path_subcolumn = split.fullSubcolumn();

        res = std::make_unique<SubstreamData>(typed_paths_serializations.at(path));
        res->type = it->second;
    }
    else
    {
        path_subcolumn = split.fullSubcolumn();
        res = std::make_unique<SubstreamData>(dynamic_path_serialization);
        res->type = std::make_shared<DataTypeDynamic>(max_dynamic_types);
    }

    if (data.column)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(*data.column);
        res->column = extractLiteralColumn(object_column, path, max_dynamic_types);
    }

    /// Get subcolumn for Dynamic type if needed.
    if (!path_subcolumn.empty())
    {
        res = DB::IDataType::getSubcolumnData(path_subcolumn, *res, initial_array_level, throw_if_null);
        if (!res)
            return nullptr;
    }

    if (typed_paths.contains(path))
        res->serialization = SerializationObjectTypedPath::create(res->serialization, path);
    else
        res->serialization = SerializationObjectDynamicPath::create(res->serialization, path, path_subcolumn, getDynamicType(), dynamic_path_serialization, res->type);

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
    size_t max_dynamic_paths = DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS;

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
            size_t max_value = identifier_name == "max_dynamic_types" ? ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT : DataTypeObject::MAX_DYNAMIC_PATHS_LIMIT;
            if (!literal || literal->value.getType() != Field::Types::UInt64 || literal->value.safeGet<UInt64>() > max_value)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "'{}' parameter for {} type should be a positive integer between 0 and {}. Got {}", identifier_name, magic_enum::enum_name(schema_format), max_value, function->arguments->children[1]->formatForErrorMessage());

            if (identifier_name == "max_dynamic_types")
                max_dynamic_types = literal->value.safeGet<UInt64>();
            else
                max_dynamic_paths = literal->value.safeGet<UInt64>();
        }
        else if (object_type_argument->path_with_type)
        {
            const auto * path_with_type = object_type_argument->path_with_type->as<ASTObjectTypedPathArgument>();
            auto data_type = DataTypeFactory::instance().get(path_with_type->type);
            if (typed_paths.contains(path_with_type->path))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Found duplicated path with type: {}", path_with_type->path);

            if (typed_paths.size() >= DataTypeObject::MAX_TYPED_PATHS)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too many typed paths. The maximum is: {}", DataTypeObject::MAX_TYPED_PATHS);
            typed_paths.emplace(path_with_type->path, data_type);
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
    thread_local static DataTypePtr type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}, Names{"paths", "values"}));
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

DataTypePtr DataTypeObject::getDynamicType() const
{
    return std::make_shared<DataTypeDynamic>(max_dynamic_types);
}

static DataTypePtr createJSON(const ASTPtr & arguments)
{
    return createObject(arguments, DataTypeObject::SchemaFormat::JSON);
}

void registerDataTypeJSON(DataTypeFactory & factory)
{
    factory.registerDataType("JSON", createJSON, DataTypeFactory::Case::Insensitive);
}

}
