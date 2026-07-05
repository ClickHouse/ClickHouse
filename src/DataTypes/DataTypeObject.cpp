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

void DataTypeObject::insertDefaultInto(IColumn & column) const
{
    auto & column_object = assert_cast<ColumnObject &>(column);
    for (auto & [path, typed_column] : column_object.getTypedPaths())
        typed_paths.at(path)->insertDefaultInto(*typed_column);
    for (auto & [_, dynamic_column] : column_object.getDynamicPathsPtrs())
        dynamic_column->insertDefault();
    column_object.getSharedDataColumn().insertDefault();
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

    /// If sub-object contains only empty objects, just use literal.
    const auto * sub_object_typed_column = assert_cast<const ColumnObject *>(sub_object_column.get());
    if (!sub_object_typed_column->hasNonEmptyRows())
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
        else if (!sub_object_typed_column->isEmptyAt(i))
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
        /// The AST may be structurally invalid, so we cannot assume that every child is an
        /// `ASTObjectTypeArgument`. Validate the cast before use.
        if (!object_type_argument)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST in {} type arguments: {}", magic_enum::enum_name(schema_format), argument->formatForErrorMessage());
        if (object_type_argument->parameter)
        {
            const auto * function = object_type_argument->parameter->as<ASTFunction>();

            if (!function || function->name != "equals")
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected parameter in {} type arguments: {}", magic_enum::enum_name(schema_format), object_type_argument->parameter->formatForErrorMessage());

            /// The `equals` function expects exactly two children: an identifier and a literal.
            /// Validate the argument list shape before indexing.
            if (!function->arguments || function->arguments->children.size() != 2)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected parameter in {} type arguments: {}. Expected expression 'max_dynamic_types=N' or 'max_dynamic_paths=N'", magic_enum::enum_name(schema_format), function->formatForErrorMessage());

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
            if (!path_with_type)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST in {} type path with type argument: {}. Expected 'path data_type'", magic_enum::enum_name(schema_format), object_type_argument->path_with_type->formatForErrorMessage());
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
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected AST in SKIP REGEXP section of {} type arguments: {}. Expected string literal with path regexp", magic_enum::enum_name(schema_format), object_type_argument->skip_path_regexp->formatForErrorMessage());

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
    factory.registerDataType("JSON", createJSON, DataTypeFactory::Case::Insensitive, Documentation{
            .description = String(R"DOCS_MD(
import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
<CardSecondary
  badgeState="success"
  badgeText=""
  description="Check out our JSON best practice guide for examples, advanced features and considerations for using the JSON type."
  icon="book"
  infoText="Read more"
  infoUrl="/docs/best-practices/use-json-where-appropriate"
  title="Looking for a guide?"
/>
</Link>
<br/>

The `JSON` type stores JavaScript Object Notation (JSON) documents in a single column.

:::note
In ClickHouse Open-Source JSON data type is marked as production ready in version 25.3. It's not recommended to use this type in production in previous versions.
:::

To declare a column of `JSON` type, you can use the following syntax:

```sql
<column_name> JSON
(
    max_dynamic_paths=N,
    max_dynamic_types=M,
    some.path TypeName,
    SKIP path.to.skip,
    SKIP REGEXP 'paths_regexp'
)
```
Where the parameters in the syntax above are defined as:

| Parameter                   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Default Value |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `max_dynamic_paths`         | An optional parameter indicating how many paths can be stored separately as sub-columns across single block of data that is stored separately (for example across single data part for MergeTree table). <br/><br/>If this limit is exceeded, all other paths will be stored together in a single structure called [shared data](#shared-data-structure).<br/><br/>There are also [ways](#controlling-the-number-of-dynamic-paths) how to change the limit on dynamic paths without changing this parameter. | `1024`        |
| `max_dynamic_types`         | An optional parameter between `1` and `255` indicating how many different data types can be stored separately inside a single path column with type `Dynamic` across single block of data that is stored separately (for example across single data part for MergeTree table). <br/><br/>If this limit is exceeded, all new types will be stored together in a single structure called `shared variant`.                                                                                    | `32`          |
| `some.path TypeName`        | An optional type hint for particular path in the JSON. Such paths will be always stored as sub-columns with specified type.                                                                                                                                                                                                                                                                                                                                                                                  |               |
| `SKIP path.to.skip`         | An optional hint for particular path that should be skipped during JSON parsing. Such paths will never be stored in the JSON column. If specified path is a nested JSON object, the whole nested object will be skipped.                                                                                                                                                                                                                                                                                     |               |
| `SKIP REGEXP 'path_regexp'` | An optional hint with a regular expression that is used to skip paths during JSON parsing. All paths that match this regular expression will never be stored in the JSON column.                                                                                                                                                                                                                                                                                                                             |               |

<WhenToUseJson />

## Creating `JSON` {#creating-json}

In this section we'll take a look at the various ways that you can create `JSON`.

### Using `JSON` in a table column definition {#using-json-in-a-table-column-definition}

```sql title="Query (Example 1)"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 1)"
┌─json────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"]}          │
│ {"f":"Hello, World!"}                       │
│ {"a":{"b":"43","e":"10"},"c":["4","5","6"]} │
└─────────────────────────────────────────────┘
```

```sql title="Query (Example 2)"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 2)"
┌─json──────────────────────────────┐
│ {"a":{"b":42},"c":["1","2","3"]}  │
│ {"a":{"b":0},"f":"Hello, World!"} │
│ {"a":{"b":43},"c":["4","5","6"]}  │
└───────────────────────────────────┘
```

### Using CAST with `::JSON` {#using-cast-with-json}

It is possible to cast various types using the special syntax `::JSON`.

#### CAST from `String` to `JSON` {#cast-from-string-to-json}

```sql title="Query"
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

#### CAST from `Tuple` to `JSON` {#cast-from-tuple-to-json}

```sql title="Query"
SET enable_named_columns_in_function_tuple = 1;
SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

#### CAST from `Map` to `JSON` {#cast-from-map-to-json}

```sql title="Query"
SET use_variant_as_common_type=1;
SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

:::note
JSON paths are stored flattened. This means that when a JSON object is formatted from a path like `a.b.c`
it is not possible to know whether the object should be constructed as `{ "a.b.c" : ... }` or `{ "a": { "b": { "c": ... } } }`.
Our implementation will always assume the latter.

For example:

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

will return:

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

and **not**:

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```
:::

## Reading JSON paths as sub-columns {#reading-json-paths-as-sub-columns}

The `JSON` type supports reading every path as a separate sub-column.
If the type of the requested path is not specified in the JSON type declaration,
then the sub column of the path will always have type [Dynamic](/sql-reference/data-types/dynamic.md).

For example:

```sql title="Query"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────┐
│ {"a":{"b":42,"g":42.42},"c":["1","2","3"],"d":"2020-01-01"} │
│ {"a":{"b":0},"d":"2020-01-02","f":"Hello, World!"}          │
│ {"a":{"b":43,"g":43.43},"c":["4","5","6"]}                  │
└─────────────────────────────────────────────────────────────┘
```

```sql title="Query (Reading JSON paths as sub-columns)"
SELECT json.a.b, json.a.g, json.c, json.d FROM test;
```

```text title="Response (Reading JSON paths as sub-columns)"
┌─json.a.b─┬─json.a.g─┬─json.c──┬─json.d─────┐
│       42 │ 42.42    │ [1,2,3] │ 2020-01-01 │
│        0 │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ    │ 2020-01-02 │
│       43 │ 43.43    │ [4,5,6] │ ᴺᵁᴸᴸ       │
└──────────┴──────────┴─────────┴────────────┘
```

You can also use `getSubcolumn` function to read subcolumns from JSON type:

```sql title="Query"
SELECT getSubcolumn(json, 'a.b'), getSubcolumn(json, 'a.g'), getSubcolumn(json, 'c'), getSubcolumn(json, 'd') FROM test;
```

```text title="Response"
┌─getSubcolumn(json, 'a.b')─┬─getSubcolumn(json, 'a.g')─┬─getSubcolumn(json, 'c')─┬─getSubcolumn(json, 'd')─┐
│                        42 │ 42.42                     │ [1,2,3]                 │ 2020-01-01              │
│                         0 │ ᴺᵁᴸᴸ                      │ ᴺᵁᴸᴸ                    │ 2020-01-02              │
│                        43 │ 43.43                     │ [4,5,6]                 │ ᴺᵁᴸᴸ                    │
└───────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

If the requested path wasn't found in the data, it will be filled with `NULL` values:

```sql title="Query"
SELECT json.non.existing.path FROM test;
```

```text title="Response"
┌─json.non.existing.path─┐
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
└────────────────────────┘
```

Let's check the data types of the returned sub-columns:

```sql title="Query"
SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM test;
```

```text title="Response"
┌─toTypeName(json.a.b)─┬─toTypeName(json.a.g)─┬─toTypeName(json.c)─┬─toTypeName(json.d)─┐
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
└──────────────────────┴──────────────────────┴────────────────────┴────────────────────┘
```

As we can see, for `a.b`, the type is `UInt32` as we specified it to be in the JSON type declaration,
and for all other sub-columns the type is `Dynamic`.

It is also possible to read sub-columns of a `Dynamic` type using the special syntax `json.some.path.:TypeName`:

```sql title="Query"
SELECT
    json.a.g.:Float64,
    dynamicType(json.a.g),
    json.d.:Date,
    dynamicType(json.d)
FROM test
```

```text title="Response"
┌─json.a.g.:`Float64`─┬─dynamicType(json.a.g)─┬─json.d.:`Date`─┬─dynamicType(json.d)─┐
│               42.42 │ Float64               │     2020-01-01 │ Date                │
│                ᴺᵁᴸᴸ │ None                  │     2020-01-02 │ Date                │
│               43.43 │ Float64               │           ᴺᵁᴸᴸ │ None                │
└─────────────────────┴───────────────────────┴────────────────┴─────────────────────┘
```

`Dynamic` sub-columns can be cast to any data type. In this case an exception will be thrown if the internal type inside `Dynamic` cannot be cast to the requested type:

```sql title="Query"
SELECT json.a.g::UInt64 AS uint
FROM test;
```

```text title="Response"
┌─uint─┐
│   42 │
│    0 │
│   43 │
└──────┘
```

```sql title="Query"
SELECT json.a.g::UUID AS float
FROM test;
```

```text title="Response"
Received exception from server:
Code: 48. DB::Exception: Received from localhost:9000. DB::Exception:
Conversion between numeric types and UUID is not supported.
Probably the passed UUID is unquoted:
while executing 'FUNCTION CAST(__table1.json.a.g :: 2, 'UUID'_String :: 1) -> CAST(__table1.json.a.g, 'UUID'_String) UUID : 0'.
(NOT_IMPLEMENTED)
```

:::note
To read subcolumns efficiently from Compact MergeTree parts make sure MergeTree setting [write_marks_for_substreams_in_compact_parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts) is enabled.
:::

## Reading JSON sub-objects as sub-columns {#reading-json-sub-objects-as-sub-columns}

The `JSON` type supports reading nested objects as sub-columns with type `JSON` using the special syntax `json.^some.path`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'), ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'), ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":"42","g":42.42}},"c":["1","2","3"],"d":{"e":{"f":{"g":"Hello, World","h":["1","2","3"]}}}} │
│ {"d":{"e":{"f":{"h":["4","5","6"]}}},"f":"Hello, World!"}                                                 │
│ {"a":{"b":{"c":"43","e":"10","g":43.43}},"c":["4","5","6"]}                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.^a.b, json.^d.e.f FROM test;
```

```text title="Response"
┌─json.^`a`.b───────────────────┬─json.^`d`.e.f──────────────────────────┐
│ {"c":"42","g":42.42}          │ {"g":"Hello, World","h":["1","2","3"]} │
│ {}                            │ {"h":["4","5","6"]}                    │
│ {"c":"43","e":"10","g":43.43} │ {}                                     │
└───────────────────────────────┴────────────────────────────────────────┘
```

:::note
When paths are stored in basic (`map`) [shared data](#shared-data-structure), reading sub-object sub-columns may be inefficient as it requires scanning the entire shared data structure. With `map_with_buckets` or `advanced` shared data serialization, reading sub-columns from shared data is highly optimized.
:::

## Reading JSON combined sub-columns {#reading-json-combined-sub-columns}

The `JSON` type supports reading a path as a **combined sub-column** using the special syntax `json.@some.path`.
A combined sub-column for a given path returns:
- The literal value stored at that path as `Dynamic`, if the path has a literal value.
- A JSON sub-object at that path as `Dynamic`, if the path has no literal value but has nested sub-paths.
- `NULL`, if neither a literal value nor any sub-paths exist for that path.

This is useful when a path may hold either a scalar value or a nested object across different rows, and is more convenient than separately querying the literal sub-column (`json.a`) and the sub-object sub-column (`json.^a`).

The following example compares all three sub-column types for path `a`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : 42, "b" : {"c" : 1, "d" : "Hello"}}'), ('{"a" : {"x": 1, "y": 2}, "b" : {"c" : 1}}'), ('{"c" : "World"}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────┐
│ {"a":42,"b":{"c":1,"d":"Hello"}}│
│ {"a":{"x":1,"y":2},"b":{"c":1}}│
│ {"c":"World"}                   │
└─────────────────────────────────┘
```

```sql title="Query"
SELECT
    json.a,
    dynamicType(json.a),
    json.^a,
    toTypeName(json.^a),
    json.@a,
    dynamicType(json.@a)
FROM test;
```

```text title="Response"
┌─json.a─┬─dynamicType(json.a)─┬─json.^a───────┬─toTypeName(json.^a)─┬─json.@a───────┬─dynamicType(json.@a)─┐
│ 42     │ Int64               │ {}            │ JSON                │ 42            │ Int64                │
│ NULL   │ None                │ {"x":1,"y":2} │ JSON                │ {"x":1,"y":2} │ JSON                 │
│ NULL   │ None                │ {}            │ JSON                │ NULL          │ None                 │
└────────┴─────────────────────┴───────────────┴─────────────────────┴───────────────┴──────────────────────┘
```

- Row 1: `a` holds a literal `42`. `json.a` returns it as `Dynamic(Int64)`, `json.^a` returns an empty sub-object `{}` (no nested keys under `a`), and `json.@a` returns the literal `42`.
- Row 2: `a` holds a nested object. `json.a` returns `NULL` (no literal at that path), `json.^a` returns the sub-object as `JSON`, and `json.@a` also returns the sub-object as `Dynamic(JSON)`.
- Row 3: `a` is absent entirely. Both `json.a` and `json.@a` return `NULL`, while `json.^a` returns an empty `{}`.

:::note
When paths are stored in basic (`map`) [shared data](#shared-data-structure), reading combined sub-columns may be inefficient as it requires scanning the entire shared data structure. With `map_with_buckets` or `advanced` shared data serialization, reading sub-columns from shared data is highly optimized.
:::

## Type inference for paths {#type-inference-for-paths}

During parsing of `JSON`, ClickHouse tries to detect the most appropriate data type for each JSON path.
It works similarly to [automatic schema inference from input data](/interfaces/schema-inference.md),
and is controlled by the same settings:

- [input_format_try_infer_dates](/operations/settings/formats#input_format_try_infer_dates)
- [input_format_try_infer_datetimes](/operations/settings/formats#input_format_try_infer_datetimes)
- [schema_inference_make_columns_nullable](/operations/settings/formats#schema_inference_make_columns_nullable)
- [input_format_json_try_infer_numbers_from_strings](/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
- [input_format_json_infer_incomplete_types_as_strings](/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
- [input_format_json_read_numbers_as_strings](/operations/settings/formats#input_format_json_read_numbers_as_strings)
- [input_format_json_read_bools_as_strings](/operations/settings/formats#input_format_json_read_bools_as_strings)
- [input_format_json_read_bools_as_numbers](/operations/settings/formats#input_format_json_read_bools_as_numbers)
- [input_format_json_read_arrays_as_strings](/operations/settings/formats#input_format_json_read_arrays_as_strings)
- [input_format_json_infer_array_of_dynamic_from_array_of_different_types](/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

Let's take a look at some examples:

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=1, input_format_try_infer_datetimes=1;
```

```text title="Response"
┌─paths_with_types─────────────────┐
│ {'a':'Date','b':'DateTime64(9)'} │
└──────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
```

```text title="Response"
┌─paths_with_types────────────┐
│ {'a':'String','b':'String'} │
└─────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=1;
```

```text title="Response"
┌─paths_with_types───────────────┐
│ {'a':'Array(Nullable(Int64))'} │
└────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=0;
```

```text title="Response"
┌─paths_with_types─────┐
│ {'a':'Array(Int64)'} │
└──────────────────────┘
```

## Handling arrays of JSON objects {#handling-arrays-of-json-objects}

JSON paths that contain an array of objects are parsed as type `Array(JSON)` and inserted into a `Dynamic` column for the path.
To read an array of objects, you can extract it from the `Dynamic` column as a sub-column:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES
('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
('{"a" : {"b" : [1, 2, 3]}}'),
('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":[{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}},{"c":"43"},{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}]}} │
│ {"a":{"b":["1","2","3"]}}                                                                                                                                               │
│ {"a":{"b":[{"c":"44","f":[[{"h":"2020-01-02"}]]},{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}]}}                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.a.b, dynamicType(json.a.b) FROM test;
```

```text title="Response"
┌─json.a.b──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─dynamicType(json.a.b)────────────────────────────────────┐
│ ['{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}}','{"c":"43"}','{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}'] │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
│ [1,2,3]                                                                                                                                                           │ Array(Nullable(Int64))                                   │
│ ['{"c":"44","f":[[{"h":"2020-01-02"}]]}','{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}']                                                  │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

As you may have noticed, the `max_dynamic_types`/`max_dynamic_paths` parameters of the nested `JSON` type got reduced compared to the default values.
This is needed to avoid the number of sub-columns growing uncontrollably on nested arrays of JSON objects.

Let's try to read sub-columns from a nested `JSON` column:

```sql title="Query"
SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

We can avoid writing `Array(JSON)` sub-column names using a special syntax:

```sql title="Query"
SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

The number of `[]` after the path indicates the array level. For example, `json.path[][]` will be transformed to `json.path.:Array(Array(JSON))`

Let's check the paths and types inside our `Array(JSON)`:

```sql title="Query"
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b[]))) FROM test;
```

```text title="Response"
┌─arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b.:`Array(JSON)`)))──┐
│ ('c','Int64')                                                         │
│ ('d','String')                                                        │
│ ('f','Array(Array(JSON(max_dynamic_types=8, max_dynamic_paths=64)))') │
│ ('k.j','Int64')                                                       │
│ ('e','Array(Nullable(Int64))')                                        │
└───────────────────────────────────────────────────────────────────────┘
```

Let's read sub-columns from an `Array(JSON)` column:

```sql title="Query"
SELECT json.a.b[].c.:Int64, json.a.b[].f[][].g.:Float64, json.a.b[].f[][].h.:Date FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c.:`Int64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.g.:`Float64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.h.:`Date`─┐
│ [42,43,NULL]                       │ [[[42.42]],[],[[43.43]]]                                     │ [[[NULL]],[],[['2020-01-01']]]                            │
│ []                                 │ []                                                           │ []                                                        │
│ [44,NULL]                          │ [[[NULL]],[[44.44]]]                                         │ [[['2020-01-02']],[[NULL]]]                               │
└────────────────────────────────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

We can also read sub-object sub-columns from a nested `JSON` column:

```sql title="Query"
SELECT json.a.b[].^k FROM test
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.^`k`─────────┐
│ ['{"j":"1000"}','{}','{"j":"2000"}'] │
│ []                                   │
│ ['{}','{"j":"3000"}']                │
└──────────────────────────────────────┘
```

## Handling JSON keys with NULL {#handling-json-keys-with-nulls}

In our JSON implementation `null` and absence of the value are considered equivalent:

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

It means that it's impossible to determine whether the original JSON data contained some path with the NULL value or didn't contain it at all.

## Handling JSON keys with dots {#handling-json-keys-with-dots}

Internally JSON column stores all paths and values in a flattened form. It means that by default these 2 objects are considered as the same:
```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

They both will be stored internally as a pair of path `a.b` and value `42`. During formatting of JSON we always form nested objects based on the path parts separated by dot:

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

As you can see, initial JSON `{"a.b" : 42}` is now formatted as `{"a" : {"b" : 42}}`.

This limitation also leads to the failure of parsing valid JSON objects like this:

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

If you want to keep keys with dots and avoid formatting them as nested objects, you can enable
setting [json_type_escape_dots_in_keys](/operations/settings/formats#json_type_escape_dots_in_keys) (available starting from version `25.8`). In this case during parsing all dots in JSON keys will be
escaped into `%2E` and unescaped back during formatting.

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a.b":"42"} │ ['a.b']             │ ['a%2Eb']           │
└──────────────────┴──────────────┴─────────────────────┴─────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, JSONAllPaths(json);
```

```text title="Response"
┌─json──────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ ['a%2Eb','a.b']    │
└───────────────────────────────────────┴────────────────────┘
```

To read key with escaped dot as a subcolumn you have to use escaped dot in the subcolumn name:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

Note: due to identifiers parser and analyzer limitations subcolumn `` json.`a.b` `` is equivalent to subcolumn `json.a.b` and won't read path with escaped dot:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

Also, if you want to specify a hint for a JSON path that contains keys with dots (or use it in the `SKIP`/`SKIP REGEX` sections), you have to use escaped dots in the hint:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(`a%2Eb` UInt8) as json, json.`a%2Eb`, toTypeName(json.`a%2Eb`);
```

```text title="Response"
┌─json────────────────────────────────┬─json.a%2Eb─┬─toTypeName(json.a%2Eb)─┐
│ {"a.b":42,"a":{"b":"Hello World!"}} │         42 │ UInt8                  │
└─────────────────────────────────────┴────────────┴────────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(SKIP `a%2Eb`) as json, json.`a%2Eb`;
```

```text title="Response"
┌─json───────────────────────┬─json.a%2Eb─┐
│ {"a":{"b":"Hello World!"}} │ ᴺᵁᴸᴸ       │
└────────────────────────────┴────────────┘
```

)DOCS_MD")
            // The JSON documentation is concatenated at runtime from several string literals because a single
            // string literal would exceed the 65536-byte length that C++ compilers are required to support.
            + R"DOCS_MD(## Reading JSON type from data {#reading-json-type-from-data}

All text formats
([`JSONEachRow`](/interfaces/formats/JSONEachRow),
[`TSV`](/interfaces/formats/TabSeparated),
[`CSV`](/interfaces/formats/CSV),
[`CustomSeparated`](/interfaces/formats/CustomSeparated),
[`Values`](/interfaces/formats/Values), etc.) support reading the `JSON` type.

Examples:

```sql title="Query"
SELECT json FROM format(JSONEachRow, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP d.e, SKIP REGEXP \'b.*\')', '
{"json" : {"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}}
{"json" : {"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}}
{"json" : {"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}}
{"json" : {"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}}
{"json" : {"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}}
')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

For text formats like `CSV`/`TSV`/etc, `JSON` is parsed from a string containing the JSON object:

```sql title="Query"
SELECT json FROM format(TSV, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP REGEXP \'b.*\')',
'{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

## Reaching the limit of dynamic paths inside JSON {#reaching-the-limit-of-dynamic-paths-inside-json}

The `JSON` data type can store only a limited number of paths as separate sub-columns internally.
By default, this limit is `1024`, but you can change it in the type declaration using parameter `max_dynamic_paths`.

When the limit is reached, all new paths inserted to a `JSON` column will be stored in a single shared data structure.
It's still possible to read such paths as sub-columns,
but it might be less efficient ([see section about shared data](#shared-data-structure)).
This limit is needed to avoid having an enormous number of different sub-columns that can make the table unusable.

Let's see what happens when the limit is reached in a few different scenarios.

### Reaching the limit during data parsing {#reaching-the-limit-during-data-parsing}

During parsing of `JSON` objects from data, when the limit is reached for the current block of data,
all new paths will be stored in a shared data structure. We can use the following two introspection functions `JSONDynamicPaths`, `JSONSharedDataPaths`:

```sql title="Query"
SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM format(JSONEachRow, 'json JSON(max_dynamic_paths=3)', '
{"json" : {"a" : {"b" : 42}, "c" : [1, 2, 3]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-01"}}
{"json" : {"a" : {"b" : 44}, "c" : [4, 5, 6]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-02", "e" : "Hello", "f" : {"g" : 42.42}}}
{"json" : {"a" : {"b" : 43}, "c" : [7, 8, 9], "f" : {"g" : 43.43}, "h" : "World"}}
')
```

```text title="Response"
┌─json───────────────────────────────────────────────────────────┬─JSONDynamicPaths(json)─┬─JSONSharedDataPaths(json)─┐
│ {"a":{"b":"42"},"c":["1","2","3"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-01"}                              │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"44"},"c":["4","5","6"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-02","e":"Hello","f":{"g":42.42}}  │ ['a.b','c','d']        │ ['e','f.g']               │
│ {"a":{"b":"43"},"c":["7","8","9"],"f":{"g":43.43},"h":"World"} │ ['a.b','c','d']        │ ['f.g','h']               │
└────────────────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────┘
```

As we can see, after inserting paths `e` and `f.g` the limit was reached,
and they got inserted into a shared data structure.

### During merges of data parts in MergeTree table engines {#during-merges-of-data-parts-in-mergetree-table-engines}

During a merge of several data parts in a `MergeTree` table the `JSON` column in the resulting data part can reach the limit of dynamic paths
and won't be able to store all paths from source parts as sub-columns.
In this case, ClickHouse chooses what paths will remain as sub-columns after merge and what paths will be stored in the shared data structure.
In most cases, ClickHouse tries to keep paths that contain
the largest number of non-null values and move the rarest paths to the shared data structure. This does, however, depend on the implementation.

Let's see an example of such a merge.
First, let's create a table with a `JSON` column, set the limit of dynamic paths to `3` and then insert values with `5` different paths:

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

Each insert will create a separate data part with the `JSON` column containing a single path:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       5 │ ['a']         │ []                │ all_1_1_0 │
│       4 │ ['b']         │ []                │ all_2_2_0 │
│       3 │ ['c']         │ []                │ all_3_3_0 │
│       2 │ ['d']         │ []                │ all_4_4_0 │
│       1 │ ['e']         │ []                │ all_5_5_0 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

Now, let's merge all parts into one and see what will happen:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│      15 │ ['a','b','c'] │ ['d','e']         │ all_1_5_2 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

As we can see, ClickHouse kept the most frequent paths `a`, `b` and `c` and moved paths `d` and `e` to a shared data structure.

## Shared data structure {#shared-data-structure}

As was described in the previous section, when the `max_dynamic_paths` limit is reached all new paths are stored in a single shared data structure.
In this section we will look into the details of the shared data structure and how we read paths sub-columns from it.

See section ["introspection functions"](/sql-reference/data-types/newjson#introspection-functions) for details of functions used for inspecting the contents of a JSON column.

### Shared data structure in memory {#shared-data-structure-in-memory}

In memory, shared data structure is just a sub-column with type `Map(String, String)` that stores mapping from a flattened JSON path to a binary encoded value.
To extract a path subcolumn from it, we just iterate over all rows in this `Map` column and try to find the requested path and its values.

### Shared data structure in MergeTree parts {#shared-data-structure-in-merge-tree-parts}

In [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables we store data in data parts that stores everything on disk (local or remote). And data on disk can be stored in a different way compared to memory.
Currently, there are 3 different shared data structure serializations in MergeTree data parts: `map`, `map_with_buckets`
and `advanced`.

The serialization version is controlled by MergeTree
settings [object_shared_data_serialization_version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
and [object_shared_data_serialization_version_for_zero_level_parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
(zero level part is the part created during inserting data into the table, during merges parts have higher level).

Note: changing shared data structure serialization is supported only
for `v3` [object serialization version](../../operations/settings/merge-tree-settings.md#object_serialization_version)

#### Map {#shared-data-map}

In `map` serialization version shared data is serialized as a single column with type `Map(String, String)` the same as it's stored in
memory. To read path sub-column from this type of serialization ClickHouse reads the whole `Map` column and
extracts the requested path in memory.

This serialization is efficient for writing data and reading the whole `JSON` column, but it's not efficient for reading paths sub-columns.

#### Map with buckets {#shared-data-map-with-buckets}

In `map_with_buckets` serialization version shared data is serialized as `N` columns ("buckets") with type `Map(String, String)`.
Each such bucket contains only subset of paths. To read path sub-column from this type of serialization ClickHouse
reads the whole `Map` column from a single bucket and extracts the requested path in memory.

This serialization is less efficient for writing data and reading the whole `JSON` column, but it's more efficient for reading paths sub-columns
because it reads data only from required buckets.

Number of buckets `N` is controlled by MergeTree settings [object_shared_data_buckets_for_compact_part](
../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (8 by default)
and [object_shared_data_buckets_for_wide_part](
../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (32 by default).
The maximum allowed value for both settings is 256.

#### Advanced {#shared-data-advanced}

In `advanced` serialization version shared data is serialized in a special data structure that maximizes the performance
of paths sub-columns reading by storing some additional information that allows to read only the data of requested paths.
This serialization also supports buckets, so each bucket contains only sub-set of paths.

This serialization is quite inefficient for writing data (so it's not recommended to use this serialization for zero-level parts), reading the whole `JSON` column is slightly less efficient compared to `map` serialization, but it's very efficient for reading paths sub-columns.

Note: because of storing some additional information inside the data structure, the disk storage size is higher with this serialization compared to
`map` and `map_with_buckets` serializations.

For more detailed overview of the new shared data serializations and implementation details read the [blog post](https://clickhouse.com/blog/json-data-type-gets-even-better).

## Controlling the number of dynamic paths inside JSON in MergeTree parts {#controlling-the-number-of-dynamic-paths}

The main way to set a limit on dynamic paths in JSON is to use `max_dynamic_paths` parameter inside the JSON type declaration.
But changing `max_dynamic_paths` for existing columns requires running `ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)` that will start a background mutation that will rewrite all existing parts.
Such mutation can be really heavy and can affect the server performance until the mutation is finished. To avoid this, you can use these 3 settings that can help you to change the limit on dynamic paths in MergeTree tables for new data parts:

- `merge_max_dynamic_subcolumns_in_wide_part` - a MergeTree setting that limits the number of dynamic subcolumns for each JSON column during merge into a Wide data part.
- `merge_max_dynamic_subcolumns_in_compact_part` - a MergeTree setting that limits the number of dynamic subcolumns for each JSON column during merge into a Compact data part.
- `max_dynamic_subcolumns_in_json_type_parsing` - a session setting that limits the number of dynamic subcolumns for each JSON column during parsing of JSON data into a JSON column.

Note: limit on dynamic paths cannot exceed the value specified in `max_dynamic_paths` parameter, even if values of described settings are higher.

## Introspection functions {#introspection-functions}

There are several functions that can help to inspect the content of the JSON column:
- [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
- [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
- [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
- [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
- [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
- [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
- [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
- [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
- [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**Examples**

Let's investigate the content of the [GH Archive](https://www.gharchive.org/) dataset for the date `2020-01-01`:

```sql title="Query"
SELECT arrayJoin(distinctJSONPaths(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
```

```text title="Response"
┌─arrayJoin(distinctJSONPaths(json))─────────────────────────┐
│ actor.avatar_url                                           │
│ actor.display_login                                        │
│ actor.gravatar_id                                          │
│ actor.id                                                   │
│ actor.login                                                │
│ actor.url                                                  │
│ created_at                                                 │
│ id                                                         │
│ org.avatar_url                                             │
│ org.gravatar_id                                            │
│ org.id                                                     │
│ org.login                                                  │
│ org.url                                                    │
│ payload.action                                             │
│ payload.before                                             │
│ payload.comment._links.html.href                           │
│ payload.comment._links.pull_request.href                   │
│ payload.comment._links.self.href                           │
│ payload.comment.author_association                         │
│ payload.comment.body                                       │
│ payload.comment.commit_id                                  │
│ payload.comment.created_at                                 │
│ payload.comment.diff_hunk                                  │
│ payload.comment.html_url                                   │
│ payload.comment.id                                         │
│ payload.comment.in_reply_to_id                             │
│ payload.comment.issue_url                                  │
│ payload.comment.line                                       │
│ payload.comment.node_id                                    │
│ payload.comment.original_commit_id                         │
│ payload.comment.original_position                          │
│ payload.comment.path                                       │
│ payload.comment.position                                   │
│ payload.comment.pull_request_review_id                     │
...
│ payload.release.node_id                                    │
│ payload.release.prerelease                                 │
│ payload.release.published_at                               │
│ payload.release.tag_name                                   │
│ payload.release.tarball_url                                │
│ payload.release.target_commitish                           │
│ payload.release.upload_url                                 │
│ payload.release.url                                        │
│ payload.release.zipball_url                                │
│ payload.size                                               │
│ public                                                     │
│ repo.id                                                    │
│ repo.name                                                  │
│ repo.url                                                   │
│ type                                                       │
└─arrayJoin(distinctJSONPaths(json))─────────────────────────┘
```

```sql title="Query"
SELECT arrayJoin(distinctJSONPathsAndTypes(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
SETTINGS date_time_input_format = 'best_effort'
```

```text title="Response"
┌─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┐
│ ('actor.avatar_url',['String'])                             │
│ ('actor.display_login',['String'])                          │
│ ('actor.gravatar_id',['String'])                            │
│ ('actor.id',['Int64'])                                      │
│ ('actor.login',['String'])                                  │
│ ('actor.url',['String'])                                    │
│ ('created_at',['DateTime'])                                 │
│ ('id',['String'])                                           │
│ ('org.avatar_url',['String'])                               │
│ ('org.gravatar_id',['String'])                              │
│ ('org.id',['Int64'])                                        │
│ ('org.login',['String'])                                    │
│ ('org.url',['String'])                                      │
│ ('payload.action',['String'])                               │
│ ('payload.before',['String'])                               │
│ ('payload.comment._links.html.href',['String'])             │
│ ('payload.comment._links.pull_request.href',['String'])     │
│ ('payload.comment._links.self.href',['String'])             │
│ ('payload.comment.author_association',['String'])           │
│ ('payload.comment.body',['String'])                         │
│ ('payload.comment.commit_id',['String'])                    │
│ ('payload.comment.created_at',['DateTime'])                 │
│ ('payload.comment.diff_hunk',['String'])                    │
│ ('payload.comment.html_url',['String'])                     │
│ ('payload.comment.id',['Int64'])                            │
│ ('payload.comment.in_reply_to_id',['Int64'])                │
│ ('payload.comment.issue_url',['String'])                    │
│ ('payload.comment.line',['Int64'])                          │
│ ('payload.comment.node_id',['String'])                      │
│ ('payload.comment.original_commit_id',['String'])           │
│ ('payload.comment.original_position',['Int64'])             │
│ ('payload.comment.path',['String'])                         │
│ ('payload.comment.position',['Int64'])                      │
│ ('payload.comment.pull_request_review_id',['Int64'])        │
...
│ ('payload.release.node_id',['String'])                      │
│ ('payload.release.prerelease',['Bool'])                     │
│ ('payload.release.published_at',['DateTime'])               │
│ ('payload.release.tag_name',['String'])                     │
│ ('payload.release.tarball_url',['String'])                  │
│ ('payload.release.target_commitish',['String'])             │
│ ('payload.release.upload_url',['String'])                   │
│ ('payload.release.url',['String'])                          │
│ ('payload.release.zipball_url',['String'])                  │
│ ('payload.size',['Int64'])                                  │
│ ('public',['Bool'])                                         │
│ ('repo.id',['Int64'])                                       │
│ ('repo.name',['String'])                                    │
│ ('repo.url',['String'])                                     │
│ ('type',['String'])                                         │
└─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┘
```

## ALTER MODIFY COLUMN to JSON type {#alter-modify-column-to-json-type}

It's possible to alter an existing table and change the type of the column to the new `JSON` type. Right now only `ALTER` from a `String` type is supported.

**Example**

```sql title="Query"
CREATE TABLE test (json String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test VALUES ('{"a" : 42}'), ('{"a" : 43, "b" : "Hello"}'), ('{"a" : 44, "b" : [1, 2, 3]}'), ('{"c" : "2020-01-01"}');
ALTER TABLE test MODIFY COLUMN json JSON;
SELECT json, json.a, json.b, json.c FROM test;
```

```text title="Response"
┌─json─────────────────────────┬─json.a─┬─json.b──┬─json.c─────┐
│ {"a":"42"}                   │ 42     │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ       │
│ {"a":"43","b":"Hello"}       │ 43     │ Hello   │ ᴺᵁᴸᴸ       │
│ {"a":"44","b":["1","2","3"]} │ 44     │ [1,2,3] │ ᴺᵁᴸᴸ       │
│ {"c":"2020-01-01"}           │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ    │ 2020-01-01 │
└──────────────────────────────┴────────┴─────────┴────────────┘
```

## Lazy Type Hints (Experimental) {#lazy-type-hints}

:::note
This feature is experimental and requires the setting `allow_experimental_json_lazy_type_hints` to be enabled.
:::

When you add or modify type hints on a JSON column using `ALTER TABLE ... MODIFY COLUMN`, ClickHouse normally rewrites all data parts to materialize the new type hints. For tables with large amounts of historical data (hundreds of terabytes), this can be extremely expensive.

**Lazy type hints** allow adding type hints as a metadata-only operation without rewriting existing data:

- **Old parts**: Type hints are applied at query time by casting from `Dynamic` to the hinted type
- **New parts**: Type hints are materialized during `INSERT` operations
- **Merges**: Type hints are materialized when parts are merged

This means you can add type hints instantly, and the data will be gradually converted as normal background merges occur.

### Enabling Lazy Type Hints {#enabling-lazy-type-hints}

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

### Example {#lazy-type-hints-example}

```sql title="Query"
-- Create a table and insert data
CREATE TABLE test_lazy (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_lazy VALUES ('{"user_id": "123", "score": "95.5"}');

-- Enable experimental setting
SET allow_experimental_json_lazy_type_hints = 1;

-- Add type hints - this completes instantly without mutation
ALTER TABLE test_lazy MODIFY COLUMN json JSON(user_id UInt64, score Float64);

-- Query the data - type hints are applied at read time
SELECT json.user_id, toTypeName(json.user_id), json.score, toTypeName(json.score) FROM test_lazy;
```

```text title="Response"
┌─json.user_id─┬─toTypeName(json.user_id)─┬─json.score─┬─toTypeName(json.score)─┐
│          123 │ UInt64                   │       95.5 │ Float64                │
└──────────────┴──────────────────────────┴────────────┴────────────────────────┘
```

### Verifying No Mutation Occurred {#verifying-no-mutation-occurred}

You can verify that the `ALTER` completed without a mutation by checking the `system.mutations` table:

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

With lazy type hints enabled, this query returns no rows, confirming the operation was metadata-only.

### Materializing Type Hints {#materializing-type-hints}

To materialize type hints in existing data, you can either:

1. **Wait for background merges**: ClickHouse will automatically materialize type hints when parts are merged
2. **Force merge**: Use `OPTIMIZE TABLE test_lazy FINAL` to merge all parts immediately
3. **Rewrite parts**: Use `ALTER TABLE test_lazy REWRITE PARTS` to rewrite parts with the new metadata

### Limitations {#lazy-type-hints-limitations}

- This feature is experimental and may change in future versions
- Query-time type conversion can have significant performance overhead compared to pre-materialized types, especially for large JSON objects
- The feature only applies when modifying `typed_paths` (type hints); other JSON parameters like `max_dynamic_paths`, `SKIP`, or `SKIP REGEXP` still require mutations

## Comparison between values of the JSON type {#comparison-between-values-of-the-json-type}

JSON objects are compared similarly to Maps.

For example:

```sql title="Query"
CREATE TABLE test (json1 JSON, json2 JSON) ENGINE=Memory;
INSERT INTO test FORMAT JSONEachRow
{"json1" : {}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : [1, 2, 3]}}
{"json1" : {"a" : 42}, "json2" : {"a" : "Hello"}}
{"json1" : {"a" : 42}, "json2" : {"b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42, "b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41, "b" : 42}}

SELECT json1, json2, json1 < json2, json1 = json2, json1 > json2 FROM test;
```

```text title="Response"
┌─json1──────┬─json2───────────────┬─less(json1, json2)─┬─equals(json1, json2)─┬─greater(json1, json2)─┐
│ {}         │ {}                  │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {}                  │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"41"}          │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"42"}          │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {"a":["1","2","3"]} │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"Hello"}       │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"b":"42"}          │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"42","b":"42"} │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"41","b":"42"} │                  0 │                    0 │                     1 │
└────────────┴─────────────────────┴────────────────────┴──────────────────────┴───────────────────────┘
```

**Note:** when 2 paths contain values of different data types, they are compared according to [comparison rule](/sql-reference/data-types/variant#comparing-values-of-variant-data) of `Variant` data type.

## Data skipping indexes for JSON {#data-skipping-indexes-for-json}

[Data skipping indexes](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) can be used with `JSON` columns in three ways:

1. **Indexes on specific subcolumns** — create a standard skip index on a known JSON path, just like on a regular column. This indexes the *values* at that path.
2. **Path-based indexes with `JSONAllPaths`** — index the *set of paths* present in each granule to skip granules that cannot contain the queried path.
3. **Value-based indexes with `JSONAllValues`** — index *all values* across all JSON paths using a [text index](/engines/table-engines/mergetree-family/textindexes.md) to accelerate full-text search on any JSON subcolumn with a single index.

### Indexes on specific subcolumns {#json-indexes-on-subcolumns}

You can create a skip index on any JSON subcolumn using the same syntax as for regular columns.
Any [supported index type](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) works (`minmax`, `set`, `bloom_filter`, `tokenbf_v1`, `ngrambf_v1`, etc.).

There are two ways to reference a JSON subcolumn in an index expression:

- **Typed path** declared in the JSON type hint — access by name directly: `json.a`.
- **Dynamic path** with explicit cast — use the `::` cast syntax: `json.b::String`.

You can also use expressions that combine multiple subcolumns, for example `json.a || json.b::String`.

#### Example {#json-indexes-on-subcolumns-example}

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id UInt32),
    INDEX idx_sensor data.sensor_id TYPE minmax GRANULARITY 1,
    INDEX idx_location data.location::String TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

The `minmax` index on the typed subcolumn `data.sensor_id` narrows the scan to matching granules:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id < 2;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: minmax GRANULARITY 1
        Parts: 1/2
        Granules: 2/8
```

The `bloom_filter` index on the cast subcolumn `data.location::String` also works:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/8
```

### Path-based indexes with JSONAllPaths {#json-indexes-jsonallpaths}

[Data skipping indexes](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) can also be created on `JSON` columns using the [`JSONAllPaths`](/sql-reference/functions/json-functions#JSONAllPaths) function.
This works similarly to creating skip indexes on [`Map`](/sql-reference/data-types/map) columns via `mapKeys` — the index stores the set of JSON paths present in each granule and uses it to skip granules that cannot contain the queried path.

#### Supported index types {#json-indexes-jsonallpaths-supported-types}

`JSONAllPaths` can be used with the following skip index types:
- [`bloom_filter`](/engines/table-engines/mergetree-family/mergetree#bloom-filter) — supports `equals`, `in`, and `IS NOT NULL`.
- [`tokenbf_v1`](/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — supports `equals` and `IS NOT NULL`.
- [`ngrambf_v1`](/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — supports `equals` and `IS NOT NULL`.
- [`text`](/engines/table-engines/mergetree-family/textindexes) (inverted index) — supports `equals`, `in` and `IS NOT NULL`.

#### Example {#json-indexes-jsonallpaths-example}

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

You can use `EXPLAIN indexes = 1` to verify that the skip index is being used. When a path exists only in one part, the index skips the other part:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

When a path does not exist in any part, all parts and granules are skipped:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` also uses the index — it skips granules where the path is absent (since the value would be `NULL`):

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

#### How it works {#json-indexes-jsonallpaths-how-it-works}

The `JSONAllPaths(json_column)` expression produces an `Array(String)` containing all paths present in a JSON value.
The skip index stores these path strings in its data structure (bloom filter or inverted index).
When a query filters on `json.some.path`, the index checks whether the string `"some.path"` is present in the index for each granule and skips granules where it is absent.

#### Safety with missing paths {#json-indexes-jsonallpaths-safety-with-missing-paths}

When a JSON path is absent from a granule, the subcolumn evaluates to:
- `NULL` for `Dynamic` type (e.g., `json.path`) and `Nullable` typed subcolumns (e.g., `json.path.:Int64`) — comparisons with `NULL` always return false, so skipping is safe.
- The type's default value for non-`Nullable` CAST expressions (e.g., `json.path::Int64` produces `0` when the path is missing) — skipping is safe only when the compared value differs from the default. The index automatically handles this distinction.

### Full-text search with JSONAllValues {#json-indexes-jsonallvalues}

[Text indexes](/engines/table-engines/mergetree-family/textindexes.md) can be used to accelerate full-text search on JSON columns via the [`JSONAllValues`](/sql-reference/functions/json-functions#JSONAllValues) function.
`JSONAllValues` returns all values from a JSON column as `Array(String)`, which can be indexed by a text index.
A single index on `JSONAllValues(json_column)` covers all JSON paths, enabling full-text search on any subcolumn without creating separate indexes for each path.

See [Value-based indexes with JSONAllValues](/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues) in the text indexes documentation for details and examples.

## Tips for better usage of the JSON type {#tips-for-better-usage-of-the-json-type}

Before creating `JSON` column and loading data into it, consider the following tips:

- Investigate your data and specify as many path hints with types as you can. It will make storage and reading much more efficient.
- Think about what paths you will need and what paths you will never need. Specify paths that you won't need in the `SKIP` section, and `SKIP REGEXP` section if needed. This will improve the storage.
- Don't set the `max_dynamic_paths` parameter to very high values, as it can make storage and reading less efficient.
  While highly dependent on system parameters such as memory, CPU, etc., a general rule of thumb would be to not set `max_dynamic_paths` greater than 10 000 for the local filesystem storage and 1024 for the remote filesystem storage.

## Further Reading {#further-reading}

- [How we built a new powerful JSON data type for ClickHouse](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
- [The billion docs JSON Challenge: ClickHouse vs. MongoDB, Elasticsearch, and more](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)
)DOCS_MD",
            .syntax = "JSON",
            .examples = {},
            .related = {"Dynamic"},
        });
}

}
