#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <Common/StringWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include "config.h"

#include <algorithm>
#include <optional>
#include <span>

#if USE_RAPIDJSON

/// Prevent stack overflow:
#define RAPIDJSON_PARSE_DEFAULT_FLAGS (kParseIterativeFlag)

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>

#include <Common/JSONParsers/RapidJSONMemoryTrackerAllocator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_DEEP_RECURSION;
}

struct FunctionConvertSettings;
using FunctionConvertSettingsPtr = std::shared_ptr<const FunctionConvertSettings>;

FunctionConvertSettingsPtr createFunctionConvertSettings(
    const ContextPtr & context,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior);

FunctionBasePtr createFunctionBaseCast(
    const FunctionConvertSettingsPtr & settings,
    const char * name,
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    std::optional<CastDiagnostic> diagnostic,
    CastType cast_type);

namespace
{
    /// rapidjson types whose DOM, parser stack and output buffer are all accounted against the
    /// memory tracker, so a pathological input cannot allocate without bound (see
    /// RapidJSONMemoryTrackerAllocator).
    using TrackedPoolAllocator = rapidjson::MemoryPoolAllocator<RapidJSONMemoryTrackerAllocator>;
    using TrackedValue = rapidjson::GenericValue<rapidjson::UTF8<char>, TrackedPoolAllocator>;
    using TrackedDocument = rapidjson::GenericDocument<rapidjson::UTF8<char>, TrackedPoolAllocator, RapidJSONMemoryTrackerAllocator>;
    using TrackedStringBuffer = rapidjson::GenericStringBuffer<rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;
    using TrackedWriter
        = rapidjson::Writer<TrackedStringBuffer, rapidjson::UTF8<char>, rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;

    /// Parsing is iterative (see RAPIDJSON_PARSE_DEFAULT_FLAGS above), but copying, merging and
    /// serializing a document all recurse over its tree, so a valid but deeply nested document
    /// would exhaust the thread stack. Reject such documents right after parsing; the check itself
    /// is iterative.
    constexpr size_t max_json_merge_patch_depth = 1000;

    void checkJSONDepth(const TrackedValue & root)
    {
        VectorWithMemoryTracking<std::pair<const TrackedValue *, size_t>> to_visit;
        to_visit.emplace_back(&root, 1);

        while (!to_visit.empty())
        {
            const auto [value, depth] = to_visit.back();
            to_visit.pop_back();

            if (depth > max_json_merge_patch_depth)
                throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
                    "Too deep nesting in a JSON document passed to function JSONMergePatch: the limit is {}",
                    max_json_merge_patch_depth);

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

    void parseJSONDocument(std::string_view str, TrackedDocument & document)
    {
        document.Parse(std::string{str}.c_str());

        if (document.HasParseError())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong JSON string to merge: {}", rapidjson::GetParseError_En(document.GetParseError()));

        if (!document.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong JSON string to merge. Expected JSON object");

        checkJSONDepth(document);
    }

    void mergeObjects(TrackedValue & dest, const TrackedValue & src, TrackedPoolAllocator & allocator)
    {
        if (!src.IsObject())
            return;

        for (auto it = src.MemberBegin(); it != src.MemberEnd(); ++it)
        {
            TrackedValue key(it->name, allocator);
            TrackedValue value(it->value, allocator);
            if (dest.HasMember(key))
            {
                if (dest[key].IsObject() && value.IsObject())
                    mergeObjects(dest[key], value, allocator);
                else
                    dest[key] = value;
            }
            else
            {
                dest.AddMember(key, value, allocator);
            }
        }
    }

    void writeJSONDocument(const TrackedValue & value, ColumnString & column)
    {
        TrackedStringBuffer buffer;
        TrackedWriter writer(buffer);

        value.Accept(writer);
        column.insertData(buffer.GetString(), buffer.GetSize());
    }

    const ColumnString & getStringColumn(const IColumn & column)
    {
        const auto * column_string = checkAndGetColumn<ColumnString>(&column);
        if (!column_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments of function JSONMergePatch must be strings");
        return *column_string;
    }

    /// The same conversion as `createInternalCast` with the query context.
    ColumnPtr castColumnTo(const ColumnPtr & column, const DataTypePtr & from, const DataTypePtr & to, const FunctionConvertSettingsPtr & convert_settings)
    {
        ColumnsWithTypeAndName arguments{
            {column, from, ""},
            {DataTypeString().createColumnConst(column->size(), to->getName()), std::make_shared<DataTypeString>(), ""}};
        auto cast = createFunctionBaseCast(convert_settings, "_CAST", arguments, to, {}, CastType::nonAccurate);
        return cast->execute(arguments, to, column->size(), /* dry_run = */ false);
    }

    /// The paths of a `JSON` value that one patch object changes.
    struct PatchOps
    {
        /// Paths the patch sets to a value which is not an object: the values at and below them are replaced.
        VectorWithMemoryTracking<StringWithMemoryTracking> replaced;
        /// Paths the patch sets to an object: a value at exactly such a path is replaced by the object.
        VectorWithMemoryTracking<StringWithMemoryTracking> dropped;
        /// The patch cannot be applied path by path, so the row is merged in its text form.
        bool fallback = false;

        [[nodiscard]] bool changesNothing() const { return !fallback && replaced.empty() && dropped.empty(); }

        [[nodiscard]] bool isReplaced(std::string_view path) const { return std::binary_search(replaced.begin(), replaced.end(), path); }

        [[nodiscard]] bool removes(std::string_view path) const
        {
            if (std::binary_search(dropped.begin(), dropped.end(), path))
                return true;

            for (size_t dot = path.find('.');; dot = path.find('.', dot + 1))
            {
                if (isReplaced(path.substr(0, dot)))
                    return true;
                if (dot == std::string_view::npos)
                    return false;
            }
        }
    };

    /// Merges patches into a column of the `JSON` type path by path, by RFC 7396 on the stored paths. The values of a
    /// patch are parsed by the same CAST as `::JSON`.
    class ObjectMergePatcher
    {
    public:
        ObjectMergePatcher(const DataTypePtr & type_, const FunctionConvertSettingsPtr & convert_settings_, const FormatSettings & format_settings)
            : type(type_)
            , typed_paths(assert_cast<const DataTypeObject &>(*type_).getTypedPaths())
            , convert_settings(convert_settings_)
            , round_trip_format_settings(getJSONRoundTripFormatSettingsForValues(format_settings))
            , escape_dots_in_keys(format_settings.json.json_type_escape_dots_in_keys)
            , descend_into_typed_paths(format_settings.json.type_json_allow_duplicated_key_with_literal_and_nested_object)
        {
        }

        [[nodiscard]] ColumnPtr apply(const ColumnPtr & column, const ColumnPtr & patches_column, size_t rows) const
        {
            const auto & object = assert_cast<const ColumnObject &>(*column);
            const auto * patches_const = typeid_cast<const ColumnConst *>(patches_column.get());
            const auto & patches = getStringColumn(patches_const ? patches_const->getDataColumn() : *patches_column);
            const bool patch_is_const = patches_const != nullptr;

            VectorWithMemoryTracking<PatchOps> ops(patch_is_const ? 1 : rows);
            bool changes_anything = false;
            bool has_fallback = false;
            String path;
            for (size_t i = 0; i < ops.size(); ++i)
            {
                TrackedDocument document;
                parseJSONDocument(patches.getDataAt(i), document);
                auto & row_ops = ops[i];
                row_ops.fallback = !collectPatchOps(document, path, true, row_ops) || !sortAndCheck(row_ops);
                path.clear();
                changes_anything |= !row_ops.changesNothing();
                has_fallback |= row_ops.fallback;
            }

            if (!changes_anything)
                return column;

            auto get_ops = [&](size_t row) -> const PatchOps & { return ops[patch_is_const ? 0 : row]; };

            /// The values of the patch, or of the whole merged row for a fallback row, parsed into the type.
            const bool values_are_const = patch_is_const && !has_fallback;
            auto value_texts = ColumnString::create();
            if (values_are_const)
            {
                value_texts->insertFrom(patches, 0);
            }
            else
            {
                const auto fallback_texts = mergeFallbackRows(column, get_ops, patches, patch_is_const, rows);
                value_texts->reserve(rows);
                size_t next_fallback_row = 0;
                for (size_t i = 0; i < rows; ++i)
                {
                    if (get_ops(i).fallback)
                        value_texts->insertFrom(*fallback_texts, next_fallback_row++);
                    else
                        value_texts->insertFrom(patches, patch_is_const ? 0 : i);
                }
            }

            const auto values_column = castColumnTo(std::move(value_texts), std::make_shared<DataTypeString>(), type, convert_settings);
            const auto & values = assert_cast<const ColumnObject &>(*values_column);
            auto get_value_row = [&](size_t row) { return values_are_const ? 0 : row; };

            /// A typed path takes the value of the patch, or the default value of its type if the patch removes it.
            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> new_typed_columns;
            for (const auto & [typed_path, typed_column] : object.getTypedPaths())
            {
                const auto & path_type = typed_paths.at(typed_path);
                const auto & value_column = *values.getTypedPaths().find(typed_path)->second;
                MutableColumnPtr result;
                if (values_are_const)
                {
                    if (ops[0].isReplaced(typed_path))
                    {
                        result = typed_column->cloneEmpty();
                        result->insertManyFrom(value_column, 0, rows);
                    }
                    else if (ops[0].removes(typed_path))
                    {
                        result = typed_column->cloneEmpty();
                        path_type->insertManyDefaultsInto(*result, rows);
                    }
                }
                else
                {
                    for (size_t i = 0; i < rows; ++i)
                    {
                        const auto & row_ops = get_ops(i);
                        const bool from_patch = row_ops.fallback || row_ops.isReplaced(typed_path);
                        if (!from_patch && !row_ops.removes(typed_path))
                        {
                            if (result)
                                result->insertFrom(*typed_column, i);
                            continue;
                        }

                        if (!result)
                        {
                            result = typed_column->cloneEmpty();
                            result->reserve(rows);
                            result->insertRangeFrom(*typed_column, 0, i);
                        }

                        if (from_patch)
                            result->insertFrom(value_column, get_value_row(i));
                        else
                            path_type->insertDefaultInto(*result);
                    }
                }

                if (result)
                    new_typed_columns.emplace(typed_path, std::move(result));
            }

            const auto & value_dynamic_paths = values.getDynamicPaths();
            const auto [value_shared_paths, value_shared_values] = values.getSharedDataPathsAndValues();
            const auto & value_shared_offsets = values.getSharedDataOffsets();
            auto find_in_value_shared_data = [&](std::string_view value_path, size_t value_row) -> std::optional<size_t>
            {
                const size_t start = value_shared_offsets[static_cast<ssize_t>(value_row) - 1];
                const size_t end = value_shared_offsets[value_row];
                const size_t pos = ColumnObject::findPathLowerBoundInSharedData(value_path, *value_shared_paths, start, end);
                if (pos < end && value_shared_paths->getDataAt(pos) == value_path)
                    return pos;
                return std::nullopt;
            };

            /// A dynamic path takes the value of the patch, or NULL if the patch removes it. It stays a dynamic path
            /// even if all its values are NULL.
            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> new_dynamic_columns;
            for (const auto & [dynamic_path, dynamic_column] : object.getDynamicPaths())
            {
                const IColumn * value_column = nullptr;
                if (auto it = value_dynamic_paths.find(dynamic_path); it != value_dynamic_paths.end())
                    value_column = it->second.get();

                /// Inserts the value of the patch, if the patch sets this path.
                auto insert_value = [&](IColumn & dest, size_t value_row)
                {
                    if (value_column)
                    {
                        if (value_column->isNullAt(value_row))
                            return false;
                        dest.insertFrom(*value_column, value_row);
                        return true;
                    }

                    auto pos = find_in_value_shared_data(dynamic_path, value_row);
                    if (!pos)
                        return false;
                    ColumnObject::deserializeValueFromSharedData(value_shared_values, *pos, dest);
                    return true;
                };

                MutableColumnPtr result;
                if (values_are_const)
                {
                    auto value = dynamic_column->cloneEmpty();
                    if (insert_value(*value, 0))
                    {
                        result = dynamic_column->cloneEmpty();
                        result->insertManyFrom(*value, 0, rows);
                    }
                    else if (ops[0].removes(dynamic_path) && dynamic_column->getNumberOfDefaultRows() != rows)
                    {
                        result = dynamic_column->cloneEmpty();
                        result->insertManyDefaults(rows);
                    }
                }
                else
                {
                    for (size_t i = 0; i < rows; ++i)
                    {
                        const auto & row_ops = get_ops(i);
                        auto ensure_result = [&]
                        {
                            if (result)
                                return;
                            result = dynamic_column->cloneEmpty();
                            result->reserve(rows);
                            result->insertRangeFrom(*dynamic_column, 0, i);
                        };

                        if (!row_ops.changesNothing())
                        {
                            const size_t value_row = get_value_row(i);
                            const bool has_value = value_column ? !value_column->isNullAt(value_row)
                                                                : find_in_value_shared_data(dynamic_path, value_row).has_value();
                            if (has_value)
                            {
                                ensure_result();
                                insert_value(*result, value_row);
                                continue;
                            }

                            if ((row_ops.fallback || row_ops.removes(dynamic_path)) && !dynamic_column->isNullAt(i))
                            {
                                ensure_result();
                                result->insertDefault();
                                continue;
                            }
                        }

                        if (result)
                            result->insertFrom(*dynamic_column, i);
                    }
                }

                if (result)
                    new_dynamic_columns.emplace(dynamic_path, std::move(result));
            }

            /// New paths of the patch become dynamic paths while there are free ones, unless some row has shared data:
            /// a path cannot be both a dynamic path and in the shared data.
            VectorWithMemoryTracking<std::string_view> new_paths;
            for (const auto & [value_path, _] : value_dynamic_paths)
            {
                if (!object.getDynamicPaths().contains(value_path))
                    new_paths.push_back(value_path);
            }
            std::sort(new_paths.begin(), new_paths.end());

            const auto [shared_paths, shared_values] = object.getSharedDataPathsAndValues();
            const auto & shared_offsets = object.getSharedDataOffsets();
            const size_t max_dynamic_paths_upper_bound = std::max(
                std::min(object.getMaxDynamicPathsUpperBound(), values.getMaxDynamicPathsUpperBound()), object.getDynamicPaths().size());
            const size_t max_dynamic_paths = std::min(object.getMaxDynamicPaths(), max_dynamic_paths_upper_bound);
            size_t num_new_dynamic_paths = 0;
            if (shared_paths->empty() && max_dynamic_paths > object.getDynamicPaths().size())
                num_new_dynamic_paths = std::min(new_paths.size(), max_dynamic_paths - object.getDynamicPaths().size());

            for (size_t k = 0; k < num_new_dynamic_paths; ++k)
            {
                const auto & value_column = *value_dynamic_paths.find(new_paths[k])->second;
                auto result = ColumnDynamic::create(object.getMaxDynamicTypes());
                if (values_are_const)
                    result->insertManyFrom(value_column, 0, rows);
                else
                    result->insertRangeFrom(value_column, 0, rows);
                new_dynamic_columns.emplace(new_paths[k], std::move(result));
            }

            /// The other new paths go to the shared data.
            const std::span<const std::string_view> overflow_paths(new_paths.begin() + num_new_dynamic_paths, new_paths.end());
            auto overflow_paths_column = ColumnString::create();
            auto overflow_values_column = ColumnString::create();
            ColumnArray::Offsets overflow_offsets;
            if (!overflow_paths.empty())
            {
                VectorWithMemoryTracking<const ColumnDynamic *> overflow_columns;
                overflow_columns.reserve(overflow_paths.size());
                for (const auto overflow_path : overflow_paths)
                    overflow_columns.push_back(&assert_cast<const ColumnDynamic &>(*value_dynamic_paths.find(overflow_path)->second));

                const size_t num_value_rows = values_are_const ? 1 : rows;
                overflow_offsets.reserve(num_value_rows);
                for (size_t value_row = 0; value_row < num_value_rows; ++value_row)
                {
                    for (size_t k = 0; k < overflow_paths.size(); ++k)
                        ColumnObject::serializePathAndValueIntoSharedData(
                            overflow_paths_column.get(), overflow_values_column.get(), overflow_paths[k], *overflow_columns[k], value_row);
                    overflow_offsets.push_back(overflow_paths_column->size());
                }
            }

            /// The shared data entries the patch adds to a row, sorted by path.
            VectorWithMemoryTracking<std::pair<std::string_view, std::string_view>> added;
            auto collect_added = [&](size_t value_row)
            {
                added.clear();
                for (size_t j = value_shared_offsets[static_cast<ssize_t>(value_row) - 1]; j < value_shared_offsets[value_row]; ++j)
                {
                    auto value_path = value_shared_paths->getDataAt(j);
                    if (!object.getDynamicPaths().contains(value_path))
                        added.emplace_back(value_path, value_shared_values->getDataAt(j));
                }

                if (overflow_paths.empty())
                    return;

                const size_t middle = added.size();
                for (size_t j = overflow_offsets[static_cast<ssize_t>(value_row) - 1]; j < overflow_offsets[value_row]; ++j)
                    added.emplace_back(overflow_paths_column->getDataAt(j), overflow_values_column->getDataAt(j));
                std::inplace_merge(
                    added.begin(), added.begin() + middle, added.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
            };

            if (values_are_const)
                collect_added(0);

            const auto & shared_data = object.getSharedDataPtr();
            MutableColumnPtr new_shared_data;
            ColumnString * new_shared_paths = nullptr;
            ColumnString * new_shared_values = nullptr;
            ColumnArray::Offsets * new_shared_offsets = nullptr;
            VectorWithMemoryTracking<size_t> kept;
            for (size_t i = 0; i < rows; ++i)
            {
                const auto & row_ops = get_ops(i);
                const size_t start = shared_offsets[static_cast<ssize_t>(i) - 1];
                const size_t end = shared_offsets[i];
                bool row_changed = false;
                if (!row_ops.changesNothing())
                {
                    if (!values_are_const)
                        collect_added(i);

                    kept.clear();
                    if (!row_ops.fallback)
                    {
                        for (size_t j = start; j < end; ++j)
                        {
                            if (!row_ops.removes(shared_paths->getDataAt(j)))
                                kept.push_back(j);
                        }
                    }
                    row_changed = !added.empty() || kept.size() != end - start;
                }

                if (!row_changed)
                {
                    if (new_shared_data)
                        new_shared_data->insertFrom(*shared_data, i);
                    continue;
                }

                if (!new_shared_data)
                {
                    new_shared_data = shared_data->cloneEmpty();
                    new_shared_data->reserve(rows);
                    new_shared_data->insertRangeFrom(*shared_data, 0, i);
                    std::tie(new_shared_paths, new_shared_values, new_shared_offsets)
                        = ColumnObject::getSharedDataPathsValuesAndOffsets(*new_shared_data);
                }

                auto insert_kept = [&](size_t j)
                {
                    new_shared_paths->insertFrom(*shared_paths, j);
                    new_shared_values->insertFrom(*shared_values, j);
                };

                /// On an equal path the value from the patch wins.
                size_t k = 0;
                for (const auto & [added_path, added_value] : added)
                {
                    for (; k < kept.size() && shared_paths->getDataAt(kept[k]) < added_path; ++k)
                        insert_kept(kept[k]);
                    if (k < kept.size() && shared_paths->getDataAt(kept[k]) == added_path)
                        ++k;
                    new_shared_paths->insertData(added_path.data(), added_path.size());
                    new_shared_values->insertData(added_value.data(), added_value.size());
                }
                for (; k < kept.size(); ++k)
                    insert_kept(kept[k]);
                new_shared_offsets->push_back(new_shared_paths->size());
            }

            if (new_typed_columns.empty() && new_dynamic_columns.empty() && !new_shared_data)
                return column;

            /// The columns that did not change are copied, as `ColumnObject::create` does for shared columns.
            auto take_column = [](UnorderedMapWithMemoryTracking<String, MutableColumnPtr> & new_columns, const String & name, ColumnPtr old_column)
            {
                if (auto it = new_columns.find(name); it != new_columns.end())
                    return std::move(it->second);
                return IColumn::mutate(std::move(old_column));
            };

            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> result_typed_paths;
            result_typed_paths.reserve(object.getTypedPaths().size());
            for (const auto & [typed_path, typed_column] : object.getTypedPaths())
                result_typed_paths.emplace(typed_path, take_column(new_typed_columns, typed_path, typed_column));

            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> result_dynamic_paths;
            result_dynamic_paths.reserve(object.getDynamicPaths().size() + num_new_dynamic_paths);
            for (const auto & [dynamic_path, dynamic_column] : object.getDynamicPaths())
                result_dynamic_paths.emplace(dynamic_path, take_column(new_dynamic_columns, dynamic_path, dynamic_column));
            for (size_t k = 0; k < num_new_dynamic_paths; ++k)
            {
                String new_path(new_paths[k]);
                auto it = new_dynamic_columns.find(new_path);
                result_dynamic_paths.emplace(std::move(new_path), std::move(it->second));
            }

            auto result_shared_data = new_shared_data ? std::move(new_shared_data) : IColumn::mutate(shared_data);
            /// Non-empty shared data locks the dynamic paths: a path added later could already be in the shared data.
            const size_t result_max_dynamic_paths
                = assert_cast<const ColumnArray &>(*result_shared_data).getData().empty() ? max_dynamic_paths : result_dynamic_paths.size();

            return ColumnObject::create(
                std::move(result_typed_paths),
                std::move(result_dynamic_paths),
                std::move(result_shared_data),
                result_max_dynamic_paths,
                max_dynamic_paths_upper_bound,
                object.getGlobalMaxDynamicPaths(),
                object.getMaxDynamicTypes());
        }

        /// Writes each row as JSON text that is parsed back into the same values.
        [[nodiscard]] ColumnPtr serializeToText(const IColumn & column, const DataTypePtr & from) const
        {
            auto texts = ColumnString::create();
            ColumnStringHelpers::WriteHelper<ColumnString> write_helper(*texts, column.size());
            auto & write_buffer = write_helper.getWriteBuffer();
            const auto serialization = from->getDefaultSerialization();
            for (size_t i = 0; i < column.size(); ++i)
            {
                serialization->serializeTextJSON(column, i, write_buffer, round_trip_format_settings);
                write_helper.finishRow();
            }
            write_helper.finalize();
            return texts;
        }

    private:
        /// Collects the paths the members of `object` change, assigning them to paths as the parser of the type does.
        /// Returns false if the row has to be merged in its text form.
        bool collectPatchOps(const TrackedValue & object, String & path, bool is_root, PatchOps & ops) const
        {
            for (auto it = object.MemberBegin(); it != object.MemberEnd(); ++it)
            {
                const size_t parent_size = path.size();
                if (!is_root)
                    path += '.';
                String key(it->name.GetString(), it->name.GetStringLength());
                path += escape_dots_in_keys ? escapeDotInJSONKey(key) : key;

                const auto & value = it->value;
                if (!value.IsObject())
                {
                    ops.replaced.emplace_back(std::string_view(path));
                }
                else if (const auto * path_type = findTypedValuePath(path))
                {
                    /// The text form merges the patch into the object stored at this path.
                    if (canHoldObject(*path_type))
                        return false;
                    ops.replaced.emplace_back(std::string_view(path));
                }
                else
                {
                    ops.dropped.emplace_back(std::string_view(path));
                    if (!collectPatchOps(value, path, false, ops))
                        return false;
                }

                path.resize(parent_size);
            }
            return true;
        }

        /// A path the patch sets twice (a duplicate key, or a key with a dot and the same nested key), or a value it
        /// replaces together with a path below it (a key with a dot), is left to the text form, which applies the
        /// members in order.
        static bool sortAndCheck(PatchOps & ops)
        {
            std::sort(ops.replaced.begin(), ops.replaced.end());
            std::sort(ops.dropped.begin(), ops.dropped.end());
            if (std::adjacent_find(ops.replaced.begin(), ops.replaced.end()) != ops.replaced.end()
                || std::adjacent_find(ops.dropped.begin(), ops.dropped.end()) != ops.dropped.end())
                return false;

            auto replaced_it = ops.replaced.begin();
            auto dropped_it = ops.dropped.begin();
            while (replaced_it != ops.replaced.end() && dropped_it != ops.dropped.end())
            {
                if (*replaced_it < *dropped_it)
                    ++replaced_it;
                else if (*dropped_it < *replaced_it)
                    ++dropped_it;
                else
                    return false;
            }

            StringWithMemoryTracking prefix;
            auto has_path_below = [&](const auto & paths)
            {
                auto it = std::lower_bound(paths.begin(), paths.end(), prefix);
                return it != paths.end() && it->starts_with(prefix);
            };
            for (const auto & replaced_path : ops.replaced)
            {
                prefix = replaced_path;
                prefix += '.';
                if (has_path_below(ops.replaced) || has_path_below(ops.dropped))
                    return false;
            }
            return true;
        }

        [[nodiscard]] bool hasTypedPathWithPrefix(std::string_view prefix) const
        {
            return std::ranges::any_of(typed_paths, [&](const auto & typed_path) { return typed_path.first.starts_with(prefix); });
        }

        /// The type of the typed path `path` if the parser of the type reads an object at it as the value of the path;
        /// nullptr if `path` is not typed or the parser descends into the object to reach typed sub-paths.
        [[nodiscard]] const DataTypePtr * findTypedValuePath(const String & path) const
        {
            auto typed = typed_paths.find(path);
            if (typed == typed_paths.end() || (descend_into_typed_paths && hasTypedPathWithPrefix(path + ".")))
                return nullptr;
            return &typed->second;
        }

        /// Whether an object at `path` is the value of a typed path that can hold one.
        [[nodiscard]] bool isObjectValuePath(const String & path) const
        {
            const auto * path_type = findTypedValuePath(path);
            return path_type && canHoldObject(*path_type);
        }

        static bool canHoldObject(const DataTypePtr & path_type)
        {
            const auto nested_type = removeLowCardinality(removeNullable(path_type));
            return isObject(nested_type) || isMap(nested_type) || isTuple(nested_type) || isDynamic(nested_type) || isVariant(nested_type);
        }

        /// Merges a patch into the text form of a stored row as mergeObjects does, except that a key with a dot sets the
        /// nested path, as the parser of the type reads it, and that a value stored at both `x` and `x.y` is two members
        /// named `x`, a literal and an object: the member of the other kind than the patch value is dropped. An object
        /// that is the value of a typed path is merged by mergeObjects.
        void mergeIntoStoredRow(TrackedValue & dest, const TrackedValue & src, String & path, bool is_root, TrackedPoolAllocator & allocator) const
        {
            for (auto it = src.MemberBegin(); it != src.MemberEnd(); ++it)
            {
                const size_t parent_size = path.size();
                TrackedValue * level = &dest;
                bool level_is_root = is_root;
                std::string_view name = memberName(*it);
                if (!escape_dots_in_keys && name.contains('.') && !crossesTypedPath(path, is_root, name))
                {
                    for (size_t dot = name.find('.'); dot != std::string_view::npos; dot = name.find('.'))
                    {
                        if (!level_is_root)
                            path += '.';
                        path += name.substr(0, dot);
                        level = &getObjectMember(*level, name.substr(0, dot), allocator);
                        level_is_root = false;
                        name.remove_prefix(dot + 1);
                    }
                }
                mergeMember(*level, name, it->value, path, level_is_root, allocator);
                path.resize(parent_size);
            }
        }

        /// Merges the patch member `name` into the object `dest`.
        void mergeMember(TrackedValue & dest, std::string_view name, const TrackedValue & value, String & path, bool is_root, TrackedPoolAllocator & allocator) const
        {
            const bool is_object = value.IsObject();

            auto target = dest.MemberEnd();
            for (auto member = dest.MemberBegin(); member != dest.MemberEnd(); ++member)
            {
                if (memberName(*member) != name)
                    continue;
                if (member->value.IsObject() == is_object)
                {
                    target = member;
                    break;
                }
                if (target == dest.MemberEnd())
                    target = member;
            }

            if (target == dest.MemberEnd())
            {
                TrackedValue key(name.data(), static_cast<rapidjson::SizeType>(name.size()), allocator);
                TrackedValue copy(value, allocator);
                dest.AddMember(key, copy, allocator);
                return;
            }

            if (is_object && target->value.IsObject())
            {
                const size_t parent_size = path.size();
                if (!is_root)
                    path += '.';
                String key(name);
                path += escape_dots_in_keys ? escapeDotInJSONKey(key) : key;
                if (isObjectValuePath(path))
                    mergeObjects(target->value, value, allocator);
                else
                    mergeIntoStoredRow(target->value, value, path, false, allocator);
                path.resize(parent_size);
            }
            else
            {
                TrackedValue copy(value, allocator);
                target->value = copy;
            }

            for (auto member = dest.MemberBegin(); member != dest.MemberEnd();)
            {
                if (memberName(*member) == name && member->value.IsObject() != is_object)
                    member = dest.EraseMember(member);
                else
                    ++member;
            }
        }

        /// Whether the path of a key with dots passes through the object value of a typed path, whose text form is ambiguous.
        [[nodiscard]] bool crossesTypedPath(const String & parent, bool is_root, std::string_view key) const
        {
            String full = parent;
            if (!is_root)
                full += '.';
            full += key;
            for (size_t dot = full.find('.'); dot != String::npos; dot = full.find('.', dot + 1))
            {
                if (isObjectValuePath(full.substr(0, dot)))
                    return true;
            }
            return false;
        }

        static std::string_view memberName(const TrackedValue::Member & member)
        {
            return {member.name.GetString(), member.name.GetStringLength()};
        }

        /// The object member named `name`, added if there is none.
        static TrackedValue & getObjectMember(TrackedValue & object, std::string_view name, TrackedPoolAllocator & allocator)
        {
            for (auto member = object.MemberBegin(); member != object.MemberEnd(); ++member)
            {
                if (memberName(*member) == name && member->value.IsObject())
                    return member->value;
            }

            TrackedValue key(name.data(), static_cast<rapidjson::SizeType>(name.size()), allocator);
            TrackedValue value(rapidjson::kObjectType);
            object.AddMember(key, value, allocator);
            return (object.MemberEnd() - 1)->value;
        }

        template <typename GetOps>
        [[nodiscard]] ColumnString::MutablePtr mergeFallbackRows(
            const ColumnPtr & column, const GetOps & get_ops, const ColumnString & patches, bool patch_is_const, size_t rows) const
        {
            IColumn::Filter filter(rows, 0);
            size_t num_fallback_rows = 0;
            for (size_t i = 0; i < rows; ++i)
            {
                if (get_ops(i).fallback)
                {
                    filter[i] = 1;
                    ++num_fallback_rows;
                }
            }

            auto result = ColumnString::create();
            if (num_fallback_rows == 0)
                return result;

            const auto texts_column = serializeToText(*column->filter(filter, num_fallback_rows), type);
            const auto & texts = assert_cast<const ColumnString &>(*texts_column);
            result->reserve(num_fallback_rows);
            size_t next_text = 0;
            String path;
            for (size_t i = 0; i < rows; ++i)
            {
                if (!filter[i])
                    continue;

                TrackedDocument merged;
                parseJSONDocument(texts.getDataAt(next_text++), merged);
                TrackedDocument patch;
                parseJSONDocument(patches.getDataAt(patch_is_const ? 0 : i), patch);
                mergeIntoStoredRow(merged, patch, path, /* is_root = */ true, merged.GetAllocator());
                /// Keys with dots add levels.
                checkJSONDepth(merged);
                writeJSONDocument(merged, *result);
            }
            return result;
        }

        const DataTypePtr type;
        const DataTypeObject::TypedPathsMap & typed_paths;
        const FunctionConvertSettingsPtr convert_settings;
        const FormatSettings round_trip_format_settings;
        const bool escape_dots_in_keys;
        const bool descend_into_typed_paths;
    };

    // select JSONMergePatch('{"a":1}','{"name": "joey"}','{"name": "tom"}','{"name": "zoey"}');
    //           ||
    //           \/
    // ┌───────────────────────┐
    // │ {"a":1,"name":"zoey"} │
    // └───────────────────────┘
    class FunctionJSONMergePatch final : public IFunction
    {
    public:
        static constexpr auto name = "JSONMergePatch";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionJSONMergePatch>(context); }

        explicit FunctionJSONMergePatch(ContextPtr context)
            : convert_settings(createFunctionConvertSettings(context, FormatSettings::DateTimeOverflowBehavior::Saturate))
            , format_settings(context ? getFormatSettings(context) : FormatSettings{})
            , format_settings_hash(context ? getFormatSettingsHash(context->getSettingsRef()) : 0)
        {
        }

        [[nodiscard]] String getName() const override { return name; }

        /// The captured settings decide how a patch is parsed into the `JSON` type, see `IFunctionBase::updateHash`.
        void updateHash(SipHash & hash) const override { hash.update(format_settings_hash); }

        [[nodiscard]] bool isVariadic() const override { return true; }
        [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
        /// A `LowCardinality` dictionary always holds the type's default value at index 0, even when no
        /// row references it, so a function that throws on the default value must not be executed on
        /// the whole dictionary - it would fail on entirely valid data.
        [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }

        [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

        [[nodiscard]] String getSignatureString() const override
        {
            return "(String, ...) -> String OR (T : JSON, [String | JSON], ...) -> T";
        }

        [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            chassert(!arguments.empty());

            if (isObject(arguments[0].type))
                return executeForObject(arguments, input_rows_count);

            TrackedPoolAllocator allocator;

            const bool is_first_const = isColumnConst(*arguments[0].column);
            const auto * first_column_arg_string = is_first_const
                        ? checkAndGetColumnConstData<ColumnString>(arguments[0].column.get())
                        : checkAndGetColumn<ColumnString>(arguments[0].column.get());

            if (!first_column_arg_string)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments of function {} must be strings", getName());

            VectorWithMemoryTracking<TrackedDocument> merged_jsons;
            merged_jsons.reserve(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto & merged_json = merged_jsons.emplace_back(rapidjson::Type::kObjectType, &allocator);
                parseJSONDocument(first_column_arg_string->getDataAt(is_first_const ? 0 : i), merged_json);
            }

            for (size_t col_idx = 1; col_idx < arguments.size(); ++col_idx)
            {
                const bool is_const = isColumnConst(*arguments[col_idx].column);
                const auto * column_arg_string = is_const
                            ? checkAndGetColumnConstData<ColumnString>(arguments[col_idx].column.get())
                            : checkAndGetColumn<ColumnString>(arguments[col_idx].column.get());

                if (!column_arg_string)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments of function {} must be strings", getName());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    TrackedDocument document(&allocator);
                    parseJSONDocument(column_arg_string->getDataAt(is_const ? 0 : i), document);
                    mergeObjects(merged_jsons[i], document, allocator);
                }
            }

            auto result = ColumnString::create();
            for (size_t i = 0; i < input_rows_count; ++i)
                writeJSONDocument(merged_jsons[i], *result);

            return result;
        }

    private:
        [[nodiscard]] ColumnPtr executeForObject(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
        {
            ColumnPtr result = arguments[0].column->convertToFullColumnIfConst();
            if (input_rows_count == 0)
                return result;

            const ObjectMergePatcher patcher(arguments[0].type, convert_settings, format_settings);
            for (size_t i = 1; i < arguments.size(); ++i)
            {
                ColumnPtr patches = arguments[i].column;
                /// A patch of the `JSON` type is applied as its text.
                if (isObject(arguments[i].type))
                {
                    if (const auto * patches_const = typeid_cast<const ColumnConst *>(patches.get()))
                        patches = ColumnConst::create(patcher.serializeToText(patches_const->getDataColumn(), arguments[i].type), input_rows_count);
                    else
                        patches = patcher.serializeToText(*patches, arguments[i].type);
                }
                result = patcher.apply(result, patches, input_rows_count);
            }
            return result;
        }

        const FunctionConvertSettingsPtr convert_settings;
        const FormatSettings format_settings;
        const UInt64 format_settings_hash;
    };

}

REGISTER_FUNCTION(JSONMergePatch)
{
    /// jsonMergePatch documentation
    FunctionDocumentation::Description description = R"(
Returns the merged JSON object string which is formed by merging multiple JSON objects.

If the first argument has the `JSON` type, the result has the same type.
Most patches update the stored paths directly, without converting the value to text.
For a `JSON` first argument, `null` in a patch removes the path; a typed path, a key in a `Map` typed path, or an element of a `Tuple` typed path gets the default value instead.
A patch of the `JSON` type is applied as its text.
    )";
    FunctionDocumentation::Syntax syntax = "JSONMergePatch(json1[, json2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"json1[, json2, ...]", "One or more strings with valid JSON, or values of the `JSON` type.", {"String", "JSON"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the merged JSON object string, if the JSON object strings are valid. If the first argument has the `JSON` type, returns a value of the same type.",
        {"String", "JSON"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT JSONMergePatch('{"a":1}', '{"name": "joey"}', '{"name": "tom"}', '{"name": "zoey"}') AS res;
        )",
        R"(
┌─res───────────────────┐
│ {"a":1,"name":"zoey"} │
└───────────────────────┘
        )"
    },
    {
        "JSON type",
        R"(
SELECT JSONMergePatch('{"a":1,"b":{"c":2,"d":3}}'::JSON, '{"b":{"c":null},"e":4}') AS res, toTypeName(res) AS type;
        )",
        R"(
┌─res───────────────────────┬─type─┐
│ {"a":1,"b":{"d":3},"e":4} │ JSON │
└───────────────────────────┴──────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionJSONMergePatch>(documentation);

    factory.registerAlias("jsonMergePatch", "JSONMergePatch");
}

}

#endif
