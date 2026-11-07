#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/TypeTree.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <DataTypes/NestedUtils.h>
#include <Core/NamesAndTypes.h>
#include <Common/checkStackSize.h>
#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/ColumnConst.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>

#include <algorithm>
#include <array>
#include <memory>
#include <unordered_set>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace FailPoints
{
    extern const char patch_parts_reverse_column_order[];
}

namespace
{

bool hasMaterializedTextIndex(
    const StorageSnapshotPtr & storage_snapshot,
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const String & virtual_column_name)
{
    if (storage_snapshot->metadata->virtuals.empty())
        return false;

    const auto * virtual_column = storage_snapshot->metadata->virtuals.tryGetDescription(virtual_column_name, VirtualsKind::All, VirtualsMaterializationPlace::Reader);
    if (!virtual_column)
        return false;

    /// Name of the text index is embedded as a comment to the virtual column.
    const auto & text_index_name = virtual_column->comment;
    for (const auto & index_desc : storage_snapshot->metadata->getSecondaryIndices())
    {
        if (index_desc.type == "text" && index_desc.name == text_index_name)
            if (const auto * loaded_part = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(&data_part_info_for_reader))
                return loaded_part->getDataPart()->hasSecondaryIndex(index_desc.name, storage_snapshot->metadata);
    }

    return false;
}

/// Columns absent in part may depend on other absent columns so we are
/// searching all required physical columns recursively. Return true if found at
/// least one existing (physical) column in part.
bool injectRequiredColumnsRecursively(
    const String & column_name,
    const StorageSnapshotPtr & storage_snapshot,
    const AlterConversionsPtr & alter_conversions,
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const GetColumnsOptions & options,
    Names & columns,
    NameSet & required_columns,
    NameSet & injected_columns)
{
    /// This is needed to prevent stack overflow in case of cyclic defaults or
    /// huge AST which for some reason was not validated on parsing/interpreter
    /// stages.
    checkStackSize();

    auto add_column = [&](const String & name)
    {
        /// Ensure each column is added only once
        if (!required_columns.contains(name))
        {
            columns.emplace_back(name);
            required_columns.emplace(name);
            injected_columns.emplace(name);
        }
    };

    auto column_in_storage = storage_snapshot->tryGetColumn(options, column_name);
    if (column_in_storage)
    {
        auto column_name_in_part = column_in_storage->getNameInStorage();
        if (alter_conversions && alter_conversions->isColumnRenamed(column_name_in_part))
            column_name_in_part = alter_conversions->getColumnOldName(column_name_in_part);

        auto column_in_part = data_part_info_for_reader.getColumns().tryGetByName(column_name_in_part);

        bool share_nested = true;
        if (const auto * merge_tree = dynamic_cast<const MergeTreeData *>(&storage_snapshot->storage))
            share_nested = (*merge_tree->getSettings())[MergeTreeSetting::share_nested_offsets];

        if (column_in_part
            /// If the column was dropped by a pending mutation that hasn't been applied yet,
            /// the data in this part is stale. Treat it as missing so that the default value is used.
            /// This can happen if the column was dropped and then re-added with the same name.
            && !(alter_conversions && alter_conversions->isColumnDropped(column_name_in_part, share_nested)))
        {
            /// Resolved against the serialization the part holds for the column - which it always does, the column
            /// comes from its own list - instead of a newly built one.
            std::optional<String> source_of_subcolumn;
            if (column_in_storage->isSubcolumn())
                source_of_subcolumn = column_in_part->type->tryGetSubcolumnType(
                        column_in_storage->getSubcolumnName(), data_part_info_for_reader.getSerialization(*column_in_part))
                    ? getSourceOfSubcolumnInPart(storage_snapshot, *column_in_storage, column_in_part->type, alter_conversions)
                    : column_in_storage->getNameInStorage();

            if (!source_of_subcolumn)
            {
                add_column(column_name);
                return true;
            }

            /// Parent is present but the part's (older) type lacks the requested subcolumn (metadata-only
            /// `ALTER MODIFY COLUMN T -> Nullable(T)`), or has another one under its name (a changed `JSON` type). Read the parent
            /// (or its path with the subcolumn) to convert it and extract the subcolumn; the subcolumn too, as a default may use it.
            add_column(column_name);
            add_column(*source_of_subcolumn);
            return true;
        }
        else if (isTextIndexVirtualColumn(column_name_in_part)
            && hasMaterializedTextIndex(storage_snapshot, data_part_info_for_reader, column_name_in_part)
            && canReadTextIndexInPart(alter_conversions))
        {
            /// If there is a materialized text index in the part, use the virtual column directly.
            add_column(column_name);
            return true;
        }
        else if (!column_in_part && column_in_storage->isSubcolumn()
            && !(alter_conversions && alter_conversions->isColumnDropped(column_name_in_part, share_nested)))
        {
            /// The part omitted the column: its source is filled from the default the part recorded, as the column is.
            auto type_in_part = tryGetColumnTypeInPart(data_part_info_for_reader, column_name_in_part);
            if (auto source = getSourceOfSubcolumnInPart(storage_snapshot, *column_in_storage, type_in_part, alter_conversions))
            {
                add_column(column_name);
                add_column(*source);
                return false;
            }
        }
    }

    /// Column doesn't have default value and don't exist in part
    /// don't need to add to required set.
    auto column_default = storage_snapshot->getDefault(column_name);

    /// A subcolumn does not have its own default expression: it is extracted from the evaluated
    /// default of the column in storage (see IMergeTreeReader::evaluateMissingDefaults),
    /// so the columns required by that expression must be read as well.
    if (!column_default && column_in_storage && column_in_storage->isSubcolumn())
        column_default = storage_snapshot->getDefault(column_in_storage->getNameInStorage());

    ASTPtr default_expression = column_default.has_value() ? column_default->expression : nullptr;
    if (!default_expression)
        return false;

    /// collect identifiers required for evaluation
    IdentifierNameSet identifiers;
    default_expression->collectIdentifierNames(identifiers);

    bool result = false;
    for (const auto & identifier : identifiers)
        result |= injectRequiredColumnsRecursively(
            identifier, storage_snapshot, alter_conversions, data_part_info_for_reader,
            options, columns, required_columns, injected_columns);

    return result;
}

using Substream = ISerialization::Substream;

bool isObjectPathSubstream(const Substream & substream)
{
    switch (substream.type)
    {
        case Substream::ObjectTypedPath:
        case Substream::ObjectTypedSharedDataPath:
        case Substream::ObjectDynamicPath:
        case Substream::ObjectSubObject:
        case Substream::ObjectCombinedPath:
        case Substream::ObjectDistinctPaths:
            return true;
        default:
            return false;
    }
}

/// The type that a substream of a subcolumn path descends into from @type, or null when it is not tracked.
const IDataType * getTypeOfSubstream(const IDataType * type, const Substream & substream)
{
    if (!type)
        return nullptr;

    switch (substream.type)
    {
        case Substream::ArrayElements:
            if (const auto * array = typeid_cast<const DataTypeArray *>(type))
                return array->getNestedType().get();
            /// A `Map` is serialized as its nested `Array(Tuple(keys, values))`.
            if (const auto * map = typeid_cast<const DataTypeMap *>(type))
                return getTypeOfSubstream(map->getNestedType().get(), substream);
            return nullptr;
        case Substream::NullableElements:
            if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
                return nullable->getNestedType().get();
            return nullptr;
        case Substream::TupleElement:
            if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type))
                if (auto position = tuple->tryGetPositionByName(substream.name_of_substream))
                    return tuple->getElement(*position).get();
            return nullptr;
        case Substream::ObjectTypedPath:
        case Substream::ObjectTypedSharedDataPath:
            if (const auto * object = typeid_cast<const DataTypeObject *>(type))
                if (auto it = object->getTypedPaths().find(substream.object_path_name); it != object->getTypedPaths().end())
                    return it->second.get();
            return nullptr;
        default:
            return nullptr;
    }
}

/// Whether a substream of @path is read from an object or from a type that `getTypeOfSubstream` does not track.
bool mayPassObject(const IDataType * type, const ISerialization::SubstreamPath & path)
{
    for (const auto & substream : path)
    {
        if (!type || isObject(*type))
            return true;
        type = getTypeOfSubstream(type, substream);
    }
    return false;
}

bool hasTypedPathUnder(const DataTypeObject & type, const String & path)
{
    String prefix = path + ".";
    return std::ranges::any_of(type.getTypedPaths(), [&](const auto & typed_path) { return typed_path.first.starts_with(prefix); });
}

/// Whether converting an object @in_part to @in_storage keeps the value of the path @substream reads: the same skip rules and
/// typed paths on the path, its prefixes and, for a value that includes them, the paths under it, except @hinted_path.
bool objectPathValueIsKept(
    const IDataType * in_storage, const IDataType * in_part, const Substream & substream, std::string_view hinted_path = {})
{
    const auto * object_in_storage = typeid_cast<const DataTypeObject *>(in_storage);
    const auto * object_in_part = typeid_cast<const DataTypeObject *>(in_part);
    if (!object_in_storage || !object_in_part || object_in_storage->getPathsToSkip() != object_in_part->getPathsToSkip()
        || object_in_storage->getPathRegexpsToSkip() != object_in_part->getPathRegexpsToSkip())
        return false;

    const auto & typed_paths_in_storage = object_in_storage->getTypedPaths();
    const auto & typed_paths_in_part = object_in_part->getTypedPaths();
    auto typed_path_is_kept = [&](const String & typed_path)
    {
        auto it_in_storage = typed_paths_in_storage.find(typed_path);
        auto it_in_part = typed_paths_in_part.find(typed_path);
        if (typed_path == hinted_path || (it_in_storage == typed_paths_in_storage.end() && it_in_part == typed_paths_in_part.end()))
            return true;
        return it_in_storage != typed_paths_in_storage.end() && it_in_part != typed_paths_in_part.end()
            && it_in_storage->second->equals(*it_in_part->second);
    };

    std::optional<String> path;
    bool value_includes_paths_under = true;
    if (substream.type == Substream::ObjectSubObject)
        path = DataTypeObject::tryGetPrefixedSubcolumn(substream.name_of_substream, DataTypeObject::SUB_OBJECT_SUBCOLUMN_PREFIX);
    else if (substream.type == Substream::ObjectCombinedPath)
        path = DataTypeObject::tryGetPrefixedSubcolumn(substream.name_of_substream, DataTypeObject::COMBINED_SUBCOLUMN_PREFIX);
    else if (substream.type != Substream::ObjectDistinctPaths)
    {
        path = substream.object_path_name;
        value_includes_paths_under = false;
    }

    /// The distinct paths depend on all paths.
    if (!path)
        return std::ranges::all_of(typed_paths_in_storage, [&](const auto & typed_path) { return typed_path_is_kept(typed_path.first); })
            && std::ranges::all_of(typed_paths_in_part, [&](const auto & typed_path) { return typed_path_is_kept(typed_path.first); });

    for (size_t pos = path->find('.'); pos != String::npos; pos = path->find('.', pos + 1))
        if (!typed_path_is_kept(path->substr(0, pos)))
            return false;

    if (!typed_path_is_kept(*path))
        return false;

    if (!value_includes_paths_under)
        return true;

    String prefix = *path + ".";
    auto is_kept_under_path = [&](const auto & typed_path) { return !typed_path.first.starts_with(prefix) || typed_path_is_kept(typed_path.first); };
    return std::ranges::all_of(typed_paths_in_storage, is_kept_under_path) && std::ranges::all_of(typed_paths_in_part, is_kept_under_path);
}

bool isTypedPathSubstream(const Substream & substream)
{
    return substream.type == Substream::ObjectTypedPath || substream.type == Substream::ObjectTypedSharedDataPath;
}

/// `Dynamic` infers `JSON` for an object, and a `Variant` tries each of its variants.
bool canHoldObjectValue(const DataTypePtr & type)
{
    auto nested_type = removeNullable(removeLowCardinality(type));
    if (DataTypeObject::canParseObjectValue(nested_type) || isDynamic(nested_type))
        return true;

    const auto * variant_type = typeid_cast<const DataTypeVariant *>(nested_type.get());
    return variant_type && std::ranges::any_of(variant_type->getVariants(), canHoldObjectValue);
}

/// A type hint of a path that the part stores as a dynamic path or with another type hint, read as a whole: it is applied
/// by casting the path's value, which is how type hints are documented to apply to old parts. Hints of the types that can
/// hold an object are not, on either side, because a part stores an object as separate paths, not as the value of the path.
bool isTypeHintCastOfPath(
    const IDataType * object_in_storage,
    const IDataType * object_in_part,
    const Substream & substream_in_storage,
    const Substream & substream_in_part)
{
    bool is_typed_path_in_part = isTypedPathSubstream(substream_in_part);
    if (!isTypedPathSubstream(substream_in_storage) || (!is_typed_path_in_part && substream_in_part.type != Substream::ObjectDynamicPath)
        || substream_in_storage.object_path_name != substream_in_part.object_path_name)
        return false;

    const auto & path = substream_in_storage.object_path_name;
    if (!objectPathValueIsKept(object_in_storage, object_in_part, substream_in_part, path))
        return false;

    /// `objectPathValueIsKept` has checked that both are objects.
    const auto & typed_object_in_storage = typeid_cast<const DataTypeObject &>(*object_in_storage);
    const auto & typed_object_in_part = typeid_cast<const DataTypeObject &>(*object_in_part);
    auto hinted_type = typed_object_in_storage.getTypedPaths().find(path);
    if (hinted_type == typed_object_in_storage.getTypedPaths().end() || hasTypedPathUnder(typed_object_in_storage, path)
        || hasTypedPathUnder(typed_object_in_part, path) || canHoldObjectValue(hinted_type->second))
        return false;

    if (!is_typed_path_in_part)
        return true;

    auto type_in_part = typed_object_in_part.getTypedPaths().find(path);
    return type_in_part != typed_object_in_part.getTypedPaths().end() && !canHoldObjectValue(type_in_part->second);
}

/// The typed path with @subcolumn in its value when that value is cast to the hint (see `isTypeHintCastOfPath`). The path's object
/// is the column or an element of its `Array` / `Map` / `Tuple` wrappers.
std::optional<String> tryGetCastPathOfSubcolumn(const NameAndTypePair & subcolumn, const DataTypePtr & column_type_in_part)
{
    const auto & type_in_storage = subcolumn.getTypeInStorage();
    const auto & subcolumn_name = subcolumn.getSubcolumnName();
    auto info_in_storage = type_in_storage->tryGetSubcolumnInfo(subcolumn_name);
    if (!info_in_storage)
        return std::nullopt;

    const auto & substreams_in_storage = info_in_storage->substreams_path;
    const IDataType * owner_in_storage = type_in_storage.get();
    size_t depth = 0;
    for (; depth < substreams_in_storage.size(); ++depth)
    {
        const auto & substream = substreams_in_storage[depth];
        if (substream.type != Substream::ArrayElements && substream.type != Substream::TupleElement)
            break;

        owner_in_storage = getTypeOfSubstream(owner_in_storage, substream);
        if (!owner_in_storage)
            return std::nullopt;
    }

    if (depth + 1 >= substreams_in_storage.size() || !isTypedPathSubstream(substreams_in_storage[depth])
        || !typeid_cast<const DataTypeObject *>(owner_in_storage))
        return std::nullopt;

    auto is_name_of_typed_path = [&](const String & name)
    {
        auto info = type_in_storage->tryGetSubcolumnInfo(name);
        return info && info->substreams_path.size() == depth + 1
            && std::equal(
                   info->substreams_path.begin(), info->substreams_path.end(), substreams_in_storage.begin(), ISerialization::isSameSubstream);
    };

    std::optional<String> path;
    for (size_t pos = subcolumn_name.find('.'); pos != String::npos && !path; pos = subcolumn_name.find('.', pos + 1))
        if (is_name_of_typed_path(subcolumn_name.substr(0, pos)))
            path = subcolumn_name.substr(0, pos);

    if (!path)
        return std::nullopt;

    auto info_in_part = column_type_in_part->tryGetSubcolumnInfo(*path);
    if (!info_in_part || info_in_part->substreams_path.size() != depth + 1)
        return std::nullopt;

    const auto & substreams_in_part = info_in_part->substreams_path;
    const IDataType * owner_in_part = column_type_in_part.get();
    for (size_t i = 0; i < depth; ++i)
    {
        if (!ISerialization::isSameSubstream(substreams_in_storage[i], substreams_in_part[i]))
            return std::nullopt;

        owner_in_part = getTypeOfSubstream(owner_in_part, substreams_in_part[i]);
    }

    if (!isTypeHintCastOfPath(owner_in_storage, owner_in_part, substreams_in_storage[depth], substreams_in_part[depth]))
        return std::nullopt;

    if (!type_in_storage->getSubcolumnType(*path)->tryGetSubcolumnType(subcolumn_name.substr(path->size() + 1)))
        return std::nullopt;

    return Nested::concatenateName(subcolumn.getNameInStorage(), *path);
}

/// `IDataType::equals`, which is linear in the typed paths of a `JSON` type. Parts with the same columns share their types
/// and a query shares the table's, so a thread keeps its last results, keyed by the owners of the types as `std::owner_less`.
bool equalsTypeInPart(const DataTypePtr & type_in_storage, const DataTypePtr & type_in_part)
{
    struct Comparison
    {
        std::weak_ptr<const IDataType> type_in_storage;
        std::weak_ptr<const IDataType> type_in_part;
        bool equals = false;
    };
    thread_local std::array<Comparison, 8> comparisons;
    thread_local size_t next_comparison = 0;

    auto is_owned_by = [](const DataTypePtr & type, const std::weak_ptr<const IDataType> & owner)
    { return !type.owner_before(owner) && !owner.owner_before(type); };

    for (const auto & comparison : comparisons)
        if (is_owned_by(type_in_storage, comparison.type_in_storage) && is_owned_by(type_in_part, comparison.type_in_part))
            return comparison.equals;

    bool equals = type_in_storage->equals(*type_in_part);
    comparisons[next_comparison++ % comparisons.size()] = {type_in_storage, type_in_part, equals};
    return equals;
}

/// Whether the subcolumn of the column converted from @column_type_in_part can differ from the converted one read from the part.
bool isSubcolumnChangedByConversion(const NameAndTypePair & subcolumn, const DataTypePtr & column_type_in_part)
{
    const auto & type_in_storage = subcolumn.getTypeInStorage();
    if (!subcolumn.isSubcolumn() || equalsTypeInPart(type_in_storage, column_type_in_part))
        return false;

    auto has_object = [](const IDataType & type) { return anyInTypeTree(type, [](const IDataType & nested) { return isObject(nested); }); };
    if (!has_object(*type_in_storage) && !has_object(*column_type_in_part))
        return false;

    auto info_in_storage = type_in_storage->tryGetSubcolumnInfo(subcolumn.getSubcolumnName());
    if (!info_in_storage)
        return false;

    /// Reading the subcolumn missing from the part with the type in the table could read the part's streams of another type.
    auto info_in_part = column_type_in_part->tryGetSubcolumnInfo(subcolumn.getSubcolumnName());
    if (!info_in_part)
        return true;

    const auto & path_in_storage = info_in_storage->substreams_path;
    const auto & path_in_part = info_in_part->substreams_path;
    if (path_in_storage.empty() || path_in_part.empty())
        return true;

    /// The conversion casts a value that no object holds or is in, as the read of the subcolumn does.
    const auto & type_of_subcolumn_in_storage = *info_in_storage->data.type;
    const auto & type_of_subcolumn_in_part = *info_in_part->data.type;
    if (!mayPassObject(type_in_storage.get(), path_in_storage) && !mayPassObject(column_type_in_part.get(), path_in_part)
        && !has_object(type_of_subcolumn_in_storage) && !has_object(type_of_subcolumn_in_part))
        return false;

    if (path_in_storage.size() != path_in_part.size())
        return true;

    const IDataType * type_of_substream_in_storage = type_in_storage.get();
    const IDataType * type_of_substream_in_part = column_type_in_part.get();
    size_t last = path_in_storage.size() - 1;
    for (size_t i = 0; i <= last; ++i)
    {
        if (i == last
            && isTypeHintCastOfPath(type_of_substream_in_storage, type_of_substream_in_part, path_in_storage[i], path_in_part[i]))
            return false;

        if (!ISerialization::isSameSubstream(path_in_storage[i], path_in_part[i]))
            return true;

        if (isObjectPathSubstream(path_in_storage[i])
            && !objectPathValueIsKept(type_of_substream_in_storage, type_of_substream_in_part, path_in_storage[i]))
            return true;

        type_of_substream_in_storage = getTypeOfSubstream(type_of_substream_in_storage, path_in_storage[i]);
        type_of_substream_in_part = getTypeOfSubstream(type_of_substream_in_part, path_in_part[i]);
    }

    /// The same path in both: a cast of the value is not the conversion of the column unless the types are the same.
    return !type_of_subcolumn_in_storage.equals(type_of_subcolumn_in_part);
}

std::optional<String> getSourceOfSubcolumnForType(const NameAndTypePair & subcolumn, const DataTypePtr & column_type_in_part)
{
    if (!isSubcolumnChangedByConversion(subcolumn, column_type_in_part))
        return std::nullopt;

    if (auto cast_path = tryGetCastPathOfSubcolumn(subcolumn, column_type_in_part))
        return cast_path;

    return subcolumn.getNameInStorage();
}

}

DataTypePtr tryGetColumnTypeInPart(const IMergeTreeDataPartInfoForReader & data_part_info_for_reader, const String & name_in_part)
{
    if (auto column_in_part = data_part_info_for_reader.getColumnsDescription().tryGetPhysical(name_in_part))
        return column_in_part->type;

    const auto * missing = data_part_info_for_reader.getSerializationInfos().getMissingColumnInfo(name_in_part);
    return missing && !missing->type_name.empty() ? DataTypeFactory::instance().get(missing->type_name) : nullptr;
}

std::optional<String> getSourceOfSubcolumnInPart(
    const StorageSnapshotPtr & storage_snapshot,
    const NameAndTypePair & subcolumn,
    const DataTypePtr & column_type_in_part,
    const AlterConversionsPtr & alter_conversions)
{
    if (!subcolumn.isSubcolumn())
        return std::nullopt;

    auto source = column_type_in_part ? getSourceOfSubcolumnForType(subcolumn, column_type_in_part) : std::nullopt;
    const auto & column_name = subcolumn.getNameInStorage();
    for (const auto & patch : alter_conversions ? alter_conversions->getAllPatches() : PatchPartsForReader{})
    {
        if (source == column_name)
            break;

        auto column_name_in_patch = column_name;
        const auto & patch_alter_conversions = patch.part->getAlterConversions();
        if (patch_alter_conversions && patch_alter_conversions->isColumnRenamed(column_name_in_patch))
            column_name_in_patch = patch_alter_conversions->getColumnOldName(column_name_in_patch);

        auto column_in_patch = patch.part->getColumnsDescription().tryGetPhysical(column_name_in_patch);
        if (!column_in_patch)
            continue;

        auto source_in_patch = getSourceOfSubcolumnForType(subcolumn, column_in_patch->type);
        if (!source || source_in_patch == column_name)
            source = std::move(source_in_patch);
    }

    /// A path is read by its name, which another column of the table can have.
    if (source && *source != column_name)
    {
        auto source_in_storage = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), *source);
        if (!source_in_storage || source_in_storage->getNameInStorage() != column_name)
            source = column_name;
    }

    const auto & metadata = storage_snapshot->metadata;
    if (source
        && (std::ranges::contains(metadata->getColumnsRequiredForSortingKey(), subcolumn.name)
            || std::ranges::contains(metadata->getColumnsRequiredForPartitionKey(), subcolumn.name)))
        return std::nullopt;

    return source;
}

bool canReadTextIndexInPart(const AlterConversionsPtr & alter_conversions)
{
    /// Patches are joined by the keys read in the first step, which the text index reader cannot read.
    return !alter_conversions || !alter_conversions->hasPatches();
}

/** If some of the requested columns are not in the part,
  * then find out which columns may need to be read further,
  * so that you can calculate the DEFAULT expression for these columns.
  * Adds them to the `columns`.
  */
NameSet injectRequiredColumns(
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const StorageSnapshotPtr & storage_snapshot,
    bool with_subcolumns,
    Names & columns)
{
    NameSet required_columns{std::begin(columns), std::end(columns)};
    NameSet injected_columns;

    bool have_at_least_one_physical_column = false;
    AlterConversionsPtr alter_conversions;
    if (!data_part_info_for_reader.isProjectionPart())
        alter_conversions = data_part_info_for_reader.getAlterConversions();

    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader)
        .withSubcolumns(with_subcolumns);

    for (size_t i = 0; i < columns.size(); ++i)
    {
        /// We are going to fetch physical columns and system columns first
        if (!storage_snapshot->tryGetColumn(options, columns[i]))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column or subcolumn {} in table", columns[i]);

        /// Copy columns[i] to avoid a dangling reference: injectRequiredColumnsRecursively may call
        /// columns.emplace_back, which can reallocate the vector and invalidate any reference into it.
        String column_name = columns[i];
        have_at_least_one_physical_column |= injectRequiredColumnsRecursively(
            column_name,
            storage_snapshot,
            alter_conversions,
            data_part_info_for_reader,
            options,
            columns,
            required_columns,
            injected_columns);
    }

    /** Add a column of the minimum size.
        * Used in case when no column is needed or files are missing, but at least you need to know number of rows.
        * Adds to the columns.
        */
    if (!have_at_least_one_physical_column)
    {
        /// Use the intersection of part columns and metadata columns to find the minimum size column.
        /// The column must exist both physically in the part (to be readable) and in the current metadata
        /// (to be resolvable by the StorageSnapshot). This handles cases where the table schema has changed
        /// since the part was created: columns may have been added (not in the part) or dropped (not in metadata).
        const auto & part_columns = data_part_info_for_reader.getColumns();
        NamesAndTypesList available_columns;
        for (const auto & column : part_columns)
        {
            if (storage_snapshot->tryGetColumn(options, column.name))
                available_columns.push_back(column);
        }

        if (available_columns.empty())
            available_columns = part_columns;

        const auto minimum_size_column_name = data_part_info_for_reader.getColumnNameWithMinimumCompressedSize(available_columns);
        columns.push_back(minimum_size_column_name);
        /// correctly report added column
        injected_columns.insert(columns.back());
    }

    return injected_columns;
}

MergeTreeBlockSizePredictor::MergeTreeBlockSizePredictor(
    const MergeTreeDataPartInfoForReaderPtr & data_part_, const Names & columns, const Block & sample_block, bool allow_subcolumns_sizes_calculation_)
    : data_part(data_part_), allow_subcolumns_sizes_calculation(allow_subcolumns_sizes_calculation_)
{
    number_of_rows_in_part = data_part->getRowCount();
    /// Initialize with sample block until update won't called.
    initialize(sample_block, {}, columns);
}

void MergeTreeBlockSizePredictor::initialize(const Block & sample_block, const Columns & columns, const Names & names, bool from_update)
{
    fixed_columns_bytes_per_row = 0;
    dynamic_columns_infos.clear();

    std::unordered_set<String> names_set;
    if (!from_update)
        names_set.insert(names.begin(), names.end());

    size_t num_columns = sample_block.columns();
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & column_with_type_and_name = sample_block.getByPosition(pos);
        const auto & column_name = column_with_type_and_name.name;
        const auto & column_data = from_update ? columns[pos] : column_with_type_and_name.column;

        if (!from_update && !names_set.contains(column_name))
            continue;

        /// At least PREWHERE filter column might be const.
        if (typeid_cast<const ColumnConst *>(column_data.get()))
            continue;

        auto column_from_part = data_part->tryGetColumn(column_name);
        if ((!column_from_part || !column_from_part->isSubcolumn()) && column_data->valuesHaveFixedSize())
        {
            size_t size_of_value = column_data->sizeOfValueIfFixed();
            fixed_columns_bytes_per_row += column_data->sizeOfValueIfFixed();
            max_size_per_row_fixed = std::max(max_size_per_row_fixed, static_cast<double>(size_of_value));
        }
        else
        {
            ColumnInfo info;
            info.name = column_name;
            info.is_subcolumn = column_from_part && column_from_part->isSubcolumn();
            /// If column isn't fixed and doesn't have checksum, than take first
            ColumnSize column_size;
            if (info.is_subcolumn && allow_subcolumns_sizes_calculation)
                column_size = data_part->getSubcolumnSize(column_name);
            else
                column_size = data_part->getColumnSize(column_from_part ? column_from_part->getNameInStorage() : column_name);

            info.bytes_per_row_global = column_size.data_uncompressed
                ? static_cast<double>(column_size.data_uncompressed) / static_cast<double>(number_of_rows_in_part)
                : static_cast<double>(column_data->byteSize()) / static_cast<double>(std::max<size_t>(1, column_data->size()));

            dynamic_columns_infos.emplace_back(info);
        }
    }

    bytes_per_row_global = static_cast<double>(fixed_columns_bytes_per_row);
    for (auto & info : dynamic_columns_infos)
    {
        info.bytes_per_row = info.bytes_per_row_global;
        bytes_per_row_global += info.bytes_per_row_global;

        max_size_per_row_dynamic = std::max<double>(max_size_per_row_dynamic, info.bytes_per_row);
    }
    bytes_per_row_current = bytes_per_row_global;
}

void MergeTreeBlockSizePredictor::startBlock()
{
    block_size_bytes = 0;
    block_size_rows = 0;
    for (auto & info : dynamic_columns_infos)
        info.size_bytes = 0;
}

/// TODO: add last_read_row_in_part parameter to take into account gaps between adjacent ranges
void MergeTreeBlockSizePredictor::update(const Block & sample_block, const Columns & columns, size_t num_rows, double decay)
{
    if (columns.size() != sample_block.columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent number of columns passed to MergeTreeBlockSizePredictor. "
                        "Have {} in sample block and {} columns in list",
                        toString(sample_block.columns()), toString(columns.size()));

    if (!is_initialized_in_update)
    {
        /// Reinitialize with read block to update estimation for DEFAULT and MATERIALIZED columns without data.
        initialize(sample_block, columns, {}, true);
        is_initialized_in_update = true;
    }

    if (num_rows < block_size_rows)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Updated block has less rows ({}) than previous one ({})",
                        num_rows, block_size_rows);
    }

    size_t diff_rows = num_rows - block_size_rows;
    block_size_bytes = num_rows * fixed_columns_bytes_per_row;
    bytes_per_row_current = static_cast<double>(fixed_columns_bytes_per_row);
    block_size_rows = num_rows;

    /// Make recursive updates for each read row: v_{i+1} = (1 - decay) v_{i} + decay v_{target}
    /// Use sum of geometric sequence formula to update multiple rows: v{n} = (1 - decay)^n v_{0} + (1 - (1 - decay)^n) v_{target}
    /// NOTE: DEFAULT and MATERIALIZED columns without data has inaccurate estimation of v_{target}
    double alpha = std::pow(1. - decay, diff_rows);

    max_size_per_row_dynamic = 0;
    for (auto & info : dynamic_columns_infos)
    {
        size_t new_size = columns[sample_block.getPositionByName(info.name)]->byteSize();
        size_t diff_size = new_size - info.size_bytes;

        double local_bytes_per_row = static_cast<double>(diff_size) / static_cast<double>(diff_rows);
        info.bytes_per_row = alpha * info.bytes_per_row + (1. - alpha) * local_bytes_per_row;

        /// For subcolumns, the output column size can be much smaller than what was
        /// actually read from disk (e.g. a Map subcolumn reads the entire Map but
        /// only extracts one key's values). Prevent the estimate from dropping below
        /// the global average so that chunk sizes stay appropriate for the real I/O cost.
        if (info.is_subcolumn)
            info.bytes_per_row = std::max(info.bytes_per_row, info.bytes_per_row_global);

        info.size_bytes = new_size;
        block_size_bytes += new_size;
        bytes_per_row_current += info.bytes_per_row;

        max_size_per_row_dynamic = std::max<double>(max_size_per_row_dynamic, info.bytes_per_row);
    }
}

PrewhereExprStepPtr createLightweightDeleteStep(bool remove_filter_column)
{
    PrewhereExprStep step
    {
        .type = PrewhereExprStep::Filter,
        .actions = nullptr,
        .filter_column_name = RowExistsColumn::name,
        .remove_filter_column = remove_filter_column,
        .need_filter = true,
        .perform_alter_conversions = true,
        .columns_overwritten_by_chain = {},
        .mutation_version = std::nullopt,
    };

    return std::make_shared<PrewhereExprStep>(std::move(step));
}

void addPatchPartsColumns(
    MergeTreeReadTaskColumns & result,
    const StorageSnapshotPtr & storage_snapshot,
    const GetColumnsOptions & options,
    const PatchPartsForReader & patch_parts,
    const Names & all_columns_to_read,
    bool has_lightweight_delete)
{
    if (patch_parts.empty())
        return;

    NameSet required_key_columns;
    result.patch_columns.resize(patch_parts.size());

    for (size_t i = 0; i < patch_parts.size(); ++i)
    {
        NameSet patch_columns_to_read_set;

        const auto & patch_part_columns = patch_parts[i].part->getColumnsDescription();
        const auto & alter_conversions = patch_parts[i].part->getAlterConversions();

        for (const auto & column_name : all_columns_to_read)
        {
            auto column_in_storage = storage_snapshot->getColumn(options, column_name);
            auto column_name_in_patch = column_in_storage.getNameInStorage();

            if (alter_conversions && alter_conversions->isColumnRenamed(column_name_in_patch))
                column_name_in_patch = alter_conversions->getColumnOldName(column_name_in_patch);

            if (!patch_part_columns.hasPhysical(column_name_in_patch))
                continue;

            /// A patch of another type extracts the subcolumn from its source after conversions, so it reads the source too.
            /// Applied before conversions it cannot, and the part extracts the subcolumn from the patched source instead.
            const auto & type_in_patch = patch_part_columns.getPhysical(column_name_in_patch).type;
            if (auto source = getSourceOfSubcolumnInPart(storage_snapshot, column_in_storage, type_in_patch, nullptr))
            {
                if (!patch_parts[i].perform_alter_conversions)
                    continue;
                patch_columns_to_read_set.insert(*source);
            }

            /// Add requested column name, not the column name in patch, for correct query analysis and applying patches.
            /// This column name will be translated to the column name in patch in MergeTree reader.
            patch_columns_to_read_set.insert(column_name);
        }

        if (has_lightweight_delete && patch_part_columns.has(RowExistsColumn::name))
        {
            patch_columns_to_read_set.insert(RowExistsColumn::name);
        }

        auto patch_key_columns = getKeyColumnsRequiredForPatch(patch_parts[i]);
        patch_columns_to_read_set.insert(patch_key_columns.begin(), patch_key_columns.end());
        required_key_columns.insert(patch_key_columns.begin(), patch_key_columns.end());

        Names patch_columns_to_read_names(patch_columns_to_read_set.begin(), patch_columns_to_read_set.end());

        fiu_do_on(FailPoints::patch_parts_reverse_column_order,
        {
            /// Simulate non-deterministic NameSet iteration producing different column
            /// orderings for different patches. This reproduces the bug fixed in
            /// getUpdatedHeader (applyPatches.cpp) where sortColumns() normalizes order
            /// before the positional assertCompatibleHeader comparison.
            if (i % 2 == 1)
                std::reverse(patch_columns_to_read_names.begin(), patch_columns_to_read_names.end());
        });

        result.patch_columns[i] = storage_snapshot->getColumnsByNames(options, patch_columns_to_read_names);
    }

    auto & first_step_columns = result.pre_columns.empty() ? result.columns : result.pre_columns.front();
    auto first_step_columns_set = first_step_columns.getNameSet();

    for (const auto & key_column_name : required_key_columns)
    {
        if (!first_step_columns_set.contains(key_column_name))
        {
            auto column = storage_snapshot->getColumn(options, key_column_name);
            first_step_columns.push_back(std::move(column));
        }
    }
}

MergeTreeReadTaskColumns getReadTaskColumns(
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const StorageSnapshotPtr & storage_snapshot,
    const Names & required_columns,
    const FilterDAGInfoPtr & row_level_filter,
    const PrewhereInfoPtr & prewhere_info,
    const PrewhereExprSteps & mutation_steps,
    const IndexReadTasks & index_read_tasks,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings,
    bool with_subcolumns)
{
    MergeTreeReadTaskColumns result;
    NameSet columns_from_previous_steps;
    Names column_to_read_after_prewhere = required_columns;

    /// Inject columns required for defaults evaluation
    injectRequiredColumns(data_part_info_for_reader, storage_snapshot, with_subcolumns, column_to_read_after_prewhere);

    auto options = GetColumnsOptions(GetColumnsOptions::All)
        .withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader)
        .withSubcolumns(with_subcolumns);

    auto add_step = [&](const PrewhereExprStep & step, bool is_on_fly_mutation_step)
    {
        /// Computation results from previous steps might be used in the current step as well. In such a case these
        /// computed columns will be present in the current step inputs. They don't need to be read from the disk so
        /// exclude them from the list of columns to read. This filtering must be done before injecting required
        /// columns to avoid adding unnecessary columns or failing to find required columns that are computation
        /// results from previous steps.
        /// Example: step1: sin(a)>b, step2: sin(a)>c

        /// If actions are empty then step is a filter step with a plain identifier as a filter column.
        Names required_source_columns;
        if (step.actions)
            required_source_columns = step.actions->getActionsDAG().getRequiredColumnsNames();
        else if (!step.filter_column_name.empty())
            required_source_columns = Names{step.filter_column_name};

        Names step_column_names;
        for (const auto & name : required_source_columns)
        {
            if (!columns_from_previous_steps.contains(name))
                step_column_names.push_back(name);
        }

        const bool has_adaptive_granularity = data_part_info_for_reader.getIndexGranularityInfo().mark_type.adaptive;
        const size_t num_required_columns = step_column_names.size();

        /// If part has non-adaptive granularity we always have to read at least one column
        /// because we cannot determine the correct size of the last granule without reading data.
        if (!step_column_names.empty() || !has_adaptive_granularity)
        {
            injectRequiredColumns(
                data_part_info_for_reader, storage_snapshot,
                with_subcolumns, step_column_names);
        }

        /// More columns could have been added, filter them as well by the list of columns from previous steps.
        Names columns_to_read_in_step;
        for (size_t i = 0; i < step_column_names.size(); ++i)
        {
            const auto & name = step_column_names[i];
            /// An on-fly mutation ends with a projection to its own columns, so a column injected here to fill or extract
            /// another one does not reach the next steps, which read it again if they need it.
            if (is_on_fly_mutation_step && i >= num_required_columns)
            {
                if (!columns_from_previous_steps.contains(name))
                    columns_to_read_in_step.push_back(name);
            }
            else if (columns_from_previous_steps.emplace(name).second)
                columns_to_read_in_step.push_back(name);
        }

        /// Add results of the step to the list of already "known" columns so that we don't read or compute them again.
        if (step.actions)
        {
            for (const auto & name : step.actions->getActionsDAG().getNames())
                columns_from_previous_steps.insert(name);
        }

        result.pre_columns.push_back(storage_snapshot->getColumnsByNames(options, columns_to_read_in_step));
    };

    for (const auto & step : mutation_steps)
        add_step(*step, /*is_on_fly_mutation_step=*/ step->actions != nullptr);

    if (prewhere_info || row_level_filter || !index_read_tasks.empty())
    {
        auto prewhere_actions = MergeTreeSelectProcessor::getPrewhereActions(
            row_level_filter,
            prewhere_info,
            index_read_tasks,
            actions_settings,
            reader_settings.enable_multiple_prewhere_read_steps,
            reader_settings.force_short_circuit_execution,
            reader_settings.read_ahead_prewhere_columns);

        for (const auto & step : prewhere_actions.steps)
            add_step(*step, /*is_on_fly_mutation_step=*/ false);
    }

    /// Remove columns read in prewehere from the list of columns to read.
    Names post_column_names;
    for (const auto & name : column_to_read_after_prewhere)
    {
        if (!columns_from_previous_steps.contains(name))
            post_column_names.push_back(name);
    }

    result.columns = storage_snapshot->getColumnsByNames(options, post_column_names);
    return result;
}

MergeTreeReadTaskColumns getReadTaskColumnsForMerge(
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    const StorageSnapshotPtr & storage_snapshot,
    const Names & required_columns,
    const PrewhereExprSteps & mutation_steps)
{
    return getReadTaskColumns(
        data_part_info_for_reader,
        storage_snapshot,
        required_columns,
        /*row_level_filter=*/ nullptr,
        /*prewhere_info=*/ nullptr,
        mutation_steps,
        /*index_read_tasks*/ {},
        /*actions_settings=*/ {},
        /*reader_settings=*/ MergeTreeReaderSettings::createFromSettings(),
        storage_snapshot->storage.supportsSubcolumns());
}

}
