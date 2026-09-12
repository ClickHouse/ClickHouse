#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

/// Prefixed subcolumns look like "<prefix>`first_path_element`.rest": the back-quote distinguishes
/// them from an ordinary path starting with the prefix character, e.g. "@`a`" versus "@a".
static bool isPrefixedSubcolumn(std::string_view subcolumn_name, char prefix)
{
    return subcolumn_name.size() >= 2 && subcolumn_name[0] == prefix && subcolumn_name[1] == '`';
}

namespace
{

/// `Substream` has no `operator==`, and its identity is the type plus whichever name member that
/// type uses: `name_of_substream` for `Substream::named_types`, `object_path_name` for the two
/// Object path steps, `variant_element_name` for Variant elements, `bucket` for bucketed ones.
/// A substream only ever fills the one it needs, so comparing all of them is exhaustive.
bool substreamsEqual(const ISerialization::Substream & lhs, const ISerialization::Substream & rhs)
{
    return lhs.type == rhs.type && lhs.name_of_substream == rhs.name_of_substream
        && lhs.object_path_name == rhs.object_path_name && lhs.variant_element_name == rhs.variant_element_name
        && lhs.bucket == rhs.bucket;
}

/// A name resolved through the table metadata: the storage column it belongs to, and which substream
/// of that column's type it is (empty when the name is the column itself).
struct ResolvedName
{
    String name_in_storage;
    ISerialization::SubstreamPath substreams_path;
};

std::optional<ResolvedName> substreamPathOf(const NameAndTypePair & column)
{
    if (!column.isSubcolumn())
        return ResolvedName{column.getNameInStorage(), {}};

    auto info = column.getTypeInStorage()->tryGetSubcolumnInfo(column.getSubcolumnName());
    if (!info)
        return std::nullopt;

    return ResolvedName{column.getNameInStorage(), std::move(info->substreams_path)};
}

/// Resolve `name` with the machinery the query itself uses, so index analysis and the read agree by
/// construction, and in the resolver's own order: whole-name column and precomputed static
/// subcolumn first, a dynamic path under a declared column only after. The order is part of the
/// answer, since a registered static subcolumn wins over a shorter dynamic root that also claims
/// the name.
///
/// The dynamic stage walks the declared columns rather than the name's dot splits, which is what
/// `ColumnsDescription::tryGetColumn` does once dynamic subcolumns are enabled. Both reach the same
/// root - shortest declared prefix that owns the remainder - but a name can embed a folded constant,
/// so splitting it costs O(length^2) allocated bytes, and index analysis runs before any data is
/// read and observes no cancellation.
std::optional<ResolvedName> resolveName(const ColumnsDescription & columns, const String & name)
{
    if (auto column = columns.tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), name))
        return substreamPathOf(*column);

    for (size_t tried_length = 0;;)
    {
        /// Shortest declared name that is a dot-prefix of `name` and longer than the last one tried.
        const ColumnDescription * root = nullptr;
        for (const auto & column : columns)
        {
            if (column.name.size() <= tried_length || (root && column.name.size() >= root->name.size()))
                continue;
            if (name.size() <= column.name.size() + 1 || !name.starts_with(column.name)
                || name[column.name.size()] != '.')
                continue;

            root = &column;
        }

        if (!root)
            return std::nullopt;

        tried_length = root->name.size();
        if (root->type->hasDynamicSubcolumns())
        {
            auto subcolumn_name = std::string_view(name).substr(root->name.size() + 1);
            if (auto info = root->type->tryGetSubcolumnInfo(subcolumn_name))
                return ResolvedName{root->name, std::move(info->substreams_path)};
        }
    }
}

/// The JSON path `name` denotes, when it is reached from `json_column` by JSON path steps alone.
std::optional<String> tryGetPathThroughJSONColumn(const ResolvedName & json_column, const ResolvedName & name)
{
    if (json_column.name_in_storage != name.name_in_storage)
        return std::nullopt;

    const auto & prefix = json_column.substreams_path;
    const auto & full = name.substreams_path;

    if (prefix.size() >= full.size())
        return std::nullopt;

    for (size_t i = 0; i < prefix.size(); ++i)
        if (!substreamsEqual(prefix[i], full[i]))
            return std::nullopt;

    String path;
    size_t position = prefix.size();
    size_t after_last_step = position;
    while (position < full.size())
    {
        if (SerializationObject::isTransparentWrapper(full[position]))
        {
            ++position;
            continue;
        }

        if (!SerializationObject::isPathStep(full[position]))
            break;

        if (!path.empty())
            path += '.';
        path += full[position].object_path_name;
        after_last_step = ++position;
    }

    /// Anything beyond the path steps reads a property derived from the path's value, and its truth
    /// does not follow from the path being present: a `.null` map yields 1 exactly where the path is
    /// ABSENT, so answering it from the path set drops the only matching rows.
    if (path.empty() || !SerializationObject::isAllowedPathTail(full, after_last_step))
        return std::nullopt;

    return path;
}

}

const ColumnsDescription & getColumnsToMatchJSONSubcolumn(const StorageMetadataPtr & metadata_snapshot)
{
    static const ColumnsDescription no_columns;
    return metadata_snapshot ? metadata_snapshot->getColumns() : no_columns;
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Block & header,
    const String & json_function_name,
    const ColumnsDescription & columns)
{
    return tryMatchJSONSubcolumnToIndex(column_name, header.getNames(), json_function_name, columns);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Names & index_columns,
    const String & json_function_name,
    const ColumnsDescription & columns)
{
    /// Scan the index columns, not the dot positions of the name: the name can embed a folded
    /// constant, so its length is unbounded while `index_columns` is not.
    const std::string_view name = column_name;
    const size_t json_column_offset = json_function_name.size() + 1;

    /// Resolved on the first textual match and then reused: most names match no index column at all,
    /// and resolution walks the metadata.
    std::optional<ResolvedName> resolved_name;
    bool name_resolved = false;

    std::string_view matched_json_column;
    std::string_view matched_subcolumn;
    String matched_path;
    size_t matched_position = 0;
    bool matched = false;

    for (size_t position = 0; position < index_columns.size(); ++position)
    {
        const std::string_view entry = index_columns[position];

        /// Entry must be `json_function_name(X)` with a non-empty X.
        if (entry.size() < json_column_offset + 2 || entry.back() != ')' || !entry.starts_with(json_function_name)
            || entry[json_function_name.size()] != '(')
            continue;

        const std::string_view json_column = entry.substr(json_column_offset, entry.size() - json_column_offset - 1);

        /// The name must be `X.<non-empty subcolumn>`.
        if (json_column.size() + 1 >= name.size() || !name.starts_with(json_column) || name[json_column.size()] != '.')
            continue;

        /// Shortest X wins, ties resolve to the first entry: several entries can match one name.
        if (matched && json_column.size() >= matched_json_column.size())
            continue;

        if (!name_resolved)
        {
            resolved_name = resolveName(columns, column_name);
            name_resolved = true;
        }

        if (!resolved_name)
            return std::nullopt;

        /// Per candidate, not once after the loop: a name may be a path of a longer index column
        /// while merely looking like a path of a shorter one, and an index can carry several JSON
        /// columns, so rejecting the shortest match must not discard the valid longer one.
        auto resolved_json_column = resolveName(columns, String(json_column));
        if (!resolved_json_column)
            continue;

        auto path = tryGetPathThroughJSONColumn(*resolved_json_column, *resolved_name);
        if (!path)
            continue;

        matched_json_column = json_column;
        matched_subcolumn = name.substr(json_column.size() + 1);
        matched_path = std::move(*path);
        matched_position = position;
        matched = true;
    }

    if (!matched)
        return std::nullopt;

    /// Sub-object (^) and combined literal+sub-object (@) access cannot use the index: such
    /// subcolumn is not NULL when the path has only sub-paths, so the presence of the path
    /// itself is not an equivalent condition.
    if (isPrefixedSubcolumn(matched_subcolumn, DataTypeObject::SUB_OBJECT_SUBCOLUMN_PREFIX)
        || isPrefixedSubcolumn(matched_subcolumn, DataTypeObject::COMBINED_SUBCOLUMN_PREFIX))
        return std::nullopt;

    return JSONSubcolumnIndexInfo{
        .json_column_name = String(matched_json_column),
        .path = std::move(matched_path),
        .header_position = matched_position,
    };
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Block & header,
    const String & json_function_name,
    const ColumnsDescription & columns)
{
    return tryMatchNodeToJSONIndex(node, header.getNames(), json_function_name, columns);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns,
    const String & json_function_name,
    const ColumnsDescription & columns)
{
    auto json_info = tryMatchJSONSubcolumnToIndex(node.getColumnName(), index_columns, json_function_name, columns);

    /// Try CAST unwrapping: CAST(json.path, 'Type') or _CAST(json.path, 'Type')
    if (!json_info && node.isFunction())
    {
        auto func = node.toFunctionNode();
        auto fname = func.getFunctionName();
        if ((fname == "CAST" || fname == "_CAST") && func.getArgumentsSize() == 2)
            json_info = tryMatchJSONSubcolumnToIndex(
                func.getArgumentAt(0).getColumnName(), index_columns, json_function_name, columns);
    }

    return json_info;
}

bool isJSONPathFilterSafe(
    const DataTypePtr & key_expression_type,
    const Field & value_field)
{
    /// Types that can contain NULL (Dynamic, Nullable, LowCardinality(Nullable), Variant)
    /// store NULL for missing paths — always safe to skip.
    if (canContainNull(*key_expression_type))
        return true;

    /// Non-nullable type: missing path produces the type's default value.
    /// If comparing to the default, we cannot safely skip the granule.
    /// Convert value_field to the key expression type before comparing.
    auto converted = convertFieldToType(value_field, *key_expression_type);
    if (converted == key_expression_type->getDefault())
        return false;

    return true;
}

}
