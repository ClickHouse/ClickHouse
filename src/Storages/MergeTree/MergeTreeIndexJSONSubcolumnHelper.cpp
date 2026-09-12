#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <algorithm>

namespace DB
{

/// Extract the JSON path from a subcolumn name, stripping any `.:\`Type\`` suffix.
/// For example:
///   "a.b"            -> "a.b"
///   "a.b.:`Int64`"   -> "a.b"
///   "a.b.:`Array(Int64)`"  -> "a.b"
static String extractPathFromSubcolumn(std::string_view subcolumn_name)
{
    /// Dynamic type subcolumn looks like "some.path.:`TypeName`..."
    /// Find the ".:`" pattern that marks the start of the type specifier.
    auto pos = subcolumn_name.find(".:`");
    if (pos == std::string_view::npos)
        return String(subcolumn_name);

    return String(subcolumn_name.substr(0, pos));
}

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

/// Whether the substream path from `from` onward descends into the value stored at the path instead
/// of reading a property derived from it. Both index contents require this, for different reasons.
/// `JSONAllValues` holds the values the paths carry, so a length or a discriminator is a constant
/// that value set can never contain and the granule is pruned exactly when the predicate is true.
/// `JSONAllPaths` answers from the path set, which is equivalent only while an absent path makes the
/// predicate false; a null map is 1 precisely where the path is ABSENT, so it inverts that.
/// The allowed set is closed: a substream type nobody has classified must refuse, not slip through.
bool isValuePreservingTail(const ISerialization::SubstreamPath & path, size_t from)
{
    using Substream = ISerialization::Substream;

    for (size_t i = from; i < path.size(); ++i)
    {
        switch (path[i].type)
        {
            case Substream::ArrayElements:
            case Substream::NullableElements:
            case Substream::TupleElement:
            case Substream::MapKeyValue:
            case Substream::VariantElements:
            case Substream::VariantElement:
            case Substream::DynamicData:
            case Substream::ObjectData:
            case Substream::ObjectTypedPath:
            case Substream::ObjectDynamicPath:
                continue;
            default:
                return false;
        }
    }

    return true;
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
std::optional<ResolvedName> resolveName(const ColumnsDescription & columns, const String & name)
{
    if (auto column = columns.tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), name))
        return substreamPathOf(*column);

    /// Every declared column that could own a path of this name, in ONE pass over the schema: index
    /// analysis precedes any read and observes no cancellation, so its cost must stay bounded by the
    /// schema rather than by the name, which can embed a folded constant.
    std::vector<const ColumnDescription *> roots;
    for (const auto & column : columns)
    {
        if (name.size() <= column.name.size() + 1 || !name.starts_with(column.name)
            || name[column.name.size()] != '.' || !column.type->hasDynamicSubcolumns())
            continue;

        roots.push_back(&column);
    }

    /// Shortest first, as the resolver does. Two distinct names cannot tie here: both are dot-prefixes
    /// of `name`, so equal length makes them the same name.
    std::sort(roots.begin(), roots.end(), [](const auto * lhs, const auto * rhs) { return lhs->name.size() < rhs->name.size(); });

    for (const auto * root : roots)
    {
        auto subcolumn_name = std::string_view(name).substr(root->name.size() + 1);
        if (auto info = root->type->tryGetSubcolumnInfo(subcolumn_name))
            return ResolvedName{root->name, std::move(info->substreams_path)};
    }

    return std::nullopt;
}

/// Whether `name` is reached from `json_column` by at least one JSON path step, followed only by
/// descents that still read the value stored at that path.
bool isJSONPathOfColumn(const ResolvedName & json_column, const ResolvedName & name)
{
    if (json_column.name_in_storage != name.name_in_storage)
        return false;

    const auto & prefix = json_column.substreams_path;
    const auto & full = name.substreams_path;

    if (prefix.size() >= full.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
        if (!substreamsEqual(prefix[i], full[i]))
            return false;

    bool path_step_seen = false;
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

        path_step_seen = true;
        after_last_step = ++position;
    }

    return path_step_seen && isValuePreservingTail(full, after_last_step);
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

    std::string_view matched_json_column;
    std::string_view matched_subcolumn;
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

        matched_json_column = json_column;
        matched_subcolumn = name.substr(json_column.size() + 1);
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

    /// Only the entry the scan selected is validated; a longer entry that also owns the name is not
    /// tried in its place. Probing that one asks for the path the name denotes inside it, and
    /// `JSONAllPaths` emits only the first element of a path that continues into a nested object.
    auto resolved_json_column = resolveName(columns, String(matched_json_column));
    auto resolved_name = resolveName(columns, column_name);
    if (!resolved_json_column || !resolved_name)
        return std::nullopt;

    if (!isJSONPathOfColumn(*resolved_json_column, *resolved_name))
        return std::nullopt;

    String path = extractPathFromSubcolumn(matched_subcolumn);
    if (path.empty())
        return std::nullopt;

    return JSONSubcolumnIndexInfo{
        .json_column_name = String(matched_json_column),
        .path = std::move(path),
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
