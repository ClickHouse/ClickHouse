#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/TypeTree.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <algorithm>

namespace DB
{

JSONIndexArgumentTypes collectJSONIndexArgumentTypes(const ExpressionActions & index_expression)
{
    JSONIndexArgumentTypes result;

    const auto & outputs = index_expression.getActionsDAG().getOutputs();
    for (size_t position = 0; position < outputs.size(); ++position)
    {
        const auto * node = outputs[position];
        if (!node || node->type != ActionsDAG::ActionType::FUNCTION || node->children.empty())
            continue;

        const auto & argument_type = node->children.front()->result_type;
        if (!argument_type)
            continue;

        /// `JSONAllPaths` and `JSONAllValues` accept a `Nullable(JSON)` argument through the default
        /// adapter for `NULL`, and its subcolumns resolve like those of a plain `JSON` column.
        auto object_type = removeLowCardinalityAndNullable(argument_type);
        if (typeid_cast<const DataTypeObject *>(object_type.get()))
            result.emplace(position, object_type);
    }

    return result;
}

/// A typed path of a `JSON` type is reported by `JSONAllPaths` and `JSONAllValues` verbatim, and
/// their contents are never recursed into: a `JSON(a JSON)` column holding `{"a":{"b":42}}` yields
/// the granule path set `['a']`, and a `JSON(a Tuple(b Int64))` column yields the same. A subcolumn
/// that reaches inside a typed path (`json.a.b`, whose path is `a.b`) is therefore absent from every
/// granule, while reading it returns the value that lives inside the typed path's own column - so
/// matching such a subcolumn to the index would prune every granule and silently lose rows.
///
/// Typed paths may overlap: `JSON(a Array(JSON), a.b Int64)` declares both `a` and `a.b`, and the
/// subcolumn `json.a.b` reads the typed path `a.b` itself, not something inside `a`. A path that is
/// a declared typed path is reported by the index verbatim, so it keeps using it regardless of any
/// shorter typed path it extends.
static bool pathIsInsideTypedPath(const DataTypePtr & json_type, const String & path)
{
    const auto * object_type = typeid_cast<const DataTypeObject *>(json_type.get());
    if (!object_type)
        return false;

    const auto & typed_paths = object_type->getTypedPaths();
    if (typed_paths.contains(path))
        return false;

    for (const auto & [typed_path, _] : typed_paths)
    {
        if (path.size() > typed_path.size() && path.starts_with(typed_path) && path[typed_path.size()] == '.')
            return true;
    }

    return false;
}

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

/// `Substream` has no `operator==`; its identity is the type plus the one name member that type fills,
/// so comparing every name member is exhaustive.
bool substreamsEqual(const ISerialization::Substream & lhs, const ISerialization::Substream & rhs)
{
    return lhs.type == rhs.type && lhs.name_of_substream == rhs.name_of_substream
        && lhs.object_path_name == rhs.object_path_name && lhs.variant_element_name == rhs.variant_element_name
        && lhs.bucket == rhs.bucket;
}

/// Whether the substreams from `from` onward read the value stored at the JSON path, not a property derived
/// from it: `JSONAllValues` never holds a length or a discriminator, and a null map is 1 where the path is absent.
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

/// `substreams_path` is empty when the name is the storage column itself.
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

/// Resolves `name` in the order `ColumnsDescription` itself uses, which decides the owner: a whole column or a
/// registered static subcolumn first, then a dynamic path under the shortest declared root.
std::optional<ResolvedName> resolveName(const ColumnsDescription & columns, const String & name)
{
    if (auto column = columns.tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), name))
        return substreamPathOf(*column);

    /// One pass over the schema: `name` can embed a folded constant, and index analysis cannot be cancelled.
    std::vector<const ColumnDescription *> roots;
    for (const auto & column : columns)
    {
        if (name.size() <= column.name.size() + 1 || !name.starts_with(column.name)
            || name[column.name.size()] != '.' || !column.type->hasDynamicSubcolumns())
            continue;

        roots.push_back(&column);
    }

    /// Shortest first, as the resolver does; two roots cannot tie, both being dot-prefixes of `name`.
    std::sort(roots.begin(), roots.end(), [](const auto * lhs, const auto * rhs) { return lhs->name.size() < rhs->name.size(); });

    for (const auto * root : roots)
    {
        auto subcolumn_name = std::string_view(name).substr(root->name.size() + 1);
        if (auto info = root->type->tryGetSubcolumnInfo(subcolumn_name))
            return ResolvedName{root->name, std::move(info->substreams_path)};
    }

    return std::nullopt;
}

/// Whether `name` is `json_column` followed by at least one JSON path step and then a value-preserving tail.
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
    const JSONIndexArgumentTypes & json_argument_types,
    const ColumnsDescription & columns)
{
    return tryMatchJSONSubcolumnToIndex(column_name, header.getNames(), json_function_name, json_argument_types, columns);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Names & index_columns,
    const String & json_function_name,
    const JSONIndexArgumentTypes & json_argument_types,
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

    /// Only the selected entry is validated: a longer owner would be probed for a nested path `JSONAllPaths` never emits.
    auto resolved_json_column = resolveName(columns, String(matched_json_column));
    auto resolved_name = resolveName(columns, column_name);
    if (!resolved_json_column || !resolved_name)
        return std::nullopt;

    if (!isJSONPathOfColumn(*resolved_json_column, *resolved_name))
        return std::nullopt;

    String path = extractPathFromSubcolumn(matched_subcolumn);
    if (path.empty())
        return std::nullopt;

    /// The index knows nothing about what is stored inside a declared typed path.
    if (auto it = json_argument_types.find(matched_position); it != json_argument_types.end() && pathIsInsideTypedPath(it->second, path))
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
    const JSONIndexArgumentTypes & json_argument_types,
    const ColumnsDescription & columns)
{
    return tryMatchNodeToJSONIndex(node, header.getNames(), json_function_name, json_argument_types, columns);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns,
    const String & json_function_name,
    const JSONIndexArgumentTypes & json_argument_types,
    const ColumnsDescription & columns)
{
    auto json_info = tryMatchJSONSubcolumnToIndex(node.getColumnName(), index_columns, json_function_name, json_argument_types, columns);

    /// Try CAST unwrapping: CAST(json.path, 'Type') or _CAST(json.path, 'Type')
    if (!json_info && node.isFunction())
    {
        auto func = node.toFunctionNode();
        auto fname = func.getFunctionName();
        if ((fname == "CAST" || fname == "_CAST") && func.getArgumentsSize() == 2)
            json_info = tryMatchJSONSubcolumnToIndex(
                func.getArgumentAt(0).getColumnName(), index_columns, json_function_name, json_argument_types, columns);
    }

    return json_info;
}

bool isJSONPathFilterSafe(
    const DataTypePtr & key_expression_type,
    const Field & value_field,
    const DataTypePtr & value_type)
{
    /// Types that can contain NULL (Dynamic, Nullable, LowCardinality(Nullable), Variant)
    /// store NULL for missing paths — always safe to skip.
    if (canContainNull(*key_expression_type))
        return true;

    /// Non-nullable type: missing path produces the type's default value.
    /// If comparing to the default, we cannot safely skip the granule.
    /// An `Enum` constant keeps its labels in its own type and the comparison uses the label rather
    /// than the underlying number, so it has to be converted with that type.
    DataTypePtr unwrapped_value_type;
    const IDataTypeEnum * enum_source = nullptr;
    if (value_type)
    {
        unwrapped_value_type = removeLowCardinalityAndNullable(value_type);

        /// A `Variant` or `Dynamic` constant hides its active alternative, so an `Enum` cannot be ruled out.
        const WhichDataType which_value(unwrapped_value_type);
        if (which_value.isVariant() || which_value.isDynamic())
            return false;

        enum_source = dynamic_cast<const IDataTypeEnum *>(unwrapped_value_type.get());

        /// Only the outermost type reaches the conversion below: `convertFieldToType` recurses into the
        /// elements of a composite without theirs, so a nested `Enum` label, or an alternative that may
        /// hold one, is absent from the converted value.
        /// The outermost type is not in question here: an `Enum` there is the `enum_source`, and a
        /// `Variant` or `Dynamic` there was ruled out above. Only what is below it is lost.
        bool nested_source_type_lost = false;
        const size_t num_children = unwrapped_value_type->getNumberOfChildren();
        for (size_t i = 0; i < num_children && !nested_source_type_lost; ++i)
        {
            nested_source_type_lost = anyInTypeTree(*unwrapped_value_type->getChild(i), [](const IDataType & nested)
            {
                const WhichDataType which_nested(nested);
                return which_nested.isEnum() || which_nested.isVariant() || which_nested.isDynamic();
            });
        }
        if (nested_source_type_lost)
            return false;
    }
    auto converted = convertFieldToType(value_field, *key_expression_type, enum_source);
    if (converted == key_expression_type->getDefault())
        return false;

    return true;
}

}
