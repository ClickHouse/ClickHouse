#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>

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
        if (argument_type && typeid_cast<const DataTypeObject *>(argument_type.get()))
            result.emplace(position, argument_type);
    }

    return result;
}

/// A typed path of a `JSON` type is reported by `JSONAllPaths` and `JSONAllValues` verbatim, and
/// their contents are never recursed into: a `JSON(a JSON)` column holding `{"a":{"b":42}}` yields
/// the granule path set `['a']`, and a `JSON(a Tuple(b Int64))` column yields the same. A subcolumn
/// that reaches inside a typed path (`json.a.b`, whose path is `a.b`) is therefore absent from every
/// granule, while reading it returns the value that lives inside the typed path's own column - so
/// matching such a subcolumn to the index would prune every granule and silently lose rows.
static bool pathIsInsideTypedPath(const DataTypePtr & json_type, const String & path)
{
    const auto * object_type = typeid_cast<const DataTypeObject *>(json_type.get());
    if (!object_type)
        return false;

    for (const auto & [typed_path, _] : object_type->getTypedPaths())
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

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Block & header,
    const String & json_function_name,
    const JSONIndexArgumentTypes & json_argument_types)
{
    return tryMatchJSONSubcolumnToIndex(column_name, header.getNames(), json_function_name, json_argument_types);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Names & index_columns,
    const String & json_function_name,
    const JSONIndexArgumentTypes & json_argument_types)
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
    const JSONIndexArgumentTypes & json_argument_types)
{
    return tryMatchNodeToJSONIndex(node, header.getNames(), json_function_name, json_argument_types);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns,
    const String & json_function_name,
    const JSONIndexArgumentTypes & json_argument_types)
{
    auto json_info = tryMatchJSONSubcolumnToIndex(node.getColumnName(), index_columns, json_function_name, json_argument_types);

    /// Try CAST unwrapping: CAST(json.path, 'Type') or _CAST(json.path, 'Type')
    if (!json_info && node.isFunction())
    {
        auto func = node.toFunctionNode();
        auto fname = func.getFunctionName();
        if ((fname == "CAST" || fname == "_CAST") && func.getArgumentsSize() == 2)
            json_info = tryMatchJSONSubcolumnToIndex(
                func.getArgumentAt(0).getColumnName(), index_columns, json_function_name, json_argument_types);
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
