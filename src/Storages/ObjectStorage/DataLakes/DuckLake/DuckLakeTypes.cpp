#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeTypes.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/TimezoneMixin.h>

#include <Common/Exception.h>

#include <Poco/String.h>

#include <fmt/format.h>

#include <algorithm>
#include <functional>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
}

namespace DuckLake
{

DataTypePtr parseScalarType(const String & type_text)
{
    const String type = Poco::toLower(type_text);

    if (type == "boolean")
        return std::make_shared<DataTypeUInt8>();
    if (type == "int8")
        return std::make_shared<DataTypeInt8>();
    if (type == "int16")
        return std::make_shared<DataTypeInt16>();
    if (type == "int32")
        return std::make_shared<DataTypeInt32>();
    if (type == "int64")
        return std::make_shared<DataTypeInt64>();
    if (type == "int128")
        return std::make_shared<DataTypeInt128>();
    if (type == "uint8")
        return std::make_shared<DataTypeUInt8>();
    if (type == "uint16")
        return std::make_shared<DataTypeUInt16>();
    if (type == "uint32")
        return std::make_shared<DataTypeUInt32>();
    if (type == "uint64")
        return std::make_shared<DataTypeUInt64>();
    if (type == "uint128")
        return std::make_shared<DataTypeUInt128>();
    if (type == "float32")
        return std::make_shared<DataTypeFloat32>();
    if (type == "float64")
        return std::make_shared<DataTypeFloat64>();
    if (type == "time")
        return std::make_shared<DataTypeTime>();
    if (type == "time_ns")
        return std::make_shared<DataTypeTime64>(9);
    if (type == "date")
        return std::make_shared<DataTypeDate32>();
    if (type == "timestamp" || type == "timestamp_us")
        return std::make_shared<DataTypeDateTime64>(6);
    if (type == "timestamp_ms")
        return std::make_shared<DataTypeDateTime64>(3);
    if (type == "timestamp_ns")
        return std::make_shared<DataTypeDateTime64>(9);
    if (type == "timestamp_s")
        return std::make_shared<DataTypeDateTime64>(0);
    if (type == "timestamptz")
        return std::make_shared<DataTypeDateTime64>(6, TimezoneMixin{"UTC"});
    if (type == "timestamptz_ns")
        return std::make_shared<DataTypeDateTime64>(9, TimezoneMixin{"UTC"});
    if (type == "varchar" || type == "json" || type == "blob")
        return std::make_shared<DataTypeString>();
    if (type == "uuid")
        return std::make_shared<DataTypeUUID>();

    if (type.starts_with("decimal(") && type.ends_with(')'))
    {
        const String inner = type.substr(8, type.size() - 9);
        const auto comma = inner.find(',');
        if (comma == String::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid DuckLake decimal type '{}', expected decimal(w,s)", type_text);
        const UInt32 precision = static_cast<UInt32>(std::stoul(inner.substr(0, comma)));
        const UInt32 scale = static_cast<UInt32>(std::stoul(inner.substr(comma + 1)));
        if (precision <= 9)
            return std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        if (precision <= 18)
            return std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
        if (precision <= 38)
            return std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "DuckLake decimal type '{}' has precision > 38 which is not supported by DuckLake writers",
            type_text);
    }

    if (type == "timetz" || type == "interval" || type == "variant" || type == "geometry" || type == "unknown")
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "DuckLake column type '{}' is not supported by the ClickHouse DuckLake integration",
            type_text);

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown DuckLake column type '{}'", type_text);
}

bool isNestedType(const String & type_text)
{
    const String type = Poco::toLower(type_text);
    return type == "struct" || type == "list" || type == "map";
}

std::vector<ColumnNode> buildColumnTree(const std::vector<ColumnInfo> & rows, Int64 snapshot_id)
{
    /// Index-based assembly: children index lists per parent position, then recursive materialization.
    std::vector<size_t> positions; /// positions[i] = index into `rows`
    std::unordered_map<Int64, size_t> pos_by_id;
    for (size_t i = 0; i < rows.size(); ++i)
    {
        if (!rows[i].isVisibleAt(snapshot_id))
            continue;
        pos_by_id.emplace(rows[i].column_id, positions.size());
        positions.push_back(i);
    }

    std::vector<std::vector<size_t>> children_of(positions.size());
    std::vector<size_t> root_positions;
    for (size_t pos = 0; pos < positions.size(); ++pos)
    {
        const auto & row = rows[positions[pos]];
        if (!row.parent_column.has_value())
        {
            root_positions.push_back(pos);
            continue;
        }
        auto it = pos_by_id.find(*row.parent_column);
        if (it == pos_by_id.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake column '{}' (id {}) references parent column {} which is not visible at snapshot {}",
                row.name,
                row.column_id,
                *row.parent_column,
                snapshot_id);
        children_of[it->second].push_back(pos);
    }

    const auto by_order = [&](size_t lhs, size_t rhs)
    {
        return rows[positions[lhs]].column_order < rows[positions[rhs]].column_order;
    };
    for (auto & children : children_of)
        std::sort(children.begin(), children.end(), by_order);
    std::sort(root_positions.begin(), root_positions.end(), by_order);

    std::function<ColumnNode(size_t)> materialize = [&](size_t pos) -> ColumnNode
    {
        ColumnNode node;
        node.info = rows[positions[pos]];
        node.children.reserve(children_of[pos].size());
        for (size_t child_pos : children_of[pos])
            node.children.push_back(materialize(child_pos));
        return node;
    };

    std::vector<ColumnNode> roots;
    roots.reserve(root_positions.size());
    for (size_t pos : root_positions)
        roots.push_back(materialize(pos));
    return roots;
}

DataTypePtr getColumnType(const ColumnNode & node)
{
    const String type = Poco::toLower(node.info.type);

    /// Complex types are never wrapped in Nullable: the Parquet reader represents optional
    /// groups as plain Tuple/Array/Map (only scalar leaves become Nullable), and the requested
    /// type must match that convention.
    if (type == "struct")
    {
        Names element_names;
        DataTypes element_types;
        element_names.reserve(node.children.size());
        element_types.reserve(node.children.size());
        for (const auto & child : node.children)
        {
            element_names.push_back(child.info.name);
            element_types.push_back(getColumnType(child));
        }
        return std::make_shared<DataTypeTuple>(element_types, element_names);
    }
    if (type == "list")
    {
        if (node.children.size() != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake list column '{}' (id {}) has {} children instead of 1",
                node.info.name,
                node.info.column_id,
                node.children.size());
        return std::make_shared<DataTypeArray>(getColumnType(node.children[0]));
    }
    if (type == "map")
    {
        if (node.children.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DuckLake map column '{}' (id {}) has {} children instead of 2",
                node.info.name,
                node.info.column_id,
                node.children.size());
        DataTypePtr key_type = getColumnType(node.children[0]);
        DataTypePtr value_type = getColumnType(node.children[1]);
        /// ClickHouse Map keys cannot be Nullable; DuckDB never writes null map keys.
        key_type = removeNullable(key_type);
        return std::make_shared<DataTypeMap>(key_type, value_type);
    }

    DataTypePtr data_type = parseScalarType(node.info.type);
    if (node.info.nulls_allowed && data_type->canBeInsideNullable())
        return std::make_shared<DataTypeNullable>(data_type);
    return data_type;
}

NamesAndTypesList getTableSchema(const std::vector<ColumnNode> & roots)
{
    NamesAndTypesList schema;
    for (const auto & root : roots)
        schema.emplace_back(root.info.name, getColumnType(root));
    return schema;
}

namespace
{

/// Synthetic name for column ids that are not visible at the pinned snapshot (dropped, or a
/// struct/list/map subtree of a dropped column). The name can never collide with a real
/// DuckLake column and is never requested, so the reader skips the physical column.
String inactiveColumnName(Int64 column_id)
{
    return fmt::format("__ducklake_inactive_column_{}", column_id);
}

/// Pick the representative row per column_id: the row visible at the snapshot if there is one,
/// otherwise the most recent historical row. Then rebuild parent links over the representatives.
struct RepresentativeTree
{
    std::vector<const ColumnInfo *> roots;
    std::unordered_map<Int64, std::vector<const ColumnInfo *>> children_by_parent;
};

RepresentativeTree buildRepresentativeTree(const std::vector<ColumnInfo> & all_rows, Int64 snapshot_id)
{
    std::unordered_map<Int64, const ColumnInfo *> repr;
    for (const auto & row : all_rows)
    {
        auto it = repr.find(row.column_id);
        if (it == repr.end())
        {
            repr.emplace(row.column_id, &row);
            continue;
        }
        const bool row_visible = row.isVisibleAt(snapshot_id);
        const bool current_visible = it->second->isVisibleAt(snapshot_id);
        if (row_visible && !current_visible)
            it->second = &row;
        else if (row_visible == current_visible && row.begin_snapshot > it->second->begin_snapshot)
            it->second = &row;
    }

    RepresentativeTree tree;
    for (const auto & [column_id, row] : repr)
    {
        if (row->parent_column.has_value())
            tree.children_by_parent[*row->parent_column].push_back(row);
        else
            tree.roots.push_back(row);
    }
    const auto by_order = [](const ColumnInfo * lhs, const ColumnInfo * rhs)
    {
        return lhs->column_order < rhs->column_order;
    };
    for (auto & [_, children] : tree.children_by_parent)
        std::sort(children.begin(), children.end(), by_order);
    std::sort(tree.roots.begin(), tree.roots.end(), by_order);
    return tree;
}

void appendFieldIdsRecursive(
    const ColumnInfo * node,
    const RepresentativeTree & tree,
    Int64 snapshot_id,
    const String & path,
    bool parent_inactive,
    std::unordered_map<String, Int64> & out)
{
    auto it = tree.children_by_parent.find(node->column_id);
    if (it == tree.children_by_parent.end())
        return;
    for (const ColumnInfo * child : it->second)
    {
        /// Under an inactive (synthetic) root the whole subtree keeps its real names: the
        /// synthetic root already guarantees no collision with live columns.
        const bool child_visible = child->isVisibleAt(snapshot_id);
        const String child_component = child_visible || parent_inactive ? child->name : inactiveColumnName(child->column_id);
        const String child_path = path.empty() ? child_component : path + "." + child_component;
        out[child_path] = child->column_id;
        appendFieldIdsRecursive(child, tree, snapshot_id, child_path, !child_visible, out);
    }
}

}

std::unordered_map<String, Int64> buildFieldIdMap(const std::vector<ColumnInfo> & all_rows, Int64 snapshot_id)
{
    RepresentativeTree tree = buildRepresentativeTree(all_rows, snapshot_id);

    std::unordered_map<String, Int64> result;
    for (const ColumnInfo * root : tree.roots)
    {
        const bool root_visible = root->isVisibleAt(snapshot_id);
        const String component = root_visible ? root->name : inactiveColumnName(root->column_id);
        result[component] = root->column_id;
        appendFieldIdsRecursive(root, tree, snapshot_id, component, !root_visible, result);
    }
    return result;
}

}
}
