#include <Analyzer/Passes/MaterializedColumnSubstitutionPass.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Analyzer/TableNode.h>
#include <Common/FieldVisitorHash.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageSnapshot.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool join_use_nulls;
    extern const SettingsBool optimize_map_materialized_columns;
}

namespace
{

struct MapKeyPair
{
    String column_name;
    Field key;

    bool operator==(const MapKeyPair & other) const = default;
};

struct MapKeyPairHash
{
    size_t operator()(const MapKeyPair & pair) const
    {
        SipHash hash;
        hash.update(pair.column_name.size());
        hash.update(pair.column_name);
        FieldVisitorHash field_hash(hash);
        applyVisitor(field_hash, pair.key);
        return hash.get64();
    }
};

struct TableColKey
{
    const TableNode * table = nullptr;
    String col_name;

    bool operator==(const TableColKey & other) const = default;
};

struct TableColKeyHash
{
    size_t operator()(const TableColKey & key) const
    {
        SipHash hash;
        hash.update(key.table);
        hash.update(key.col_name.size());
        hash.update(key.col_name);
        return hash.get64();
    }
};

struct MaterializedCandidate
{
    NameAndTypePair col;
    bool is_in_primary_key = false;
};

struct MaterializedTableData
{
    UnorderedMapWithMemoryTracking<String, DataTypePtr> key_types;
    UnorderedMapWithMemoryTracking<MapKeyPair, VectorWithMemoryTracking<MaterializedCandidate>, MapKeyPairHash> by_key;
};

Field normalizeMapKeyField(const Field & value, const IDataType & key_type)
{
    auto unwrapped = removeLowCardinality(key_type.getPtr());
    Field converted = tryConvertFieldToType(value, *unwrapped, nullptr /*from_type_hint*/, {} /*format_settings*/, true /*strict*/);
    /// Canonicalize -0.0 to +0.0 so FieldVisitorHash matches Field::operator==.
    if (converted.getType() == Field::Types::Float64 && converted.safeGet<Float64>() == 0.0)
        converted = Field(0.0);
    return converted;
}

bool isConstArrayElementFirstArg(const IQueryTreeNode * parent, const IQueryTreeNode * child)
{
    const auto * fn = parent ? parent->as<FunctionNode>() : nullptr;
    if (!fn || !fn->isResolved() || !fn->isOrdinaryCall() || fn->getFunctionName() != "arrayElement")
        return false;
    const auto & args = fn->getArguments().getNodes();
    if (args.size() != 2 || args[0].get() != child)
        return false;
    const auto * key_const = args[1]->as<ConstantNode>();
    return key_const && !key_const->isNull();
}

void collectColumnRefsOutsideConstArrayElement(
    const QueryTreeNodePtr & node,
    const IQueryTreeNode * parent,
    UnorderedSetWithMemoryTracking<TableColKey, TableColKeyHash> & out)
{
    if (!node || node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
        return;

    if (const auto * col_node = node->as<ColumnNode>())
    {
        if (!col_node->hasExpression())
        {
            auto src = col_node->getColumnSourceOrNull();
            if (const auto * table_node = src ? src->as<TableNode>() : nullptr)
            {
                if (!isConstArrayElementFirstArg(parent, col_node))
                    out.insert(TableColKey{table_node, col_node->getColumnName()});
            }
            return;
        }
    }

    const IQueryTreeNode * next_parent = (node->getNodeType() == QueryTreeNodeType::LIST) ? parent : node.get();
    for (const auto & child : node->getChildren())
    {
        if (child)
            collectColumnRefsOutsideConstArrayElement(child, next_parent, out);
    }
}

const IQueryTreeNode * getEnclosingNonListParent(const VectorWithMemoryTracking<const IQueryTreeNode *> & parent_stack)
{
    for (auto it = parent_stack.rbegin(); it != parent_stack.rend(); ++it)
    {
        if ((*it)->getNodeType() != QueryTreeNodeType::LIST)
            return *it;
    }
    return nullptr;
}

class MaterializedPrecheckVisitor : public InDepthQueryTreeVisitorWithContext<MaterializedPrecheckVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<MaterializedPrecheckVisitor>;
    using Base::Base;

    UnorderedSetWithMemoryTracking<const TableNode *> skip_tables;
    UnorderedSetWithMemoryTracking<const QueryNode *> skip_query_nodes;
    UnorderedSetWithMemoryTracking<TableColKey, TableColKeyHash> forbidden_cols;
    UnorderedSetWithMemoryTracking<TableColKey, TableColKeyHash> whole_map_read_cols;

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr &)
    {
        return parent->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (const auto * qn = node->as<QueryNode>())
        {
            join_depth_stack.push_back(0);
            if (getSettings()[Setting::group_by_use_nulls]
                && (qn->isGroupByWithRollup() || qn->isGroupByWithCube() || qn->isGroupByWithGroupingSets()))
                skip_query_nodes.insert(qn);

            for (const auto & gb_node : qn->getGroupBy().getNodes())
                collectColumnRefsOutsideConstArrayElement(gb_node, nullptr, forbidden_cols);

            for (const auto & corr_node : qn->getCorrelatedColumns().getNodes())
                collectColumnRefsOutsideConstArrayElement(corr_node, nullptr, forbidden_cols);
        }
        else if (node->getNodeType() == QueryTreeNodeType::JOIN)
        {
            if (!join_depth_stack.empty())
                ++join_depth_stack.back();
        }
        else if (const auto * tn = node->as<TableNode>())
        {
            if (getSettings()[Setting::join_use_nulls] && !join_depth_stack.empty() && join_depth_stack.back() > 0)
                skip_tables.insert(tn);
        }
        else if (const auto * col_node = node->as<ColumnNode>())
        {
            if (!col_node->hasExpression() && isMap(col_node->getColumnType()))
            {
                auto src = col_node->getColumnSourceOrNull();
                if (const auto * table_node = src ? src->as<TableNode>() : nullptr)
                {
                    const auto * parent = getEnclosingNonListParent(parent_stack);
                    if (!isConstArrayElementFirstArg(parent, col_node))
                        whole_map_read_cols.insert(TableColKey{table_node, col_node->getColumnName()});
                }
            }
        }

        parent_stack.push_back(node.get());
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!parent_stack.empty())
            parent_stack.pop_back();

        if (node->getNodeType() == QueryTreeNodeType::JOIN)
        {
            if (!join_depth_stack.empty() && join_depth_stack.back() > 0)
                --join_depth_stack.back();
        }
        else if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            if (!join_depth_stack.empty())
                join_depth_stack.pop_back();
        }
    }

private:
    VectorWithMemoryTracking<size_t> join_depth_stack;
    VectorWithMemoryTracking<const IQueryTreeNode *> parent_stack;
};

MaterializedTableData buildMaterializedMapForTable(const TableNode & table_node)
{
    MaterializedTableData result;
    const auto & storage = table_node.getStorage();
    if (!storage || storage->isRemote() || storage->getName() == "Alias")
        return result;

    if (table_node.hasTableExpressionModifiers() && table_node.getTableExpressionModifiers()->hasFinal())
        return result;

    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(storage.get());
    if (!merge_tree || merge_tree->merging_params.mode != MergeTreeData::MergingParams::Ordinary)
        return result;

    const auto & metadata = table_node.getStorageSnapshot()->metadata;
    if (!metadata)
        return result;

    UnorderedSetWithMemoryTracking<String> pk_columns;
    for (const auto & pk_col : metadata->getPrimaryKey().column_names)
        pk_columns.insert(pk_col);

    const auto & columns = metadata->getColumns();
    for (const auto & col : columns)
    {
        /// A column with independent TTL can expire before the source Map column and diverge.
        if (col.ttl)
            continue;

        const auto & default_desc = col.default_desc;
        if (default_desc.kind != ColumnDefaultKind::Materialized || !default_desc.expression)
            continue;

        const auto * fn = default_desc.expression->as<ASTFunction>();
        if (!fn || fn->name != "arrayElement" || !fn->arguments || fn->arguments->children.size() != 2)
            continue;

        const auto * base_id = fn->arguments->children[0]->as<ASTIdentifier>();
        const auto * key_lit = fn->arguments->children[1]->as<ASTLiteral>();
        if (!base_id || !base_id->isShort() || !key_lit || key_lit->value.isNull())
            continue;

        const String & base_col_name = base_id->shortName();
        if (!columns.hasPhysical(base_col_name))
            continue;

        const auto base_col_type = columns.getPhysical(base_col_name).type;
        if (!isMap(base_col_type))
            continue;

        const auto & map_type = assert_cast<const DataTypeMap &>(*base_col_type);
        const auto & value_type = map_type.getValueType();
        if (!col.type->equals(*value_type))
            continue;

        Field normalized_key = normalizeMapKeyField(key_lit->value, *map_type.getKeyType());
        if (normalized_key.isNull())
            continue;

        result.key_types.try_emplace(base_col_name, map_type.getKeyType());
        result.by_key[MapKeyPair{base_col_name, std::move(normalized_key)}].push_back(
            MaterializedCandidate{NameAndTypePair{col.name, col.type}, pk_columns.contains(col.name)});
    }

    for (auto & [key_pair, candidates] : result.by_key)
    {
        std::stable_sort(
            candidates.begin(),
            candidates.end(),
            [](const MaterializedCandidate & lhs, const MaterializedCandidate & rhs)
            {
                return lhs.is_in_primary_key && !rhs.is_in_primary_key;
            });
    }

    return result;
}

class MaterializedColumnSubstitutionVisitor : public InDepthQueryTreeVisitorWithContext<MaterializedColumnSubstitutionVisitor>
{
public:
    MaterializedColumnSubstitutionVisitor(ContextPtr context_, MaterializedPrecheckVisitor precheck_)
        : InDepthQueryTreeVisitorWithContext(std::move(context_))
        , precheck(std::move(precheck_))
    {}

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr &)
    {
        return parent->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        parent_stack.push_back(node.get());

        if (const auto * qn = node->as<QueryNode>())
            query_node_stack.push_back(qn);

        if (const auto * col_node = node->as<ColumnNode>(); col_node && col_node->hasExpression())
        {
            auto src = col_node->getColumnSourceOrNull();
            if (src && src->getNodeType() == QueryTreeNodeType::TABLE)
                ++alias_depth;
        }
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!parent_stack.empty())
            parent_stack.pop_back();

        if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            if (!query_node_stack.empty())
                query_node_stack.pop_back();
        }

        if (const auto * col_node = node->as<ColumnNode>(); col_node && col_node->hasExpression())
        {
            auto src = col_node->getColumnSourceOrNull();
            if (src && src->getNodeType() == QueryTreeNodeType::TABLE && alias_depth > 0)
                --alias_depth;
            return;
        }

        if (!getSettings()[Setting::optimize_map_materialized_columns])
            return;

        if (!query_node_stack.empty() && precheck.skip_query_nodes.contains(query_node_stack.back()))
            return;

        const auto * fn = node->as<FunctionNode>();
        if (!fn || !fn->isResolved() || !fn->isOrdinaryCall() || fn->getFunctionName() != "arrayElement")
            return;

        const auto & args = fn->getArguments().getNodes();
        if (args.size() != 2)
            return;

        const auto * map_col = args[0]->as<ColumnNode>();
        const auto * key_const = args[1]->as<ConstantNode>();
        if (!map_col || map_col->hasExpression() || !key_const || key_const->isNull())
            return;

        auto column_source = map_col->getColumnSourceOrNull();
        const auto * table_node = column_source ? column_source->as<TableNode>() : nullptr;
        if (!table_node || precheck.skip_tables.contains(table_node))
            return;

        TableColKey map_col_key{table_node, map_col->getColumnName()};
        if (precheck.forbidden_cols.contains(map_col_key))
            return;

        auto it = table_maps.find(table_node);
        if (it == table_maps.end())
            it = table_maps.emplace(table_node, buildMaterializedMapForTable(*table_node)).first;

        const auto & table_data = it->second;
        auto key_type_it = table_data.key_types.find(map_col->getColumnName());
        if (key_type_it == table_data.key_types.end() || !key_type_it->second)
            return;

        Field normalized_key = normalizeMapKeyField(key_const->getValue(), *key_type_it->second);
        if (normalized_key.isNull())
            return;

        auto mat_it = table_data.by_key.find(MapKeyPair{map_col->getColumnName(), std::move(normalized_key)});
        if (mat_it == table_data.by_key.end())
            return;

        const bool whole_map_also_read = precheck.whole_map_read_cols.contains(map_col_key);
        /// If replacing the direct root expression of an ordinary `ALIAS` column turns it into a
        /// trivial rename (`alias -> __mat_col`), `FunctionToSubcolumnsPass::resolveTrivialAliasChain`
        /// would unwrap it and check `SELECT` on `__mat_col` (e.g. `empty(svc)` -> `__mat_service.size`).
        /// Require `SELECT` on the target column in that case (as well as when outside an ALIAS).
        const bool is_direct_alias_root = !parent_stack.empty() && parent_stack.back()->getNodeType() == QueryTreeNodeType::COLUMN;
        const bool skip_rbac_check = (alias_depth > 0 && !is_direct_alias_root);
        const auto & storage_id = table_node->getStorageID();

        const MaterializedCandidate * chosen = nullptr;
        for (const auto & candidate : mat_it->second)
        {
            if (whole_map_also_read && !candidate.is_in_primary_key)
                continue;
            if (!candidate.col.type->equals(*fn->getResultType()))
                continue;
            if (!skip_rbac_check
                && !getContext()->getAccess()->isGranted(
                    AccessType::SELECT, storage_id.getDatabaseName(), storage_id.getTableName(), candidate.col.name))
                continue;
            chosen = &candidate;
            break;
        }

        if (!chosen)
            return;

        auto mat_node = std::make_shared<ColumnNode>(chosen->col, column_source);
        if (fn->hasAlias())
            mat_node->setAlias(fn->getAlias());
        node = std::move(mat_node);
    }

private:
    MaterializedPrecheckVisitor precheck;
    size_t alias_depth = 0;
    VectorWithMemoryTracking<const IQueryTreeNode *> parent_stack;
    VectorWithMemoryTracking<const QueryNode *> query_node_stack;
    UnorderedMapWithMemoryTracking<const TableNode *, MaterializedTableData> table_maps;
};

}

void MaterializedColumnSubstitutionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::optimize_map_materialized_columns]
        || context->canUseParallelReplicasOnInitiator()
        || context->canUseOffsetParallelReplicas()
        || context->canUseParallelReplicasCustomKey())
        return;

    MaterializedPrecheckVisitor precheck(context);
    precheck.visit(query_tree_node);

    MaterializedColumnSubstitutionVisitor visitor(std::move(context), std::move(precheck));
    visitor.visit(query_tree_node);
}

}
