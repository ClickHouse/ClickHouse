#include <Analyzer/Passes/IvfClusterPruningPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Core/Settings.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/BlockIO.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexVectorSimilarityIvf.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageProxy.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 ivf_nprobe;
    extern const SettingsFloat ivf_epsilon;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

namespace
{

/// True if the subtree of node references a column of the table expression (with column_name, only that column).
bool referencesTable(const QueryTreeNodePtr & node, const ITableExpressionNode * table_expression, const String * column_name = nullptr)
{
    if (const auto * column_node = node->as<ColumnNode>())
    {
        if (column_node->getColumnSourceOrNull().get() == table_expression && (!column_name || column_node->getColumnName() == *column_name))
            return true;
    }

    for (const auto & child : node->getChildren())
    {
        if (child && referencesTable(child, table_expression, column_name))
            return true;
    }

    return false;
}

/// True if every function in the subtree of node returns the same result within a query (unlike rand, for example).
bool isDeterministicInScopeOfQuery(const QueryTreeNodePtr & node)
{
    if (const auto * function_node = node->as<FunctionNode>())
    {
        if (!function_node->isOrdinaryFunction() || !function_node->getFunctionOrThrow()->isDeterministicInScopeOfQuery())
            return false;
    }

    for (const auto & child : node->getChildren())
    {
        if (child && !isDeterministicInScopeOfQuery(child))
            return false;
    }

    return true;
}

std::optional<NameAndTypePair> findColumn(const NamesAndTypesList & columns, const String & name)
{
    for (const auto & column : columns)
    {
        if (column.name == name)
            return column;
    }
    return {};
}

class IvfClusterPruningVisitor : public InDepthQueryTreeVisitorWithContext<IvfClusterPruningVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<IvfClusterPruningVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        /// Must be ORDER BY ... LIMIT k without WITH TIES.
        if (!query_node->hasOrderBy() || !query_node->hasLimit() || query_node->isLimitWithTies())
            return;

        /// FROM must be a single physical table.
        auto & join_tree = query_node->getJoinTreeNode();
        auto * table_node = join_tree->as<TableNode>();
        if (!table_node)
            return;

        const auto & storage = table_node->getStorage();
        if (!castStorage<MergeTreeData>(storage, DeferredTable::Load))
            return;

        auto context = getContext();
        const auto & query_settings = context->getSettingsRef();
        const UInt64 nprobe = query_settings[Setting::ivf_nprobe];
        const Float64 epsilon = static_cast<Float64>(query_settings[Setting::ivf_epsilon].value);
        if (nprobe == 0)
            return;

        /// The table must have an IVF index; CREATE has validated that cid is in the primary key, so cid IN (...) prunes.
        auto data_metadata = table_node->getStorageSnapshot()->metadata;
        const IndexDescription * ivf_index = nullptr;
        for (const auto & index : data_metadata->getSecondaryIndices())
        {
            if (isVectorSimilarityIvfIndex(index))
            {
                ivf_index = &index;
                break;
            }
        }
        if (!ivf_index)
            return;

        const String & vec_column_name = ivf_index->column_names[0];
        const String & cid_column_name = ivf_index->column_names[1];
        const String metric_name = parseVectorSimilarityIvfIndexArguments(ivf_index->arguments).distance_function;

        /// First ORDER BY expression must be metric(vec_column, q) ascending.
        const auto & order_by_nodes = query_node->getOrderBy().getNodes();
        if (order_by_nodes.empty())
            return;

        const auto * sort_node = order_by_nodes.front()->as<SortNode>();
        if (!sort_node || sort_node->getSortDirection() != SortDirection::ASCENDING)
            return;

        const auto * dist_function = sort_node->getExpression()->as<FunctionNode>();
        if (!dist_function || dist_function->getFunctionName() != metric_name)
            return;

        const auto & dist_arguments = dist_function->getArguments().getNodes();
        if (dist_arguments.size() != 2)
            return;

        /// arg0 = the data table's vector column; arg1 = the query vector. The query vector is copied into the centroids
        /// subquery, so it must not depend on the table, and must be deterministic: otherwise the clusters would be chosen for
        /// another vector than the one of the ORDER BY.
        const auto * vec_column = dist_arguments[0]->as<ColumnNode>();
        if (!vec_column || vec_column->getColumnName() != vec_column_name)
            return;

        auto data_table_expression = query_node->getJoinTreeNodeTyped();
        QueryTreeNodePtr query_vector = dist_arguments[1];
        if (referencesTable(query_vector, data_table_expression.get()) || !isDeterministicInScopeOfQuery(query_vector))
            return;

        /// A filter on the cluster id column already chooses the clusters; intersecting it with the nearest clusters could
        /// remove the rows it asks for.
        if ((query_node->hasWhere() && referencesTable(query_node->getWhere(), data_table_expression.get(), &cid_column_name))
            || (query_node->hasPrewhere() && referencesTable(query_node->getPrewhere(), data_table_expression.get(), &cid_column_name)))
            return;

        /// Resolve the data table's cid column (for the outer IN predicate).
        const auto data_columns = data_metadata->getColumns().getAllPhysical();
        auto data_cid = findColumn(data_columns, cid_column_name);
        if (!data_cid)
            return;

        /// The centroids are in the hidden table owned by the index.
        const StorageID centroids_id = getVectorSimilarityIvfCentroidsTableID(table_node->getStorageID(), ivf_index->name);
        auto centroids_storage = DatabaseCatalog::instance().tryGetTable(centroids_id, context);
        if (!centroids_storage)
            throw Exception(ErrorCodes::UNKNOWN_TABLE,
                "Centroids table {} of IVF vector similarity index {} does not exist", centroids_id.getNameForLogs(), ivf_index->name);

        /// Until the centroids are trained there are no clusters to prune by, and every row must be read.
        const auto centroids_rows = centroids_storage->totalRows(context);
        if (!centroids_rows || *centroids_rows == 0)
            return;

        auto centroids_table_node = std::make_shared<TableNode>(centroids_storage, context);
        auto centroids_columns = centroids_table_node->getStorageSnapshot()->metadata->getColumns().getAllPhysical();

        /// Structure of the centroids table: see getVectorSimilarityIvfCentroidsTableID.
        auto centroid_cid = findColumn(centroids_columns, "cid");
        auto centroid_vec = findColumn(centroids_columns, "vec");
        auto centroid_level = findColumn(centroids_columns, "level");
        chassert(centroid_cid && centroid_vec && centroid_level);

        /// Only the leaves (level 0) are clusters of the table. Upper levels are not used for search yet.
        auto make_leaf_filter = [&](const std::shared_ptr<TableNode> & centroids_table_expression)
        {
            auto level_node = std::make_shared<ColumnNode>(*centroid_level, centroids_table_expression);
            auto zero_node = std::make_shared<ConstantNode>(Field(UInt64(0)), std::make_shared<DataTypeUInt8>());
            auto equals_function = std::make_shared<FunctionNode>("equals");
            equals_function->getArguments().getNodes() = {level_node, zero_node};
            resolveOrdinaryFunctionNodeByName(*equals_function, "equals", context);
            return equals_function;
        };

        /// Build: SELECT cid FROM centroids [WHERE metric(vec, q) <= (1 + epsilon) * (SELECT min(...))]
        ///        ORDER BY metric(vec, q) ASC LIMIT nprobe
        auto centroid_cid_node = std::make_shared<ColumnNode>(*centroid_cid, centroids_table_node);
        auto centroid_vec_node = std::make_shared<ColumnNode>(*centroid_vec, centroids_table_node);

        auto sub_dist_function = std::make_shared<FunctionNode>(metric_name);
        sub_dist_function->getArguments().getNodes() = {centroid_vec_node, query_vector->clone()};
        resolveOrdinaryFunctionNodeByName(*sub_dist_function, metric_name, context);

        auto sub_sort_node = std::make_shared<SortNode>(sub_dist_function, SortDirection::ASCENDING);

        auto subquery_node = std::make_shared<QueryNode>(Context::createCopy(context));
        subquery_node->setIsSubquery(true);
        subquery_node->getJoinTreeNode() = centroids_table_node;
        subquery_node->getProjection().getNodes().push_back(centroid_cid_node);
        subquery_node->resolveProjectionColumns({*centroid_cid});
        subquery_node->getOrderBy().getNodes().push_back(sub_sort_node);
        subquery_node->getLimit() = std::make_shared<ConstantNode>(Field(nprobe), std::make_shared<DataTypeUInt64>());
        subquery_node->getWhere() = make_leaf_filter(centroids_table_node);

        /// With epsilon > 0 keep only centroids within (1 + epsilon) * nearest distance, capped by nprobe. The nearest distance comes
        /// from a comma join with SELECT min(...): a scalar subquery would not be analyzed, since passes run after analysis.
        if (epsilon > 0)
        {
            /// The comma join scans the centroids twice; distinct aliases keep the planner's
            /// column identifiers ("<alias>.vec") from colliding across the two scans.
            centroids_table_node->setAlias("__ivf_centroids");

            /// Derived table: SELECT min(metric(vec, q)) AS md FROM centroids
            auto min_centroids_table_node = std::make_shared<TableNode>(centroids_storage, context);
            min_centroids_table_node->setAlias("__ivf_centroids_min");
            auto min_vec_node = std::make_shared<ColumnNode>(*centroid_vec, min_centroids_table_node);

            auto min_dist_function = std::make_shared<FunctionNode>(metric_name);
            min_dist_function->getArguments().getNodes() = {min_vec_node, query_vector->clone()};
            resolveOrdinaryFunctionNodeByName(*min_dist_function, metric_name, context);

            auto min_function = std::make_shared<FunctionNode>("min");
            min_function->getArguments().getNodes() = {min_dist_function};
            resolveAggregateFunctionNodeByName(*min_function, "min");

            NameAndTypePair md_column{"md", min_function->getResultType()};
            /// The aliases are needed since the subquery is executed from its SQL text.
            min_function->setAlias("md");
            auto min_derived_query = std::make_shared<QueryNode>(Context::createCopy(context));
            min_derived_query->setAlias("__ivf_nearest");
            min_derived_query->setIsSubquery(true);
            min_derived_query->getJoinTreeNode() = min_centroids_table_node;
            min_derived_query->getWhere() = make_leaf_filter(min_centroids_table_node);
            min_derived_query->getProjection().getNodes().push_back(min_function);
            min_derived_query->resolveProjectionColumns({md_column});

            /// centroids, (SELECT min(...) AS md FROM centroids)
            auto cross_join_node = std::make_shared<CrossJoinNode>(centroids_table_node);
            cross_join_node->appendTable(min_derived_query, CrossJoinNode::JoinType{.is_comma = true});
            subquery_node->getJoinTreeNode() = cross_join_node;

            /// metric(vec, q) <= (1 + epsilon) * md
            auto md_column_node = std::make_shared<ColumnNode>(md_column, min_derived_query);
            auto factor_node = std::make_shared<ConstantNode>(Field(1.0 + epsilon), std::make_shared<DataTypeFloat64>());
            auto multiply_function = std::make_shared<FunctionNode>("multiply");
            multiply_function->getArguments().getNodes() = {factor_node, md_column_node};
            resolveOrdinaryFunctionNodeByName(*multiply_function, "multiply", context);

            auto threshold_function = std::make_shared<FunctionNode>("lessOrEquals");
            threshold_function->getArguments().getNodes() = {sub_dist_function->clone(), multiply_function};
            resolveOrdinaryFunctionNodeByName(*threshold_function, "lessOrEquals", context);

            auto and_function = std::make_shared<FunctionNode>("and");
            and_function->getArguments().getNodes() = {make_leaf_filter(centroids_table_node), threshold_function};
            resolveOrdinaryFunctionNodeByName(*and_function, "and", context);
            subquery_node->getWhere() = and_function;
        }

        /// Find the nearest clusters now and filter by them as a constant: the centroids table is hidden and belongs to the
        /// table, so like the inner table of a materialized view it is read without checking the grants of the user on it.
        auto internal_context = Context::createCopy(context->getGlobalContext());
        internal_context->applySettingsChanges(context->getSettingsRef().changes());
        internal_context->makeQueryContext();
        internal_context->setCurrentQueryId({});
        subquery_node->setIsSubquery(false);
        Array cluster_ids;
        {
            auto io = executeQuery(subquery_node->toAST()->formatWithSecretsOneLine(), internal_context, QueryFlags{.internal = true}).second;
            PullingPipelineExecutor executor(io.pipeline);
            Block block;
            while (executor.pull(block))
            {
                const auto & column = block.getByPosition(0).column;
                for (size_t row = 0; row < column->size(); ++row)
                    cluster_ids.push_back(column->getUInt(row));
            }
            io.onFinish();
        }
        if (cluster_ids.empty())
            return;

        /// Build: cid IN [cluster ids]
        auto in_function_name = getInFunctionNameForPassCreatedNode("in", data_cid->type, context);
        if (!in_function_name)
            return;

        auto data_cid_node = std::make_shared<ColumnNode>(*data_cid, data_table_expression);
        auto in_function_node = std::make_shared<FunctionNode>(*in_function_name);
        in_function_node->markAsOperator();
        QueryTreeNodePtr cluster_ids_node = std::make_shared<ConstantNode>(
            Field(std::move(cluster_ids)), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()));
        in_function_node->getArguments().getNodes() = {data_cid_node, cluster_ids_node};
        resolveOrdinaryFunctionNodeByName(*in_function_node, *in_function_name, context);

        /// AND it into the existing WHERE (or set it if there is none).
        QueryTreeNodePtr predicate = in_function_node;
        if (query_node->hasWhere())
        {
            auto and_function_node = std::make_shared<FunctionNode>("and");
            and_function_node->getArguments().getNodes() = {query_node->getWhere(), predicate};
            resolveOrdinaryFunctionNodeByName(*and_function_node, "and", context);
            query_node->getWhere() = and_function_node;
        }
        else
        {
            query_node->getWhere() = predicate;
        }
    }
};

}

void IvfClusterPruningPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    IvfClusterPruningVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
