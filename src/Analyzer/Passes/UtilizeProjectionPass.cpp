#include <Analyzer/Passes/UtilizeProjectionPass.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/IdentifierNode.h>

#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

StorageSnapshotPtr getStorageSnapshot(const QueryTreeNodePtr & node)
{
    StorageSnapshotPtr storage_snapshot{nullptr};
    if (auto * table_node = node->as<TableNode>())
        return table_node->getStorageSnapshot();
    else if (auto * table_function_node = node->as<TableFunctionNode>())
        return table_function_node->getStorageSnapshot();
    return nullptr;
}

String getIdentifier(QueryTreeNodePtr & argument)
{
    if (argument->getNodeType() == QueryTreeNodeType::COLUMN)
    {
        auto id = argument->as<ColumnNode>();
        return id->getColumnName();
    }
    else if (argument->getNodeType() == QueryTreeNodeType::CONSTANT)
        return "";
    else
        return getIdentifier(argument->as<FunctionNode>()->getArguments().getNodes().at(0));
}

bool hasKey(String & key, const NamesAndTypes & keys_set)
{
    for (auto & keys : keys_set)
    {
        if (keys.name == key)
        {
            return true;
        }
    }
    return false;
}

class UtilizeProjectionVisitor : public InDepthQueryTreeVisitorWithContext<UtilizeProjectionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<UtilizeProjectionVisitor>;
    using Base::Base;


    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr &)
    {
        return false;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_project_query)
            return;


        if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            auto query_node = node->as<QueryNode>();
            if (!query_node->hasWhere() || !query_node->getJoinTree())
                return;

            const auto metadata = getStorageSnapshot(query_node->getJoinTree())->metadata;
            const auto & projections = metadata->getProjections();

            if (!metadata)
                return;
            const auto primary_keys_col = metadata->getPrimaryKey();
            NamesAndTypes primary_keys;
            for (size_t i = 0; i < primary_keys_col.column_names.size(); i++)
                primary_keys.emplace_back(primary_keys_col.column_names.at(i), primary_keys_col.data_types.at(i));

            // include virtual column "_part" into select query
            auto part_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
            primary_keys.emplace_back("_part", part_type);

            auto new_node = pkOptimization(projections, node, primary_keys);
            query_node->getWhere() = std::move(new_node);
        }

    }

    QueryTreeNodePtr pkOptimization(
        const ProjectionsDescription & projections,
        const QueryTreeNodePtr & query_node,
        NamesAndTypes & primary_keys)
    {
        auto where_predicate = query_node->as<QueryNode>()->getWhere();
        NameSet proj_pks = {};
        for (const auto & projection: projections)
        {
            if (projection.type == ProjectionDescription::Type::Normal)
            {
                //sorting key of projection
                const auto & projection_primary_key_name = projection.metadata->getSortingKey().column_names.at(0);

                proj_pks.insert(projection_primary_key_name);
                auto projection_columns = projection.getRequiredColumns();

                // projection columns needs to include projection primary key and main table primary key
                // in order to use this optimization
                bool proj_col_include_ppk = std::find(projection_columns.begin(), projection_columns.end(), projection_primary_key_name) != projection_columns.end();
                bool proj_col_include_mpk = std::find(projection_columns.begin(), projection_columns.end(), primary_keys[0].name) != projection_columns.end();

                if (!proj_col_include_ppk || !proj_col_include_mpk)
                {
                    return where_predicate;
                }
            }
        }

        if (proj_pks.size() < 1)
            return where_predicate;

        auto primary_key_predicates = std::make_shared<ListNode>();
        return analyze_where_predicate(query_node, where_predicate, primary_key_predicates->getNodes(), proj_pks, primary_keys);
    }

    QueryTreeNodePtr analyze_where_predicate(
        const QueryTreeNodePtr & original_query,
        QueryTreeNodePtr & node,
        QueryTreeNodes & primary_key_predicates,
        const NameSet & proj_pks,
        const NamesAndTypes & primary_keys)
    {
        bool contains_pk = false;
        if (auto * function_node = node->as<FunctionNode>())
        {
            auto arg_size = function_node->getArguments().getNodes().size();

            // Simple predicate looks like (key = '123')
            if ((function_node->getFunctionName() == "equals" || function_node->getFunctionName() == "notEquals") && arg_size == 2)
            {
                auto lhs_argument = function_node->getArguments().getNodes().at(0);
                auto rhs_argument = function_node->getArguments().getNodes().at(1);
                String lhs = getIdentifier(lhs_argument);
                String rhs = getIdentifier(rhs_argument);
                auto col_name = (!lhs.empty()) ? lhs:rhs;
                contains_pk = hasKey(col_name, primary_keys);
                if (proj_pks.contains(col_name) && !contains_pk)
                {
                    QueryTreeNodePtr rewrite_ast;
                    rewrite_ast = create_proj_optimized_node(original_query, node, primary_key_predicates, primary_keys);
                    auto and_func = std::make_shared<FunctionNode>("and");
                    and_func->getArguments().getNodes().push_back(std::move(rewrite_ast));
                    and_func->getArguments().getNodes().push_back(node);
                    resolveFunction("and", and_func);

                    return and_func;
                }
            }
            // "IN" predicates, such as (key in ('A', 'B', 'C'))
            else if (function_node->getFunctionName() == "in" || function_node->getFunctionName() == "notIn")
            {
                QueryTreeNodePtr rewrite_ast;
                auto subquery = function_node->getArguments().getNodes().at(1)->as<QueryNode>();

                // (in <subquery>) is currently not supported
                if (!subquery)
                {
                    auto lhs_argument = function_node->getArguments().getNodes().at(0);
                    contains_pk = false;
                    bool proj_pks_contains = false;
                    if (auto func = lhs_argument->as<FunctionNode>())
                    {
                        if (func->getFunctionName() == "tuple")
                        {
                            for (auto tuple_arg : func->getArguments().getNodes())
                            {
                                String col_name = getIdentifier(tuple_arg);
                                contains_pk = hasKey(col_name, primary_keys);
                                if (proj_pks.contains(col_name))
                                    proj_pks_contains = true;
                            }
                        }
                    }
                    else
                    {
                        String col_name = getIdentifier(lhs_argument);
                        contains_pk = hasKey(col_name, primary_keys);
                        if (proj_pks.contains(col_name))
                            proj_pks_contains = proj_pks.contains(col_name);
                    }

                    if (proj_pks_contains && !contains_pk)
                    {
                        rewrite_ast = create_proj_optimized_node(original_query, node, primary_key_predicates, primary_keys);
                        auto and_func = std::make_shared<FunctionNode>("and");
                        and_func->getArguments().getNodes().push_back(std::move(rewrite_ast));
                        and_func->getArguments().getNodes().push_back(node);

                        resolveFunction("and", and_func);
                        return and_func;
                    }
                }
            }
            // Handle <AND>, recursively analyze each child predicate
            else if (function_node->getFunctionName() == "and")
            {
                // loop through where predicates
                findPrimaryKeyPredicates(node, primary_key_predicates, primary_keys);
                auto current_func = std::make_shared<FunctionNode>(function_node->getFunctionName());
                for (size_t i = 0; i < arg_size; i++)
                {
                    auto argument = function_node->getArguments().getNodes().at(i);
                    auto new_ast = analyze_where_predicate(original_query, argument, primary_key_predicates, proj_pks, primary_keys);
                    current_func->getArguments().getNodes().push_back(std::move(new_ast));
                }
                primary_key_predicates.clear();
                resolveFunction(function_node->getFunctionName(), current_func);
                return current_func;
            }
            // Handle <OR>, recursively analyze each child predicate
            // Differences between <OR> and <AND> is that under <AND> logic, parallel primary key predicate
            // also needs to be push down to indexHint() conditions
            // while <OR> does not need to do that
            else if (function_node->getFunctionName() == "or")
            {
                auto current_func = std::make_shared<FunctionNode>(function_node->getFunctionName());
                for (size_t i = 0; i < arg_size; i++)
                {
                    auto argument = function_node->getArguments().getNodes().at(i);
                    auto new_ast = analyze_where_predicate(original_query, argument, primary_key_predicates, proj_pks, primary_keys);
                    current_func->getArguments().getNodes().push_back(std::move(new_ast));
                }
                resolveFunction(function_node->getFunctionName(), current_func);
                return current_func;
            }
        }
        return node;
    }

    /**
     * @brief Manually rewrite the WHERE query, Insert a new where condition in order to
     * leverage projection features
     *
     * Storage is not empty while calling this function
     * For example, a qualified table with projection
     * CREATE TABLE test_a(`src` String,`dst` String, `other_cols` String,
     * PROJECTION p1(SELECT src, dst ORDER BY dst)) ENGINE = MergeTree ORDER BY src;
     *
     * A qualified SELECT query would looks like this
     * select * from test_a where dst='-42';
     * The where key is the projection table primary key.
     * The following code will convert this select query to the following
     * select * from test_a where indexHint(src in (select src from test_a where dst='-42')) and dst='-42';
     */
    QueryTreeNodePtr create_proj_optimized_node(const QueryTreeNodePtr & node, const QueryTreeNodePtr & where_predicate, QueryTreeNodes & primary_key_predicates, const NamesAndTypes & primary_keys)
    {
        auto * where_predicate_node = where_predicate->as<FunctionNode>();
        auto * original_query_node = node->as<QueryNode>();
        auto * table_node = original_query_node->getJoinTree()->as<IdentifierNode>();
        auto * table_func_node = original_query_node->getJoinTree()->as<TableNode>();

        if (!original_query_node || !where_predicate_node || (!table_node && !table_func_node))
            return where_predicate;

        ContextPtr current_context = original_query_node->getContext();

        auto subquery = std::make_shared<QueryNode>(Context::createCopy(current_context));
        subquery->setIsSubquery(true);

        if (primary_key_predicates.size() >= 1)
        {
            auto new_where_predicates = std::make_shared<FunctionNode>("and");
            for (auto predicate : primary_key_predicates)
                new_where_predicates->getArguments().getNodes().push_back(predicate->clone());
            new_where_predicates->getArguments().getNodes().push_back(where_predicate->clone());
            resolveFunction("and", new_where_predicates);
            subquery->getWhere() = std::move(new_where_predicates);
        }
        else
        {
            subquery->getWhere() = where_predicate->clone();
        }
        subquery->getJoinTree() = original_query_node->getJoinTree();

        auto in_function = std::make_shared<FunctionNode>("in");
        auto column_nodes = std::make_shared<ListNode>();
        column_nodes->getNodes().reserve(primary_keys.size());

        if (primary_keys.size() == 1)
        {
            column_nodes->getNodes().push_back(std::make_shared<ColumnNode>(primary_keys.at(0), original_query_node->getJoinTree()));
            in_function->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(primary_keys.at(0), original_query_node->getJoinTree()));
        }
        else
        {
            auto tuples = std::make_shared<FunctionNode>("tuple");
            for (size_t i = 0; i < primary_keys.size(); i++)
            {
                auto column = primary_keys.at(i);
                tuples->getArguments().getNodes().push_back(std::make_shared<ColumnNode>(column, original_query_node->getJoinTree()));
                column_nodes->getNodes().push_back(std::make_shared<ColumnNode>(column, original_query_node->getJoinTree()));
            }
            resolveFunction("tuple", tuples);
            in_function->getArguments().getNodes().push_back(tuples);
        }
        subquery->getProjectionNode() = std::move(column_nodes);
        subquery->resolveProjectionColumns(primary_keys);
        in_function->getArguments().getNodes().push_back(std::move(subquery));
        resolveFunction("in", in_function);

        auto indexFunc = std::make_shared<FunctionNode>("indexHint");
        indexFunc->getArguments().getNodes().push_back(std::move(in_function));
        resolveFunction("indexHint", indexFunc);

        return indexFunc;
    }

    void findPrimaryKeyPredicates(QueryTreeNodePtr where_predicate, QueryTreeNodes & primary_key_predicates, const NamesAndTypes & primary_keys)
    {
        auto func = where_predicate->as<FunctionNode>();
        if (!func)
            return;


        const static std::unordered_set<String> supported_predicates_relations = {
            "equals",
            "notEquals",
            "less",
            "greater",
            "lessOrEquals",
            "greaterOrEquals",
        };

        auto arg_size = func->getArguments().getNodes().size();

        if (supported_predicates_relations.contains(func->getFunctionName()) && arg_size == 2)
        {
            auto lhs_argument = func->getArguments().getNodes().at(0);
            auto rhs_argument = func->getArguments().getNodes().at(1);
            String lhs = getIdentifier(lhs_argument);
            String rhs = getIdentifier(rhs_argument);
            auto col_name = (!lhs.empty()) ? lhs:rhs;
            bool contains_pk = hasKey(col_name, primary_keys);
            if (contains_pk)
            {
                primary_key_predicates.push_back(func->clone());
            }
        }
        else if (func->getFunctionName() == "and")
        {
            for (size_t i = 0; i < arg_size; i++)
            {
                findPrimaryKeyPredicates(func->getArguments().getNodes().at(i), primary_key_predicates, primary_keys);
            }
        }
    }

    void resolveFunction(String func_name, QueryTreeNodePtr func_ptr)
    {
        auto func = func_ptr->as<FunctionNode>();
        auto function_base = FunctionFactory::instance().get(func_name, getContext())->build(func->getArgumentColumns());
        func->resolveAsFunction(std::move(function_base));
    }
};

void UtilizeProjectionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    UtilizeProjectionVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
