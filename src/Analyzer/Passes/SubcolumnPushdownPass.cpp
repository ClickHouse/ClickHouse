#include <Analyzer/Passes/SubcolumnPushdownPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class SubcolumnPushdownVisitor : public InDepthQueryTreeVisitorWithContext<SubcolumnPushdownVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<SubcolumnPushdownVisitor>;
    using Base::Base;

    bool & madeChanges() { return made_changes; }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "getSubcolumn")
            return;

        const auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        auto * column_node = args[0]->as<ColumnNode>();
        auto * subcolumn_name_node = args[1]->as<ConstantNode>();

        if (!column_node || !subcolumn_name_node)
            return;

        if (subcolumn_name_node->getValue().getType() != Field::Types::String)
            return;

        auto column_source = column_node->getColumnSource();
        auto * query_source = column_source->as<QueryNode>();
        if (!query_source)
            return;

        const String & base_column_name = column_node->getColumnName();
        const String subcolumn_name = subcolumn_name_node->getValue().safeGet<String>();
        const String full_subcolumn_name = base_column_name + "." + subcolumn_name;

        /// Check if the subcolumn type is valid
        auto subcolumn_type = column_node->getResultType()->tryGetSubcolumnType(subcolumn_name);
        if (!subcolumn_type)
            return;

        /// Find and update the projection in the source query
        auto & projection_nodes = query_source->getProjection().getNodes();

        for (size_t i = 0; i < projection_nodes.size(); ++i)
        {
            auto * proj_column = projection_nodes[i]->as<ColumnNode>();
            if (!proj_column || proj_column->getColumnName() != base_column_name)
                continue;

            auto proj_source = proj_column->getColumnSource();
            auto proj_source_type = proj_source->getNodeType();

            QueryTreeNodePtr new_proj_node;

            if (proj_source_type == QueryTreeNodeType::TABLE || proj_source_type == QueryTreeNodeType::TABLE_FUNCTION)
            {
                /// Direct table access - create subcolumn reference directly
                NameAndTypePair subcolumn_name_and_type{full_subcolumn_name, subcolumn_type};
                new_proj_node = std::make_shared<ColumnNode>(subcolumn_name_and_type, proj_source);
            }
            else if (proj_source_type == QueryTreeNodeType::QUERY || proj_source_type == QueryTreeNodeType::UNION)
            {
                /// Nested subquery - create getSubcolumn call that will be processed in next iteration
                auto get_subcolumn_func = std::make_shared<FunctionNode>("getSubcolumn");
                auto & func_args = get_subcolumn_func->getArguments().getNodes();
                func_args.push_back(proj_column->clone());
                func_args.push_back(std::make_shared<ConstantNode>(subcolumn_name));

                auto func = FunctionFactory::instance().get("getSubcolumn", getContext());
                get_subcolumn_func->resolveAsFunction(func->build(get_subcolumn_func->getArgumentColumns()));
                new_proj_node = get_subcolumn_func;
            }
            else
            {
                continue;
            }

            /// Replace the projection
            projection_nodes[i] = new_proj_node;

            /// Update the projection columns
            auto projection_columns = query_source->getProjectionColumns();
            projection_columns[i] = NameAndTypePair{full_subcolumn_name, subcolumn_type};
            query_source->resolveProjectionColumns(std::move(projection_columns));

            /// Replace getSubcolumn(column, 'subcolumn') with direct column reference
            NameAndTypePair new_column_name_and_type{full_subcolumn_name, subcolumn_type};
            node = std::make_shared<ColumnNode>(new_column_name_and_type, column_source);

            made_changes = true;
            return;
        }
    }

private:
    bool made_changes = false;
};

}

void SubcolumnPushdownPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    /// Run the pass iteratively until no more changes are made (for nested subqueries)
    const size_t max_iterations = 10; /// Safety limit
    for (size_t i = 0; i < max_iterations; ++i)
    {
        SubcolumnPushdownVisitor visitor(context);
        visitor.visit(query_tree_node);
        if (!visitor.madeChanges())
            break;
    }
}

}
