#include <Storages/MergeTree/MergeTreeIndexUtils.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>

namespace DB
{

ASTPtr buildFilterNode(const ASTPtr & select_query, ASTs additional_filters)
{
    auto & select_query_typed = select_query->as<ASTSelectQuery &>();

    ASTs filters;
    if (select_query_typed.where())
        filters.push_back(select_query_typed.where());

    if (select_query_typed.prewhere())
        filters.push_back(select_query_typed.prewhere());

    filters.insert(filters.end(), additional_filters.begin(), additional_filters.end());

    if (filters.empty())
        return nullptr;

    ASTPtr filter_node;

    if (filters.size() == 1)
    {
        filter_node = filters.front();
    }
    else
    {
        auto function = std::make_shared<ASTFunction>();

        function->name = "and";
        function->arguments = std::make_shared<ASTExpressionList>();
        function->children.push_back(function->arguments);
        function->arguments->children = std::move(filters);

        filter_node = std::move(function);
    }

    return filter_node;
}

namespace
{

bool isTrivialCast(const ActionsDAG::Node & node)
{
    if (node.function_base->getName() != "CAST" || node.children.size() != 2 || node.children[1]->type != ActionsDAG::ActionType::COLUMN)
        return false;

    const auto * column_const = typeid_cast<const ColumnConst *>(node.children[1]->column.get());
    if (!column_const)
        return false;

    Field field = column_const->getField();
    if (field.getType() != Field::Types::String)
        return false;

    auto type_name = field.safeGet<String>();
    return node.children[0]->result_type->getName() == type_name;
}

const ActionsDAG::Node & cloneFilterDAGNodeForIndexesAnalysis(
    const ActionsDAG::Node & node,
    ActionsDAG & new_dag,
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> & cloned_nodes)
{
    auto it = cloned_nodes.find(&node);
    if (it != cloned_nodes.end())
        return *it->second;

    const ActionsDAG::Node * res;
    switch (node.type)
    {
        case (ActionsDAG::ActionType::INPUT):
        {
            res = &new_dag.addInput({node.column, node.result_type, node.result_name});
            break;
        }
        case (ActionsDAG::ActionType::COLUMN):
        {
            String name;
            if (const auto * column_const = typeid_cast<const ColumnConst *>(node.column.get());
                column_const && column_const->getDataType() != TypeIndex::Function)
            {
                /// Re-generate column name for constant.
                name = ASTLiteral(column_const->getField()).getColumnName();
            }
            else
                name = node.result_name;

            res = &new_dag.addColumn({node.column, node.result_type, name});
            break;
        }
        case (ActionsDAG::ActionType::ALIAS):
        {
            auto arg = cloneFilterDAGNodeForIndexesAnalysis(*node.children.front(), new_dag, cloned_nodes);
            res = &new_dag.addAlias(arg, node.result_name);
            break;
        }
        case (ActionsDAG::ActionType::ARRAY_JOIN):
        {
            auto arg = cloneFilterDAGNodeForIndexesAnalysis(*node.children.front(), new_dag, cloned_nodes);
            res = &new_dag.addArrayJoin(arg, {});
            break;
        }
        case (ActionsDAG::ActionType::FUNCTION):
        {
            if (isTrivialCast(node))
            {
                /// Remove trivial cast and keep its first argument.
                res = &cloneFilterDAGNodeForIndexesAnalysis(*node.children[0], new_dag, cloned_nodes);
            }
            else
            {
                ActionsDAG::NodeRawConstPtrs children(node.children);
                for (auto & arg : children)
                    arg = &cloneFilterDAGNodeForIndexesAnalysis(*arg, new_dag, cloned_nodes);
                res = &new_dag.addFunction(node.function_base, children, "");
            }
            break;
        }
    }

    cloned_nodes[&node] = res;
    return *res;
}

}

ActionsDAG cloneFilterDAGForIndexesAnalysis(const ActionsDAG & dag)
{
    ActionsDAG new_dag;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> cloned_nodes;
    ActionsDAG::NodeRawConstPtrs outputs = dag.getOutputs();
    for (auto & node : outputs)
        node = &cloneFilterDAGNodeForIndexesAnalysis(*node, new_dag, cloned_nodes);

    new_dag.getOutputs().swap(outputs);
    return new_dag;
}

}
