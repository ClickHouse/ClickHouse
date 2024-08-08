#include <algorithm>
#include <memory>
#include <stack>
#include <Core/NamesAndTypes.h>
#include <Core/TypeId.h>

#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/misc.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeDateTime.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/VirtualColumnUtils.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include "Functions/FunctionsLogical.h"
#include "Functions/IFunction.h"
#include "Functions/IFunctionAdaptors.h"
#include "Functions/indexHint.h"
#include <Parsers/makeASTForLogicalFunction.h>
#include <Columns/ColumnSet.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ActionsVisitor.h>


namespace DB
{

namespace VirtualColumnUtils
{

void buildSetsForDAG(const ActionsDAG & dag, const ContextPtr & context)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::COLUMN)
        {
            const ColumnSet * column_set = checkAndGetColumnConstData<const ColumnSet>(node.column.get());
            if (!column_set)
                column_set = checkAndGetColumn<const ColumnSet>(node.column.get());

            if (column_set)
            {
                auto future_set = column_set->getData();
                if (!future_set->get())
                {
                    if (auto * set_from_subquery = typeid_cast<FutureSetFromSubquery *>(future_set.get()))
                        set_from_subquery->buildSetInplace(context);
                }
            }
        }
    }
}

ExpressionActionsPtr buildFilterExpression(ActionsDAG dag, ContextPtr context)
{
    buildSetsForDAG(dag, context);
    return std::make_shared<ExpressionActions>(std::move(dag));
}

void filterBlockWithExpression(const ExpressionActionsPtr & actions, Block & block)
{
    Block block_with_filter = block;
    actions->execute(block_with_filter, /*dry_run=*/ false, /*allow_duplicates_in_input=*/ true);

    /// Filter the block.
    String filter_column_name = actions->getActionsDAG().getOutputs().at(0)->result_name;
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column->convertToFullColumnIfConst();

    ConstantFilterDescription constant_filter(*filter_column);

    if (constant_filter.always_true)
    {
        return;
    }

    if (constant_filter.always_false)
    {
        block = block.cloneEmpty();
        return;
    }

    FilterDescription filter(*filter_column);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnPtr & column = block.safeGetByPosition(i).column;
        column = column->filter(*filter.data, -1);
    }
}

NameSet getVirtualNamesForFileLikeStorage()
{
    return {"_path", "_file", "_size", "_time", "_etag"};
}

VirtualColumnsDescription getVirtualsForFileLikeStorage(const ColumnsDescription & storage_columns)
{
    VirtualColumnsDescription desc;

    auto add_virtual = [&](const auto & name, const auto & type)
    {
        if (storage_columns.has(name))
            return;

        desc.addEphemeral(name, type, "");
    };

    add_virtual("_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));
    add_virtual("_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));
    add_virtual("_size", makeNullable(std::make_shared<DataTypeUInt64>()));
    add_virtual("_time", makeNullable(std::make_shared<DataTypeDateTime>()));
    add_virtual("_etag", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));

    return desc;
}

static void addPathAndFileToVirtualColumns(Block & block, const String & path, size_t idx)
{
    if (block.has("_path"))
        block.getByName("_path").column->assumeMutableRef().insert(path);

    if (block.has("_file"))
    {
        auto pos = path.find_last_of('/');
        String file;
        if (pos != std::string::npos)
            file = path.substr(pos + 1);
        else
            file = path;

        block.getByName("_file").column->assumeMutableRef().insert(file);
    }

    block.getByName("_idx").column->assumeMutableRef().insert(idx);
}

std::optional<ActionsDAG> createPathAndFileFilterDAG(const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns)
{
    if (!predicate || virtual_columns.empty())
        return {};

    Block block;
    for (const auto & column : virtual_columns)
    {
        if (column.name == "_file" || column.name == "_path")
            block.insert({column.type->createColumn(), column.type, column.name});
    }

    block.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});
    return splitFilterDagForAllowedInputs(predicate, &block);
}

ColumnPtr getFilterByPathAndFileIndexes(const std::vector<String> & paths, const ExpressionActionsPtr & actions, const NamesAndTypesList & virtual_columns)
{
    Block block;
    for (const auto & column : virtual_columns)
    {
        if (column.name == "_file" || column.name == "_path")
            block.insert({column.type->createColumn(), column.type, column.name});
    }
    block.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_idx"});

    for (size_t i = 0; i != paths.size(); ++i)
        addPathAndFileToVirtualColumns(block, paths[i], i);

    filterBlockWithExpression(actions, block);

    return block.getByName("_idx").column;
}

void addRequestedFileLikeStorageVirtualsToChunk(
    Chunk & chunk, const NamesAndTypesList & requested_virtual_columns,
    VirtualsForFileLikeStorage virtual_values)
{
    for (const auto & virtual_column : requested_virtual_columns)
    {
        if (virtual_column.name == "_path")
        {
            chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), virtual_values.path)->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_file")
        {
            if (virtual_values.filename)
            {
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), (*virtual_values.filename))->convertToFullColumnIfConst());
            }
            else
            {
                size_t last_slash_pos = virtual_values.path.find_last_of('/');
                auto filename_from_path = virtual_values.path.substr(last_slash_pos + 1);
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), filename_from_path)->convertToFullColumnIfConst());
            }
        }
        else if (virtual_column.name == "_size")
        {
            if (virtual_values.size)
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), *virtual_values.size)->convertToFullColumnIfConst());
            else
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_time")
        {
            if (virtual_values.last_modified)
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), virtual_values.last_modified->epochTime())->convertToFullColumnIfConst());
            else
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
        else if (virtual_column.name == "_etag")
        {
            if (virtual_values.etag)
                chunk.addColumn(virtual_column.type->createColumnConst(chunk.getNumRows(), (*virtual_values.etag))->convertToFullColumnIfConst());
            else
                chunk.addColumn(virtual_column.type->createColumnConstWithDefaultValue(chunk.getNumRows())->convertToFullColumnIfConst());
        }
    }
}

static bool canEvaluateSubtree(const ActionsDAG::Node * node, const Block * allowed_inputs)
{
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(node);
    while (!nodes.empty())
    {
        const auto * cur = nodes.top();
        nodes.pop();

        if (cur->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;

        if (cur->type == ActionsDAG::ActionType::INPUT && allowed_inputs && !allowed_inputs->has(cur->result_name))
            return false;

        for (const auto * child : cur->children)
            nodes.push(child);
    }

    return true;
}

bool isDeterministicInScopeOfQuery(const ActionsDAG::Node * node)
{
    for (const auto * child : node->children)
    {
        if (!isDeterministicInScopeOfQuery(child))
            return false;
    }

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    if (!node->function_base->isDeterministicInScopeOfQuery())
        return false;

    return true;
}

static const ActionsDAG::Node * splitFilterNodeForAllowedInputs(
    const ActionsDAG::Node * node,
    const Block * allowed_inputs,
    ActionsDAG::Nodes & additional_nodes)
{
    if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        if (node->function_base->getName() == "and")
        {
            auto & node_copy = additional_nodes.emplace_back(*node);
            node_copy.children.clear();
            for (const auto * child : node->children)
                if (const auto * child_copy = splitFilterNodeForAllowedInputs(child, allowed_inputs, additional_nodes))
                    node_copy.children.push_back(child_copy);

            if (node_copy.children.empty())
                return nullptr;

            if (node_copy.children.size() == 1)
            {
                const ActionsDAG::Node * res = node_copy.children.front();
                /// Expression like (not_allowed AND 256) can't be resuced to (and(256)) because AND requires
                /// at least two arguments; also it can't be reduced to (256) because result type is different.
                if (!res->result_type->equals(*node->result_type))
                {
                    ActionsDAG tmp_dag;
                    res = &tmp_dag.addCast(*res, node->result_type, {});
                    additional_nodes.splice(additional_nodes.end(), ActionsDAG::detachNodes(std::move(tmp_dag)));
                }

                return res;
            }

            return &node_copy;
        }
        else if (node->function_base->getName() == "or")
        {
            auto & node_copy = additional_nodes.emplace_back(*node);
            for (auto & child : node_copy.children)
                if (child = splitFilterNodeForAllowedInputs(child, allowed_inputs, additional_nodes); !child)
                    return nullptr;

            return &node_copy;
        }
        else if (node->function_base->getName() == "indexHint")
        {
            if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node->function_base.get()))
            {
                if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                {
                    auto index_hint_dag = index_hint->getActions().clone();
                    ActionsDAG::NodeRawConstPtrs atoms;
                    for (const auto & output : index_hint_dag.getOutputs())
                        if (const auto * child_copy = splitFilterNodeForAllowedInputs(output, allowed_inputs, additional_nodes))
                            atoms.push_back(child_copy);

                    if (!atoms.empty())
                    {
                        const auto * res = atoms.at(0);

                        if (atoms.size() > 1)
                        {
                            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
                            res = &index_hint_dag.addFunction(func_builder_and, atoms, {});
                        }

                        if (!res->result_type->equals(*node->result_type))
                            res = &index_hint_dag.addCast(*res, node->result_type, {});

                        additional_nodes.splice(additional_nodes.end(), ActionsDAG::detachNodes(std::move(index_hint_dag)));
                        return res;
                    }
                }
            }
        }
        else if (!isDeterministicInScopeOfQuery(node))
        {
            return nullptr;
        }
    }

    if (!canEvaluateSubtree(node, allowed_inputs))
        return nullptr;

    return node;
}

std::optional<ActionsDAG> splitFilterDagForAllowedInputs(const ActionsDAG::Node * predicate, const Block * allowed_inputs)
{
    if (!predicate)
        return {};

    ActionsDAG::Nodes additional_nodes;
    const auto * res = splitFilterNodeForAllowedInputs(predicate, allowed_inputs, additional_nodes);
    if (!res)
        return {};

    return ActionsDAG::cloneSubDAG({res}, true);
}

void filterBlockWithPredicate(const ActionsDAG::Node * predicate, Block & block, ContextPtr context)
{
    auto dag = splitFilterDagForAllowedInputs(predicate, &block);
    if (dag)
        filterBlockWithExpression(buildFilterExpression(std::move(*dag), context), block);
}

}

}
