#include <Storages/MergeTree/RPNBuilder.h>

#include <Common/FieldVisitorToString.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Core/Settings.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>

#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>

#include <Functions/indexHint.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsMiscellaneous.h>

#include <Interpreters/Context.h>

#include <IO/WriteHelpers.h>

#include <Storages/KeyDescription.h>

#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilterText.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void appendColumnNameWithoutAlias(const ActionsDAG::Node & node, WriteBuffer & out, const ContextPtr & context, bool use_analyzer, bool legacy = false)
{
    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
            writeString(node.result_name, out);
            break;
        case ActionsDAG::ActionType::COLUMN:
        {
            /// If it was created from ASTLiteral, then result_name can be an alias.
            /// We need to convert value back to string here.
            const auto * column_const = typeid_cast<const ColumnConst *>(node.column.get());
            if (column_const && !use_analyzer)
                writeString(applyVisitor(FieldVisitorToString(), column_const->getField()), out);
            else
                writeString(node.result_name, out);
            break;
        }
        case ActionsDAG::ActionType::ALIAS:
            appendColumnNameWithoutAlias(*node.children.front(), out, context, use_analyzer, legacy);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            writeCString("arrayJoin(", out);
            appendColumnNameWithoutAlias(*node.children.front(), out, context, use_analyzer, legacy);
            writeChar(')', out);
            break;
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (const auto * func_capture = typeid_cast<const ExecutableFunctionCapture *>(node.function.get()))
            {
                const auto & capture = func_capture->getCapture();
                auto capture_dag = func_capture->getActions()->getActionsDAG().clone();
                if (!node.children.empty())
                {
                    auto captured_columns_dag = ActionsDAG::cloneSubDAG(node.children, false);
                    auto & outputs = captured_columns_dag.getOutputs();
                    for (size_t i = 0; i < capture->captured_names.size(); ++i)
                        outputs[i] = &captured_columns_dag.addAlias(*outputs[i], capture->captured_names[i]);

                    capture_dag = ActionsDAG::merge(std::move(captured_columns_dag), std::move(capture_dag));
                }

                writeString("lambda(tuple(", out);
                bool first = true;
                for (const auto & arg : capture->lambda_arguments)
                {
                    if (!first)
                        writeCString(", ", out);
                    first = false;

                    writeString(arg.name, out);
                }
                writeString("), ", out);

                ActionsDAGWithInversionPushDown inverted_capture_dag(capture_dag.getOutputs().at(0), context);
                appendColumnNameWithoutAlias(*inverted_capture_dag.predicate, out, context, use_analyzer, legacy);
                writeChar(')', out);
                break;
            }
            else
            {
                auto name = node.function_base->getName();
                if (legacy && name == "modulo")
                    writeCString("moduloLegacy", out);
                else
                    writeString(name, out);

                writeChar('(', out);
            }

            bool first = true;
            for (const auto * arg : node.children)
            {
                if (!first)
                    writeCString(", ", out);
                first = false;

                appendColumnNameWithoutAlias(*arg, out, context, use_analyzer, legacy);
            }
            writeChar(')', out);
            break;
        }
        case ActionsDAG::ActionType::PLACEHOLDER:
            writeString(node.result_name, out);
            break;
    }
}

String getColumnNameWithoutAlias(const ActionsDAG::Node & node, const ContextPtr & context, bool use_analyzer, bool legacy = false)
{
    WriteBufferFromOwnString out;
    appendColumnNameWithoutAlias(node, out, context, use_analyzer, legacy);

    return std::move(out.str());
}

const ActionsDAG::Node * getNodeWithoutAlias(const ActionsDAG::Node * node)
{
    const ActionsDAG::Node * result = node;

    while (result->type == ActionsDAG::ActionType::ALIAS)
        result = result->children[0];

    return result;
}

}

RPNBuilderTreeContext::RPNBuilderTreeContext(ContextPtr query_context_)
    : query_context(std::move(query_context_))
{}

RPNBuilderTreeContext::RPNBuilderTreeContext(ContextPtr query_context_, Block block_with_constants_, PreparedSetsPtr prepared_sets_)
    : query_context(std::move(query_context_))
    , block_with_constants(std::move(block_with_constants_))
    , prepared_sets(std::move(prepared_sets_))
{}

const Settings & RPNBuilderTreeContext::getSettings() const
{
    return query_context->getSettingsRef();
}

RPNBuilderTreeNode::RPNBuilderTreeNode(const ActionsDAG::Node * dag_node_, RPNBuilderTreeContext & tree_context_)
    : dag_node(dag_node_)
    , tree_context(tree_context_)
{
    assert(dag_node);
}

RPNBuilderTreeNode::RPNBuilderTreeNode(const IAST * ast_node_, RPNBuilderTreeContext & tree_context_)
    : ast_node(ast_node_)
    , tree_context(tree_context_)
{
    assert(ast_node);
}

std::string RPNBuilderTreeNode::getColumnName() const
{
    if (ast_node)
        return ast_node->getColumnNameWithoutAlias();

    return getColumnNameWithoutAlias(*dag_node, getTreeContext().getQueryContext(), getTreeContext().getSettings()[Setting::allow_experimental_analyzer]);
}

std::string RPNBuilderTreeNode::getColumnNameWithModuloLegacy() const
{
    if (ast_node)
    {
        auto adjusted_ast = ast_node->clone();
        KeyDescription::moduloToModuloLegacyRecursive(adjusted_ast);
        return adjusted_ast->getColumnNameWithoutAlias();
    }

    return getColumnNameWithoutAlias(*dag_node, getTreeContext().getQueryContext(), getTreeContext().getSettings()[Setting::allow_experimental_analyzer], true /*legacy*/);
}

bool RPNBuilderTreeNode::isFunction() const
{
    if (ast_node)
    {
        return typeid_cast<const ASTFunction *>(ast_node);
    }

    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return node_without_alias->type == ActionsDAG::ActionType::FUNCTION;
}

bool RPNBuilderTreeNode::isConstant() const
{
    if (ast_node)
    {
        bool is_literal = typeid_cast<const ASTLiteral *>(ast_node);
        if (is_literal)
            return true;

        String column_name = ast_node->getColumnName();
        const auto & block_with_constants = tree_context.getBlockWithConstants();

        if (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column))
            return true;

        return false;
    }

    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return node_without_alias->column && isColumnConst(*node_without_alias->column);
}

bool RPNBuilderTreeNode::isSubqueryOrSet() const
{
    if (ast_node)
    {
        return
            typeid_cast<const ASTSubquery *>(ast_node) ||
            typeid_cast<const ASTTableIdentifier *>(ast_node);
    }

    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return node_without_alias->result_type->getTypeId() == TypeIndex::Set;
}

ColumnWithTypeAndName RPNBuilderTreeNode::getConstantColumn() const
{
    if (!isConstant())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RPNBuilderTree node is not a constant");

    ColumnWithTypeAndName result;

    if (ast_node)
    {
        const auto * literal = typeid_cast<const ASTLiteral *>(ast_node);
        if (literal)
        {
            result.type = applyVisitor(FieldToDataType(), literal->value);
            result.column = result.type->createColumnConst(0, literal->value);

            return result;
        }

        String column_name = ast_node->getColumnName();
        const auto & block_with_constants = tree_context.getBlockWithConstants();

        return block_with_constants.getByName(column_name);
    }

    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    result.type = node_without_alias->result_type;
    result.column = node_without_alias->column;

    return result;
}

bool RPNBuilderTreeNode::tryGetConstant(Field & output_value, DataTypePtr & output_type) const
{
    if (ast_node)
    {
        // Constant expr should use alias names if any
        String column_name = ast_node->getColumnName();
        const auto & block_with_constants = tree_context.getBlockWithConstants();

        if (const auto * literal = ast_node->as<ASTLiteral>())
        {
            /// By default block_with_constants has only one column named "_dummy".
            /// If block contains only constants it's may not be preprocessed by
            //  ExpressionAnalyzer, so try to look up in the default column.
            if (!block_with_constants.has(column_name))
                column_name = "_dummy";

            /// Simple literal
            output_value = literal->value;
            output_type = block_with_constants.getByName(column_name).type;

            /// If constant is not Null, we can assume it's type is not Nullable as well.
            if (!output_value.isNull())
                output_type = removeNullable(output_type);

            return true;
        }
        if (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column))
        {
            /// An expression which is dependent on constants only
            const auto & constant_column = block_with_constants.getByName(column_name);
            output_value = (*constant_column.column)[0];
            output_type = constant_column.type;

            if (!output_value.isNull())
                output_type = removeNullable(output_type);

            return true;
        }
    }
    else
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);

        if (node_without_alias->column && isColumnConst(*node_without_alias->column))
        {
            output_value = (*node_without_alias->column)[0];
            output_type = node_without_alias->result_type;

            if (!output_value.isNull())
                output_type = removeNullable(output_type);

            return true;
        }
    }

    return false;
}

namespace
{

FutureSetPtr tryGetSetFromDAGNode(const ActionsDAG::Node * dag_node)
{
    if (!dag_node->column)
        return {};

    const IColumn * column = dag_node->column.get();
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
        column = &column_const->getDataColumn();

    if (const auto * column_set = typeid_cast<const ColumnSet *>(column))
        return column_set->getData();

    return {};
}

}

FutureSetPtr RPNBuilderTreeNode::tryGetPreparedSet() const
{
    const auto & prepared_sets = getTreeContext().getPreparedSets();

    if (ast_node && prepared_sets)
    {
        auto key = ast_node->getTreeHash(/*ignore_aliases=*/ true);
        const auto & sets = prepared_sets->getSetsFromTuple();
        auto it = sets.find(key);
        if (it != sets.end() && !it->second.empty())
            return it->second.at(0);

        return prepared_sets->findSubquery(key);
    }
    if (dag_node)
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        return tryGetSetFromDAGNode(node_without_alias);
    }

    return {};
}

FutureSetPtr RPNBuilderTreeNode::tryGetPreparedSet(const DataTypes & data_types) const
{
    const auto & prepared_sets = getTreeContext().getPreparedSets();

    if (prepared_sets && ast_node)
    {
        if (ast_node->as<ASTSubquery>() || ast_node->as<ASTTableIdentifier>())
            return prepared_sets->findSubquery(ast_node->getTreeHash(/*ignore_aliases=*/ true));

        return prepared_sets->findTuple(ast_node->getTreeHash(/*ignore_aliases=*/ true), data_types);
    }
    if (dag_node)
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        return tryGetSetFromDAGNode(node_without_alias);
    }

    return nullptr;
}

RPNBuilderFunctionTreeNode RPNBuilderTreeNode::toFunctionNode() const
{
    if (!isFunction())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RPNBuilderTree node is not a function");

    if (ast_node)
        return RPNBuilderFunctionTreeNode(ast_node, tree_context);
    return RPNBuilderFunctionTreeNode(getNodeWithoutAlias(dag_node), tree_context);
}

std::optional<RPNBuilderFunctionTreeNode> RPNBuilderTreeNode::toFunctionNodeOrNull() const
{
    if (!isFunction())
        return {};

    if (ast_node)
        return RPNBuilderFunctionTreeNode(this->ast_node, tree_context);
    return RPNBuilderFunctionTreeNode(getNodeWithoutAlias(dag_node), tree_context);
}

std::string RPNBuilderFunctionTreeNode::getFunctionName() const
{
    if (ast_node)
        return assert_cast<const ASTFunction *>(ast_node)->name;
    return dag_node->function_base->getName();
}

FunctionBasePtr RPNBuilderFunctionTreeNode::getFunctionBase() const
{
    return ast_node ? nullptr : dag_node->function_base;
}

size_t RPNBuilderFunctionTreeNode::getArgumentsSize() const
{
    if (ast_node)
    {
        const auto * ast_function = assert_cast<const ASTFunction *>(ast_node);
        return ast_function->arguments ? ast_function->arguments->children.size() : 0;
    }

    // indexHint arguments are stored inside of `FunctionIndexHint` class,
    // because they are used only for index analysis.
    if (dag_node->function_base->getName() == "indexHint")
    {
        const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(dag_node->function_base.get());
        const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get());
        return index_hint->getActions().getOutputs().size();
    }

    return dag_node->children.size();
}

RPNBuilderTreeNode RPNBuilderFunctionTreeNode::getArgumentAt(size_t index) const
{
    const size_t total_arguments = getArgumentsSize();
    if (index >= total_arguments) /// Bug #52632
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                "RPNBuilderFunctionTreeNode has {} arguments, attempted to get argument at index {}",
                total_arguments, index);

    if (ast_node)
    {
        const auto * ast_function = assert_cast<const ASTFunction *>(ast_node);
        return RPNBuilderTreeNode(ast_function->arguments->children[index].get(), tree_context);
    }

    // indexHint arguments are stored inside of `FunctionIndexHint` class,
    // because they are used only for index analysis.
    if (dag_node->function_base->getName() == "indexHint")
    {
        const auto & adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor &>(*dag_node->function_base);
        const auto & index_hint = typeid_cast<const FunctionIndexHint &>(*adaptor.getFunction());
        return RPNBuilderTreeNode(index_hint.getActions().getOutputs()[index], tree_context);
    }

    return RPNBuilderTreeNode(dag_node->children[index], tree_context);
}

template <typename RPNElement>
RPNBuilder<RPNElement>::RPNBuilder(
    const ActionsDAG::Node * filter_actions_dag_node,
    ContextPtr query_context_,
    const ExtractAtomFromTreeFunction & extract_atom_from_tree_function_)
    : extract_atom_from_tree_function(extract_atom_from_tree_function_)
{
    RPNBuilderTreeContext tree_context(query_context_);
    traverseTree(RPNBuilderTreeNode(filter_actions_dag_node, tree_context));
}

template <typename RPNElement>
RPNBuilder<RPNElement>::RPNBuilder(const RPNBuilderTreeNode & node, const ExtractAtomFromTreeFunction & extract_atom_from_tree_function_)
    : extract_atom_from_tree_function(extract_atom_from_tree_function_)
{
    traverseTree(node);
}

template <typename RPNElement>
RPNBuilder<RPNElement>::RPNElements && RPNBuilder<RPNElement>::extractRPN() &&
{
    return std::move(rpn_elements);
}

template <typename RPNElement>
void RPNBuilder<RPNElement>::traverseTree(const RPNBuilderTreeNode & node)
{
    RPNElement element;

    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();

        if (extractLogicalOperatorFromTree(function_node, element))
        {
            size_t arguments_size = function_node.getArgumentsSize();

            for (size_t argument_index = 0; argument_index < arguments_size; ++argument_index)
            {
                auto function_node_argument = function_node.getArgumentAt(argument_index);
                traverseTree(function_node_argument);

                /** The first part of the condition is for the correct support of `and` and `or` functions of arbitrary arity
                      * - in this case `n - 1` elements are added (where `n` is the number of arguments).
                      */
                if (argument_index != 0 || element.function == RPNElement::FUNCTION_NOT)
                    rpn_elements.emplace_back(std::move(element)); /// NOLINT(bugprone-use-after-move,hicpp-invalid-access-moved)
            }

            if (arguments_size == 0 && function_node.getFunctionName() == "indexHint")
            {
                element.function = RPNElement::ALWAYS_TRUE;
                rpn_elements.emplace_back(std::move(element));
            }

            return;
        }
    }

    if (!extract_atom_from_tree_function(node, element))
        element.function = RPNElement::FUNCTION_UNKNOWN;

    rpn_elements.emplace_back(std::move(element));
}

template <typename RPNElement>
bool RPNBuilder<RPNElement>::extractLogicalOperatorFromTree(const RPNBuilderFunctionTreeNode & function_node, RPNElement & out)
{
    /** Functions AND, OR, NOT.
          * Also a special function `indexHint` - works as if instead of calling a function there are just parentheses
          * (or, the same thing - calling the function `and` from one argument).
          */

    auto function_name = function_node.getFunctionName();
    if (function_name == "not")
    {
        if (function_node.getArgumentsSize() != 1)
            return false;

        out.function = RPNElement::FUNCTION_NOT;
    }
    else
    {
        if (function_name == "and" || function_name == "indexHint")
            out.function = RPNElement::FUNCTION_AND;
        else if (function_name == "or")
            out.function = RPNElement::FUNCTION_OR;
        else
            return false;
    }

    return true;
}

template class RPNBuilder<KeyCondition::RPNElement>;
template class RPNBuilder<ConditionSelectivityEstimator::RPNElement>;
template class RPNBuilder<MergeTreeConditionBloomFilterText::RPNElement>;
template class RPNBuilder<MergeTreeIndexConditionBloomFilter::RPNElement>;
template class RPNBuilder<MergeTreeIndexConditionText::RPNElement>;
}
