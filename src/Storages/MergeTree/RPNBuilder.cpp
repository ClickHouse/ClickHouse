#include <Storages/MergeTree/RPNBuilder.h>

#include <Storages/MergeTree/KeyCondition.h>

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

#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilterText.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void appendColumnNameWithoutAlias(const ActionsDAG::Node & node, WriteBuffer & out, const ContextPtr & context, bool legacy = false);

/// Produces the lambda's column name in the AST format `lambda(tuple(args), body)`.
/// Used both for live FUNCTION nodes wrapping `ExecutableFunctionCapture`/`FunctionCapture` and for
/// constant-folded COLUMN nodes that hold a `ColumnConst<ColumnFunction>` (e.g. when all captured
/// arguments are constants and `removeUnusedActions` collapsed the FUNCTION into a COLUMN).
void appendLambdaColumnName(
    const LambdaCapture & capture,
    ActionsDAG capture_dag,
    WriteBuffer & out,
    const ContextPtr & context,
    bool legacy)
{
    writeString("lambda(tuple(", out);
    bool first = true;
    for (const auto & arg : capture.lambda_arguments)
    {
        if (!first)
            writeCString(", ", out);
        first = false;

        writeString(arg.name, out);
    }
    writeString("), ", out);

    /// The lambda body is a value expression whose reconstructed name must match the original
    /// expression exactly, so truthiness-only rewrites must not apply.
    ActionsDAGWithInversionPushDown inverted_capture_dag(capture_dag.getOutputs().at(0), context, /* boolean_context */ false);
    appendColumnNameWithoutAlias(*inverted_capture_dag.predicate, out, context, legacy);
    writeChar(')', out);
}

/// For a constant-folded lambda (`ColumnConst` wrapping `ColumnFunction`), reconstruct the lambda
/// AST-format name. Returns true on success and writes the name to `out`.
bool tryAppendConstantFunctionColumnName(
    const ActionsDAG::Node & node,
    WriteBuffer & out,
    const ContextPtr & context,
    bool legacy)
{
    const auto * column_const = node.column.get();
    if (!column_const)
        return false;

    const auto * column_function = typeid_cast<const ColumnFunction *>(&column_const->getDataColumn());
    if (!column_function)
        return false;

    const auto * function_expression = typeid_cast<const FunctionExpression *>(column_function->getFunction().get());
    if (!function_expression)
        return false;

    const auto & capture = function_expression->getCapture();
    auto capture_dag = function_expression->getAcionsDAG().clone();

    /// Stitch the captured constant columns into the body DAG so the body's input nodes
    /// are resolved to actual constants. After `ActionsDAGWithInversionPushDown` rewrites
    /// the constant column names to their AST form, the resulting name will match the one
    /// produced for the index sample block (which was built via the old analyzer).
    const auto & captured_columns = column_function->getCapturedColumns();
    if (!captured_columns.empty())
    {
        if (captured_columns.size() != capture.captured_names.size())
            return false;

        ActionsDAG captured_columns_dag;
        auto & outputs = captured_columns_dag.getOutputs();
        outputs.reserve(captured_columns.size());
        for (size_t i = 0; i < captured_columns.size(); ++i)
        {
            const auto & captured = captured_columns[i];
            auto captured_column_const = assert_cast<const ColumnConst &>(*captured.column).getPtr();
            const auto & captured_node = captured_columns_dag.addColumn(std::move(captured_column_const), captured.type, captured.name);
            const auto & alias_node = captured_columns_dag.addAlias(captured_node, capture.captured_names[i]);
            outputs.push_back(&alias_node);
        }

        capture_dag = ActionsDAG::merge(std::move(captured_columns_dag), std::move(capture_dag));
    }

    appendLambdaColumnName(capture, std::move(capture_dag), out, context, legacy);
    return true;
}

void appendColumnNameWithoutAlias(const ActionsDAG::Node & node, WriteBuffer & out, const ContextPtr & context, bool legacy)
{
    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
            writeString(node.result_name, out);
            break;
        case ActionsDAG::ActionType::COLUMN:
        {
            /// A constant-folded lambda is a `ColumnConst` of a `ColumnFunction`. Recover the
            /// `lambda(tuple(args), body)` AST form so the name aligns with what the index sample
            /// block produced for the same expression (the index goes through the old analyzer).
            if (tryAppendConstantFunctionColumnName(node, out, context, legacy))
                break;

            writeString(node.result_name, out);
            break;
        }
        case ActionsDAG::ActionType::ALIAS:
            appendColumnNameWithoutAlias(*node.children.front(), out, context, legacy);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            writeCString("arrayJoin(", out);
            appendColumnNameWithoutAlias(*node.children.front(), out, context, legacy);
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

                appendLambdaColumnName(*capture, std::move(capture_dag), out, context, legacy);
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

                appendColumnNameWithoutAlias(*arg, out, context, legacy);
            }
            writeChar(')', out);
            break;
        }
        case ActionsDAG::ActionType::PLACEHOLDER:
            writeString(node.result_name, out);
            break;
    }
}

String getColumnNameWithoutAlias(const ActionsDAG::Node & node, const ContextPtr & context, bool legacy = false)
{
    WriteBufferFromOwnString out;
    appendColumnNameWithoutAlias(node, out, context, legacy);

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

RPNBuilderTreeNode::RPNBuilderTreeNode(const ActionsDAG::Node * dag_node_, const ContextPtr & query_context_)
    : WithContext(query_context_)
    , dag_node(dag_node_)
{
    chassert(dag_node);
}

std::string RPNBuilderTreeNode::getColumnName() const
{
    return getColumnNameWithoutAlias(*dag_node, getContext());
}

std::string RPNBuilderTreeNode::getColumnNameWithModuloLegacy() const
{
    return getColumnNameWithoutAlias(*dag_node, getContext(), true /*legacy*/);
}

bool RPNBuilderTreeNode::isFunction() const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return node_without_alias->type == ActionsDAG::ActionType::FUNCTION;
}

bool RPNBuilderTreeNode::isConstant() const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return node_without_alias->column != nullptr;
}

bool RPNBuilderTreeNode::isNullable() const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return node_without_alias->result_type && node_without_alias->result_type->isNullable();
}

bool RPNBuilderTreeNode::isSubqueryOrSet() const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return node_without_alias->result_type->getTypeId() == TypeIndex::Set;
}

ColumnWithTypeAndName RPNBuilderTreeNode::getConstantColumn() const
{
    if (!isConstant())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RPNBuilderTree node is not a constant");

    const auto * node_without_alias = getNodeWithoutAlias(dag_node);

    ColumnWithTypeAndName result;
    result.type = node_without_alias->result_type;
    result.column = node_without_alias->column;

    return result;
}

bool RPNBuilderTreeNode::tryGetConstant(Field & output_value, DataTypePtr & output_type) const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    if (!node_without_alias->column)
        return false;

    output_value = node_without_alias->column->getField();
    output_type = node_without_alias->result_type;

    /// If constant is not Null, we can assume it's type is not Nullable as well.
    if (!output_value.isNull())
        output_type = removeNullable(output_type);

    return true;
}

namespace
{

FutureSetPtr tryGetSetFromDAGNode(const ActionsDAG::Node * dag_node)
{
    if (!dag_node->column)
        return {};

    if (const auto * column_set = typeid_cast<const ColumnSet *>(&dag_node->column->getDataColumn()))
        return column_set->getData();

    return {};
}

}

FutureSetPtr RPNBuilderTreeNode::tryGetPreparedSet() const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    return tryGetSetFromDAGNode(node_without_alias);
}

RPNBuilderFunctionTreeNode RPNBuilderTreeNode::toFunctionNode() const
{
    if (!isFunction())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RPNBuilderTree node is not a function");

    return RPNBuilderFunctionTreeNode(getNodeWithoutAlias(dag_node), getContext());
}

std::optional<RPNBuilderFunctionTreeNode> RPNBuilderTreeNode::toFunctionNodeOrNull() const
{
    if (!isFunction())
        return {};

    return RPNBuilderFunctionTreeNode(getNodeWithoutAlias(dag_node), getContext());
}

std::optional<RPNBuilderTreeNode> RPNBuilderTreeNode::getArrayJoinArgument() const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    if (node_without_alias->type == ActionsDAG::ActionType::ARRAY_JOIN && node_without_alias->children.size() == 1)
        return RPNBuilderTreeNode(node_without_alias->children[0], getContext());

    return {};
}

std::string RPNBuilderFunctionTreeNode::getFunctionName() const
{
    return dag_node->function_base->getName();
}

FunctionBasePtr RPNBuilderFunctionTreeNode::getFunctionBase() const
{
    return dag_node->function_base;
}

size_t RPNBuilderFunctionTreeNode::getArgumentsSize() const
{
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
    if (index >= total_arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                "RPNBuilderFunctionTreeNode has {} arguments, attempted to get argument at index {}",
                total_arguments, index);

    // indexHint arguments are stored inside of `FunctionIndexHint` class,
    // because they are used only for index analysis.
    if (dag_node->function_base->getName() == "indexHint")
    {
        const auto & adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor &>(*dag_node->function_base);
        const auto & index_hint = typeid_cast<const FunctionIndexHint &>(*adaptor.getFunction());
        return RPNBuilderTreeNode(index_hint.getActions().getOutputs()[index], getContext());
    }

    return RPNBuilderTreeNode(dag_node->children[index], getContext());
}

template <typename RPNElement>
RPNBuilder<RPNElement>::RPNBuilder(
    const ActionsDAG::Node * filter_actions_dag_node,
    ContextPtr query_context_,
    const ExtractAtomFromTreeFunction & extract_atom_from_tree_function_)
    : extract_atom_from_tree_function(extract_atom_from_tree_function_)
{
    traverseTree(RPNBuilderTreeNode(filter_actions_dag_node, query_context_));
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

        if constexpr (!RPNBuilderTraits<RPNElement>::expand_index_hint)
        {
            if (function_node.getFunctionName() == "indexHint")
            {
                element.function = RPNElement::ALWAYS_TRUE;
                rpn_elements.emplace_back(std::move(element));
                return;
            }
        }

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

/// Estimating selectivity is the one use that must not descend into `indexHint`.
template <>
struct RPNBuilderTraits<ConditionSelectivityEstimator::RPNElement>
{
    static constexpr bool expand_index_hint = false;
};

template class RPNBuilder<KeyCondition::RPNElement>;
template class RPNBuilder<ConditionSelectivityEstimator::RPNElement>;
template class RPNBuilder<MergeTreeConditionBloomFilterText::RPNElement>;
template class RPNBuilder<MergeTreeIndexConditionBloomFilter::RPNElement>;
template class RPNBuilder<MergeTreeIndexConditionText::RPNElement>;
}
