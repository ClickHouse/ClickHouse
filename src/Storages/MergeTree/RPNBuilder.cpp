#include <Storages/MergeTree/RPNBuilder.h>

#include <algorithm>

#include <Storages/MergeTree/KeyCondition.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>

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

/// Whether `modulo(dividend, divisor)` returns the same value as `moduloLegacy(dividend, divisor)` for every
/// dividend, judging by the argument types (and the divisor, if it is a constant).
///
/// `moduloLegacy` applies the C++ `%` to its arguments and casts the result to the signed type of the divisor's
/// width if either argument is signed. `modulo` returns the mathematical remainder, whose sign follows the dividend.
/// With both arguments signed or both unsigned the two agree: `|result| < |divisor|` always fits.
/// With mixed signs they agree only if:
///  * the usual arithmetic conversions of `%` end in a signed type, i.e. the signed operand is wider than the
///    unsigned one, or both are narrower than `int` (`Int32 % UInt32` converts a negative dividend to unsigned);
///  * and the remainder fits the signed type of the divisor's width. A non-negative remainder (unsigned dividend)
///    always does. One of a signed dividend does when the dividend is not wider than the divisor, and
///    otherwise only when the constant divisor is small enough: `Int32 % 16` is fine, `Int32 % 200` is not
///    (`-199` does not fit `Int8`), and neither is `Int32 % 129` (`128` does not).
bool isModuloSameAsModuloLegacy(const ActionsDAG::Node & modulo_node)
{
    if (modulo_node.children.size() != 2)
        return false;

    const auto & divisor_node = *modulo_node.children[1];
    const auto dividend_type = removeNullable(removeLowCardinality(modulo_node.children[0]->result_type));
    const auto divisor_type = removeNullable(removeLowCardinality(divisor_node.result_type));
    const WhichDataType dividend(dividend_type);
    const WhichDataType divisor(divisor_type);

    auto is_integer = [](const WhichDataType & type) { return type.isInt() || type.isUInt(); };
    if (!is_integer(dividend) || !is_integer(divisor))
        return false;

    if (dividend.isInt() == divisor.isInt())
        return true;

    const size_t dividend_size = dividend_type->getSizeOfValueInMemory();
    const size_t divisor_size = divisor_type->getSizeOfValueInMemory();
    const size_t signed_size = dividend.isInt() ? dividend_size : divisor_size;
    const size_t unsigned_size = dividend.isInt() ? divisor_size : dividend_size;

    if (!(signed_size > unsigned_size || (signed_size < 4 && unsigned_size < 4)))
        return false;

    if (!dividend.isInt())
        return true;

    if (dividend_size <= divisor_size)
        return true;

    /// The remainder reaches `divisor - 1` in magnitude, which has to fit the signed type of the divisor's width.
    if (divisor_size > sizeof(UInt64))
        return false;

    const ActionsDAG::Node * constant = &divisor_node;
    while (constant->type == ActionsDAG::ActionType::ALIAS)
        constant = constant->children.front();

    if (constant->type != ActionsDAG::ActionType::COLUMN || !constant->column || !isColumnConst(*constant->column))
        return false;

    const Field value = (*constant->column)[0];
    if (value.getType() != Field::Types::UInt64 || value.safeGet<UInt64>() == 0)
        return false;

    return value.safeGet<UInt64>() - 1 <= (UInt64(1) << (8 * divisor_size - 1)) - 1;
}

/// Whether every `modulo` in the expression can be replaced with `moduloLegacy` without changing the value.
/// The names of lambdas are not inspected, so an expression with one is declined.
bool canReplaceModuloWithModuloLegacy(const ActionsDAG::Node & node)
{
    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
        case ActionsDAG::ActionType::PLACEHOLDER:
            return true;
        case ActionsDAG::ActionType::COLUMN:
            return !node.column || !typeid_cast<const ColumnFunction *>(&node.column->getDataColumn());
        case ActionsDAG::ActionType::ALIAS:
        case ActionsDAG::ActionType::ARRAY_JOIN:
            return canReplaceModuloWithModuloLegacy(*node.children.front());
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (typeid_cast<const ExecutableFunctionCapture *>(node.function.get()))
                return false;

            if (node.function_base->getName() == "modulo" && !isModuloSameAsModuloLegacy(node))
                return false;

            return std::ranges::all_of(node.children, [](const auto * child) { return canReplaceModuloWithModuloLegacy(*child); });
        }
    }
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

std::optional<std::string> RPNBuilderTreeNode::getColumnNameWithModuloLegacy() const
{
    if (!canReplaceModuloWithModuloLegacy(*dag_node))
        return std::nullopt;

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

/// A `Field` is the plain value of a constant: `LowCardinality` is only an encoding of the column,
/// and a value that is not NULL has no `Nullable` type.
static DataTypePtr getTypeOfConstantValue(const Field & value, const DataTypePtr & type)
{
    auto value_type = removeLowCardinality(type);
    if (!value.isNull())
        value_type = removeNullable(value_type);
    return value_type;
}

bool RPNBuilderTreeNode::tryGetConstant(Field & output_value, DataTypePtr & output_type) const
{
    const auto * node_without_alias = getNodeWithoutAlias(dag_node);
    if (!node_without_alias->column)
        return false;

    output_value = node_without_alias->column->getField();
    output_type = getTypeOfConstantValue(output_value, node_without_alias->result_type);
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

namespace
{

/// Whether converting a value of type `from` to type `to` never changes it and never throws.
/// `Nullable` cannot be dropped, because it may throw on NULL.
bool isLosslessConversion(const DataTypePtr & from, const DataTypePtr & to)
{
    auto from_type = removeLowCardinality(from);
    auto to_type = removeLowCardinality(to);

    if (to_type->isNullable())
    {
        from_type = removeNullable(from_type);
        to_type = removeNullable(to_type);
    }
    else if (from_type->isNullable())
    {
        return false;
    }

    if (from_type->equals(*to_type))
        return true;

    const auto * from_array = typeid_cast<const DataTypeArray *>(from_type.get());
    const auto * to_array = typeid_cast<const DataTypeArray *>(to_type.get());
    return from_array && to_array && isLosslessConversion(from_array->getNestedType(), to_array->getNestedType());
}

}

bool isLosslessConversionFunction(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return false;

    const auto function_name = node.function_base->getName();
    const size_t arguments_size = node.children.size();

    const bool is_cast = (function_name == "CAST" || function_name == "_CAST") && arguments_size == 2;
    const bool is_wrapper = (function_name == "toNullable" || function_name == "toLowCardinality") && arguments_size == 1;

    if (!is_cast && !is_wrapper)
        return false;

    return isLosslessConversion(node.children.front()->result_type, node.result_type);
}

RPNBuilderTreeNode unwrapLosslessConversion(const RPNBuilderTreeNode & node)
{
    if (!node.isFunction())
        return node;

    const auto function = node.toFunctionNode();
    if (!isLosslessConversionFunction(*function.getDAGNode()))
        return node;

    return unwrapLosslessConversion(function.getArgumentAt(0));
}

const ActionsDAG::Node * unwrapLosslessConversion(const ActionsDAG::Node * node)
{
    const auto * node_without_alias = getNodeWithoutAlias(node);

    if (!isLosslessConversionFunction(*node_without_alias))
        return node;

    return unwrapLosslessConversion(node_without_alias->children.front());
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
