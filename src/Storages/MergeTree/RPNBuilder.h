#pragma once

#include <Core/ColumnWithTypeAndName.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class Field;
class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;

class RPNBuilderFunctionTreeNode;

/** RPNBuilderTreeNode is wrapper around an ActionsDAG node.
  * It defines unified interface for index analysis.
  */
class RPNBuilderTreeNode : public WithContext
{
public:
    /// Construct RPNBuilderTreeNode with non null dag node and the query context shared by all nodes of one tree
    explicit RPNBuilderTreeNode(const ActionsDAG::Node * dag_node_, const ContextPtr & query_context_);

    /// Get DAG node
    const ActionsDAG::Node * getDAGNode() const { return dag_node; }

    /// Get column name
    std::string getColumnName() const;

    /** Get column name.
      * Function `modulo` is replaced with `moduloLegacy`.
      */
    std::string getColumnNameWithModuloLegacy() const;

    /// Is node function
    bool isFunction() const;

    /// Is node constant
    bool isConstant() const;

    /// Whether the node's result type is Nullable
    bool isNullable() const;

    bool isSubqueryOrSet() const;

    /** Get constant as constant column.
      * Node must be constant before calling these method, otherwise logical exception is thrown.
      */
    ColumnWithTypeAndName getConstantColumn() const;

    /** Try get constant from node. If node is constant returns true, and constant value and constant type output parameters are set.
      * Otherwise false is returned.
      */
    bool tryGetConstant(Field & output_value, DataTypePtr & output_type) const;

    /// Try get prepared set from node
    FutureSetPtr tryGetPreparedSet() const;

    /** Convert node to function node.
      * Node must be function before calling these method, otherwise exception is thrown.
      */
    RPNBuilderFunctionTreeNode toFunctionNode() const;

    /// Convert node to function node or null optional
    std::optional<RPNBuilderFunctionTreeNode> toFunctionNodeOrNull() const;

    /// If this node is the `ARRAY_JOIN` action `arrayJoin(x)`, return its argument node `x`; otherwise std::nullopt.
    std::optional<RPNBuilderTreeNode> getArrayJoinArgument() const;

protected:
    const ActionsDAG::Node * dag_node = nullptr;
};

/** RPNBuilderFunctionTreeNode is wrapper around RPNBuilderTreeNode with function type.
  * It provide additional functionality that is specific for function.
  */
class RPNBuilderFunctionTreeNode : public RPNBuilderTreeNode
{
public:
    using RPNBuilderTreeNode::RPNBuilderTreeNode;

    /// Get function name
    std::string getFunctionName() const;

    FunctionBasePtr getFunctionBase() const;

    /// Get function arguments size
    size_t getArgumentsSize() const;

    /// Get function argument at index
    RPNBuilderTreeNode getArgumentAt(size_t index) const;
};

/** RPN Builder build stack of reverse polish notation elements (RPNElements) required for index analysis.
  *
  * RPNBuilder client must provide RPNElement type that has following interface:
  *
  * struct RPNElementInterface
  * {
  *     enum Function
  *     {
  *         FUNCTION_UNKNOWN, /// Can take any value.
  *         /// Operators of the logical expression.
  *         FUNCTION_NOT,
  *         FUNCTION_AND,
  *         FUNCTION_OR,
  *         ...
  *     };
  *
  *   RPNElementInterface();
  *
  *   Function function = FUNCTION_UNKNOWN;
  *
  * }
  *
  * RPNBuilder take care of building stack of RPNElements with `NOT`, `AND`, `OR` types.
  * In addition client must provide ExtractAtomFromTreeFunction that returns true and RPNElement as output parameter,
  * if it can convert RPNBuilderTree node to RPNElement, false otherwise.
  */
/// `indexHint` exists so that index analysis can see a condition that is never executed. A consumer
/// that analyses indexes has to descend into it - that is the whole point of the hint. A consumer
/// that estimates how selective an expression is must not: the condition removes no rows, since the
/// function evaluates to 1 for every row, so descending into it applies a selectivity the query does
/// not have. Where the hint holds a conjunct derived from its siblings (`LogicalExpressionOptimizerPass`
/// wraps those it derived from a chain of comparisons), it would also apply that conjunct's
/// selectivity twice, once for the original and once for the copy. Such a consumer specialises this
/// trait and gets an `ALWAYS_TRUE` leaf for the whole hint.
///
/// `ALWAYS_TRUE` bounds the claim to the number of rows the condition removes; a hint is not inert.
/// It takes part in index analysis and prunes the read set, so a relation under one can yield fewer
/// rows than a selectivity-based estimate suggests. That is invisible to
/// `ConditionSelectivityEstimator` for every predicate, not just hints: it estimates
/// `total_rows * selectivity`, and `total_rows` counts whole parts, mark ranges included whether the
/// index selected them or not. Pruning is carried by a separate estimate,
/// `RowEstimateSource::PrimaryIndex`, which is used only when column statistics are missing.
template <typename RPNElement>
struct RPNBuilderTraits
{
    static constexpr bool expand_index_hint = true;
};

template <typename RPNElement>
class RPNBuilder
{
public:
    using RPNElements = std::vector<RPNElement>;
    using ExtractAtomFromTreeFunction = std::function<bool (const RPNBuilderTreeNode & node, RPNElement & out)>;

    explicit RPNBuilder(
        const ActionsDAG::Node * filter_actions_dag_node,
        ContextPtr query_context_,
        const ExtractAtomFromTreeFunction & extract_atom_from_tree_function_);

    explicit RPNBuilder(const RPNBuilderTreeNode & node, const ExtractAtomFromTreeFunction & extract_atom_from_tree_function_);
    RPNElements && extractRPN() &&;

private:
    void traverseTree(const RPNBuilderTreeNode & node);
    bool extractLogicalOperatorFromTree(const RPNBuilderFunctionTreeNode & function_node, RPNElement & out);
    const ExtractAtomFromTreeFunction & extract_atom_from_tree_function;
    RPNElements rpn_elements;
};

}
