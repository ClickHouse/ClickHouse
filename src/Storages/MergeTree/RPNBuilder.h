#pragma once

#include <Core/Block.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class IAST;
class Field;
class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;
class PreparedSets;
using PreparedSetsPtr = std::shared_ptr<PreparedSets>;
struct Settings;

/** Context of RPNBuilderTree.
  *
  * For AST tree context, precalculated block with constants and prepared sets are required for index analysis.
  * For DAG tree precalculated block with constants and prepared sets are not required, because constants and sets already
  * calculated inside COLUMN actions dag node.
  */
class RPNBuilderTreeContext
{
public:
    /// Construct RPNBuilderTreeContext for ActionsDAG tree
    explicit RPNBuilderTreeContext(ContextPtr query_context_);

    /// Construct RPNBuilderTreeContext for AST tree
    explicit RPNBuilderTreeContext(ContextPtr query_context_, Block block_with_constants_, PreparedSetsPtr prepared_sets_);

    /// Get query context
    const ContextPtr & getQueryContext() const
    {
        return query_context;
    }

    /// Get query context settings
    const Settings & getSettings() const;

    /** Get block with constants.
      * Valid only for AST tree.
      */
    const Block & getBlockWithConstants() const
    {
        return block_with_constants;
    }

    /** Get prepared sets.
      * Valid only for AST tree.
      */
    const PreparedSetsPtr & getPreparedSets() const
    {
        return prepared_sets;
    }

private:
    /// Valid for both AST and ActionDAG tree
    ContextPtr query_context;

    /// Valid only for AST tree
    Block block_with_constants;

    /// Valid only for AST tree
    PreparedSetsPtr prepared_sets;
};

class RPNBuilderFunctionTreeNode;

/** RPNBuilderTreeNode is wrapper around DAG or AST node.
  * It defines unified interface for index analysis.
  */
class RPNBuilderTreeNode
{
public:
    /// Construct RPNBuilderTreeNode with non null dag node and tree context
    explicit RPNBuilderTreeNode(const ActionsDAG::Node * dag_node_, RPNBuilderTreeContext & tree_context_);

    /// Construct RPNBuilderTreeNode with non null ast node and tree context
    explicit RPNBuilderTreeNode(const IAST * ast_node_, RPNBuilderTreeContext & tree_context_);

    /// Get AST node
    const IAST * getASTNode() const { return ast_node; }

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

    /// Try get prepared set from node that match data types
    FutureSetPtr tryGetPreparedSet(const DataTypes & data_types) const;

    /** Convert node to function node.
      * Node must be function before calling these method, otherwise exception is thrown.
      */
    RPNBuilderFunctionTreeNode toFunctionNode() const;

    /// Convert node to function node or null optional
    std::optional<RPNBuilderFunctionTreeNode> toFunctionNodeOrNull() const;

    /** If this node is `arrayJoin(x)`, return its argument node `x`; otherwise std::nullopt.
      * Handles both the DAG `ARRAY_JOIN` action node and the AST `ASTFunction` named `arrayJoin`.
      */
    std::optional<RPNBuilderTreeNode> getArrayJoinArgument() const;

    /// Get tree context
    const RPNBuilderTreeContext & getTreeContext() const
    {
        return tree_context;
    }

    /// Get tree context
    RPNBuilderTreeContext & getTreeContext()
    {
        return tree_context;
    }

protected:
    const IAST * ast_node = nullptr;
    const ActionsDAG::Node * dag_node = nullptr;
    RPNBuilderTreeContext & tree_context;
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
