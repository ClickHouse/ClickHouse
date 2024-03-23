#pragma once

#include <Core/Block.h>

#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

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
    const Settings & getSettings() const
    {
        return query_context->getSettingsRef();
    }

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
template <typename RPNElement>
class RPNBuilder
{
public:
    using RPNElements = std::vector<RPNElement>;
    using ExtractAtomFromTreeFunction = std::function<bool (const RPNBuilderTreeNode & node, RPNElement & out)>;

    explicit RPNBuilder(const ActionsDAG::Node * filter_actions_dag_node,
        ContextPtr query_context_,
        const ExtractAtomFromTreeFunction & extract_atom_from_tree_function_)
        : tree_context(std::move(query_context_))
        , extract_atom_from_tree_function(extract_atom_from_tree_function_)
    {
        traverseTree(RPNBuilderTreeNode(filter_actions_dag_node, tree_context));
    }

    RPNElements && extractRPN() && { return std::move(rpn_elements); }

private:
    void traverseTree(const RPNBuilderTreeNode & node)
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

    bool extractLogicalOperatorFromTree(const RPNBuilderFunctionTreeNode & function_node, RPNElement & out)
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

    RPNBuilderTreeContext tree_context;
    const ExtractAtomFromTreeFunction & extract_atom_from_tree_function;
    RPNElements rpn_elements;
};

}
