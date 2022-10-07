#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/ConstantValue.h>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;

/** Function node represents function in query tree.
  * Function syntax: function_name(parameter_1, ...)(argument_1, ...).
  * If function does not have parameters its syntax is function_name(argument_1, ...).
  * If function does not have arguments its syntax is function_name().
  *
  * In query tree function parameters and arguments are represented by ListNode.
  *
  * Function can be:
  * 1. Aggregate function. Example: quantile(0.5)(x), sum(x).
  * 2. Non aggregate function. Example: plus(x, x).
  * 3. Window function. Example: sum(x) OVER (PARTITION BY expr ORDER BY expr).
  *
  * Initially function node is initialize with function name.
  * For window function client must initialize function window node.
  *
  * During query analysis pass function must be resolved using `resolveAsFunction`, `resolveAsAggregateFunction`, `resolveAsWindowFunction` methods.
  * Resolved function is function that has result type and is initialized with concrete aggregate or non aggregate function.
  */
class FunctionNode;
using FunctionNodePtr = std::shared_ptr<FunctionNode>;

class FunctionNode final : public IQueryTreeNode
{
public:
    /** Initialize function node with function name.
      * Later during query analysis pass function must be resolved.
      */
    explicit FunctionNode(String function_name_);

    /// Get name
    const String & getFunctionName() const
    {
        return function_name;
    }

    /// Get parameters
    const ListNode & getParameters() const
    {
        return children[parameters_child_index]->as<const ListNode &>();
    }

    /// Get parameters
    ListNode & getParameters()
    {
        return children[parameters_child_index]->as<ListNode &>();
    }

    /// Get parameters node
    const QueryTreeNodePtr & getParametersNode() const
    {
        return children[parameters_child_index];
    }

    /// Get parameters node
    QueryTreeNodePtr & getParametersNode()
    {
        return children[parameters_child_index];
    }

    /// Get arguments
    const ListNode & getArguments() const
    {
        return children[arguments_child_index]->as<const ListNode &>();
    }

    /// Get arguments
    ListNode & getArguments()
    {
        return children[arguments_child_index]->as<ListNode &>();
    }

    /// Get arguments node
    const QueryTreeNodePtr & getArgumentsNode() const
    {
        return children[arguments_child_index];
    }

    /// Get arguments node
    QueryTreeNodePtr & getArgumentsNode()
    {
        return children[arguments_child_index];
    }

    /// Has window
    bool hasWindow() const
    {
        return children[window_child_index] != nullptr;
    }

    /** Get window node.
      * Valid only for window function node.
      * Can be identifier if window function is defined as expr OVER window_name.
      * Or can be window node if window function is defined as expr OVER (window_name ...).
      */
    const QueryTreeNodePtr & getWindowNode() const
    {
        return children[window_child_index];
    }

    /** Get window node.
      * Valid only for window function node.
      */
    QueryTreeNodePtr & getWindowNode()
    {
        return children[window_child_index];
    }

    /** Get non aggregate function.
      * If function is not resolved nullptr returned.
      */
    const FunctionOverloadResolverPtr & getFunction() const
    {
        return function;
    }

    /** Get aggregate function.
      * If function is not resolved nullptr returned.
      * If function is resolved as non aggregate function nullptr returned.
      */
    const AggregateFunctionPtr & getAggregateFunction() const
    {
        return aggregate_function;
    }

    /// Is function node resolved
    bool isResolved() const
    {
        return result_type != nullptr && (function != nullptr || aggregate_function != nullptr);
    }

    /// Is function node window function
    bool isWindowFunction() const
    {
        return getWindowNode() != nullptr;
    }

    /// Is function node aggregate function
    bool isAggregateFunction() const
    {
        return aggregate_function != nullptr && !isWindowFunction();
    }

    /// Is function node ordinary function
    bool isOrdinaryFunction() const
    {
        return function != nullptr;
    }

    /** Resolve function node as non aggregate function.
      * It is important that function name is updated with resolved function name.
      * Main motivation for this is query tree optimizations.
      * Assume we have `multiIf` function with single argument, it can be converted to `if` function.
      * Function name must be updated accordingly.
      */
    void resolveAsFunction(FunctionOverloadResolverPtr function_value, DataTypePtr result_type_value);

    /** Resolve function node as aggregate function.
      * It is important that function name is updated with resolved function name.
      * Main motivation for this is query tree optimizations.
      */
    void resolveAsAggregateFunction(AggregateFunctionPtr aggregate_function_value, DataTypePtr result_type_value);

    /** Resolve function node as window function.
      * It is important that function name is updated with resolved function name.
      * Main motivation for this is query tree optimizations.
      */
    void resolveAsWindowFunction(AggregateFunctionPtr window_function_value, DataTypePtr result_type_value);

    /// Perform constant folding for function node
    void performConstantFolding(ConstantValuePtr constant_folded_value)
    {
        constant_value = std::move(constant_folded_value);
    }

    ConstantValuePtr getConstantValueOrNull() const override
    {
        return constant_value;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::FUNCTION;
    }

    DataTypePtr getResultType() const override
    {
        return result_type;
    }

    String getName() const override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & hash_state) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    String function_name;
    FunctionOverloadResolverPtr function;
    AggregateFunctionPtr aggregate_function;
    DataTypePtr result_type;
    ConstantValuePtr constant_value;

    static constexpr size_t parameters_child_index = 0;
    static constexpr size_t arguments_child_index = 1;
    static constexpr size_t window_child_index = 2;
    static constexpr size_t children_size = window_child_index + 1;
};

}
