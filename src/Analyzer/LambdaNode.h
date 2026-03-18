#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/IdentifierNode.h>

#include <Parsers/ASTFunction.h>

namespace DB
{

/** Lambda node represents lambda expression in query tree.
  *
  * Lambda consist of argument names and lambda expression body.
  * Lambda expression body does not necessary use lambda arguments. Example: SELECT arrayMap(x -> 1, [1, 2, 3])
  *
  * Initially lambda is initialized with argument names and lambda body expression.
  *
  * Lambda expression result type can depend on arguments types.
  * Example: WITH (x -> x) as lambda SELECT lambda(1), lambda('string_value').
  *
  * During query analysis pass lambdas must be resolved.
  * Lambda resolve must set concrete lambda arguments and resolve lambda expression body.
  * In query tree lambda arguments are represented by ListNode.
  * If client modified lambda arguments array its size must be equal to initial lambda argument names array.
  *
  * Examples:
  * WITH (x -> x + 1) as lambda SELECT lambda(1);
  * SELECT arrayMap(x -> x + 1, [1,2,3]);
  */
class LambdaNode;
using LambdaNodePtr = std::shared_ptr<LambdaNode>;

class LambdaNode final : public IQueryTreeNode
{
public:
    /// Initialize lambda with argument names and lambda body expression
    explicit LambdaNode(Names argument_names_, QueryTreeNodePtr expression_, DataTypePtr result_type_ = {});

    /// Get argument names
    const Names & getArgumentNames() const
    {
        return argument_names;
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

    /// Get expression
    const QueryTreeNodePtr & getExpression() const
    {
        return children[expression_child_index];
    }

    /// Get expression
    QueryTreeNodePtr & getExpression()
    {
        return children[expression_child_index];
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::LAMBDA;
    }

    DataTypePtr getResultType() const override
    {
        return result_type;
    }

    void resolve(DataTypePtr lambda_type)
    {
        result_type = std::move(lambda_type);
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    Names argument_names;
    DataTypePtr result_type;

    static constexpr size_t arguments_child_index = 0;
    static constexpr size_t expression_child_index = 1;
    static constexpr size_t children_size = expression_child_index + 1;
};

}
