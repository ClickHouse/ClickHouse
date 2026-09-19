#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Core/Names.h>
#include <DataTypes/IDataType_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/assert_cast.h>

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

class LambdaArgumentsNode;
using LambdaArgumentsNodePtr = std::shared_ptr<LambdaArgumentsNode>;

class LambdaArgumentsNode final : public ITableExpressionNode
{
public:
    explicit LambdaArgumentsNode(Names argument_names);

    QueryTreeNodeType getNodeType() const override { return QueryTreeNodeType::LAMBDA_ARGS; }
    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    const Names & getNames() const { return names; }
    const DataTypes & getTypes() const { return types; }

    void resolve(DataTypes argument_types)
    {
        types = std::move(argument_types);
    }

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions compare_options) const override;
    void updateTreeHashImpl(HashState & hash_state, CompareOptions compare_options) const override;
    QueryTreeNodePtr cloneImpl() const override;
    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

    Names names;
    DataTypes types;
};

class LambdaNode final : public IQueryTreeNode
{
public:
    /// Initialize lambda with argument names and lambda body expression
    explicit LambdaNode(LambdaArgumentsNodePtr arguments_, QueryTreeNodePtr expression_, bool is_operator_, DataTypePtr result_type_ = {});

    /// Get arguments
    const LambdaArgumentsNode & getArguments() const
    {
        return assert_cast<const LambdaArgumentsNode &>(*children[arguments_child_index]);
    }

    LambdaArgumentsNode & getArguments()
    {
        return assert_cast<LambdaArgumentsNode &>(*children[arguments_child_index]);
    }

    LambdaArgumentsNodePtr getArgumentsTyped()
    {
        return static_pointer_cast<LambdaArgumentsNode>(children[arguments_child_index]);
    }

    // /// Get arguments
    // ListNode & getArguments()
    // {
    //     return children[arguments_child_index]->as<ListNode &>();
    // }

    /// Get arguments node
    // const QueryTreeNodePtr & getArgumentsNode() const
    // {
    //     return children[arguments_child_index];
    // }

    // /// Get arguments node
    // QueryTreeNodePtr & getArgumentsNode()
    // {
    //     return children[arguments_child_index];
    // }

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
    DataTypePtr result_type;
    bool is_operator = false;

    static constexpr size_t arguments_child_index = 0;
    static constexpr size_t expression_child_index = 1;
    static constexpr size_t children_size = expression_child_index + 1;
};

}
