#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

enum class EmitType
{
    PERIODIC
};

/// Convert emit type to string
const char * toString(EmitType type);

class EmitNode final : public IQueryTreeNode
{
public:
    explicit EmitNode(EmitType emit_type_, QueryTreeNodePtr interval_function_);

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::EMIT;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    EmitType getEmitType() const
    {
        return emit_type;
    }

    bool hasIntervalFunction() const
    {
        return children[interval_function_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getIntervalFunction() const
    {
        return children[interval_function_child_index];
    }

    QueryTreeNodePtr & getIntervalFunction()
    {
        return children[interval_function_child_index];
    }

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & state) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    EmitType emit_type;

    static constexpr size_t interval_function_child_index = 0;
    static constexpr size_t children_size = interval_function_child_index + 1;

};
}
