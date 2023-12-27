#include <Analyzer/EmitNode.h>

#include <Analyzer/ConstantNode.h>
#include <Common/assert_cast.h>
#include <Common/SipHash.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Parsers/Streaming/ASTEmitQuery.h>

namespace DB
{

const char * toString(EmitType type)
{
    switch (type)
    {
        case EmitType::PERIODIC: return "PERIODIC";
    }
}

EmitNode::EmitNode(EmitType emit_type_, QueryTreeNodePtr interval_function_)
    : IQueryTreeNode(children_size)
    , emit_type(std::move(emit_type_))
{
    children[interval_function_child_index] = std::move(interval_function_);
}

void EmitNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "EMIT id: " << format_state.getNodeId(this) << ", emit_type: " << toString(emit_type) << '\n';

    getIntervalFunction()->dumpTreeImpl(buffer, format_state, indent + 2);
}

bool EmitNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const EmitNode &>(rhs);
    return emit_type == rhs_typed.emit_type;
}

void EmitNode::updateTreeHashImpl(HashState & state) const
{
    state.update(emit_type);
    state.update(window_interval.interval);
    state.update(window_interval.unit);
}

QueryTreeNodePtr EmitNode::cloneImpl() const
{
    auto res = std::make_shared<EmitNode>(emit_type, getIntervalFunction());
    res->window_interval = window_interval;
    return res;
}

ASTPtr EmitNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto emit_ast = std::make_shared<ASTEmitQuery>();
    emit_ast->periodic_interval = children[interval_function_child_index]->toAST(options);

    return nullptr;
}

}
