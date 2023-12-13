#include <Analyzer/EmitNode.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

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

bool EmitNode::isEqualImpl(const IQueryTreeNode & /* rhs */) const
{
    // TODO: proton
    return true;
}

void EmitNode::updateTreeHashImpl(HashState & /* state */) const
{
    // TODO: proton
}

QueryTreeNodePtr EmitNode::cloneImpl() const
{
    return std::make_shared<EmitNode>(emit_type, getIntervalFunction());
}

ASTPtr EmitNode::toASTImpl(const ConvertToASTOptions & /* options */) const
{
    // TODO: proton
    return nullptr;
}

}
