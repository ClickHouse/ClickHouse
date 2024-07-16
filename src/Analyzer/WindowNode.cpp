#include <Analyzer/WindowNode.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>

namespace DB
{

WindowNode::WindowNode(WindowFrame window_frame_)
    : IQueryTreeNode(children_size)
    , window_frame(std::move(window_frame_))
{
    children[partition_by_child_index] = std::make_shared<ListNode>();
    children[order_by_child_index] = std::make_shared<ListNode>();
}

void WindowNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "WINDOW id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    if (!parent_window_name.empty())
        buffer << ", parent_window_name: " << parent_window_name;

    buffer << ", frame_type: " << window_frame.type;

    auto window_frame_bound_type_to_string = [](WindowFrame::BoundaryType boundary_type, bool boundary_preceding)
    {
        std::string value;

        if (boundary_type == WindowFrame::BoundaryType::Unbounded)
            value = "unbounded";
        else if (boundary_type == WindowFrame::BoundaryType::Current)
            value = "current";
        else if (boundary_type == WindowFrame::BoundaryType::Offset)
            value = "offset";

        if (boundary_type != WindowFrame::BoundaryType::Current)
        {
            if (boundary_preceding)
                value += " preceding";
            else
                value += " following";
        }

        return value;
    };

    buffer << ", frame_begin_type: " << window_frame_bound_type_to_string(window_frame.begin_type, window_frame.begin_preceding);
    buffer << ", frame_end_type: " << window_frame_bound_type_to_string(window_frame.end_type, window_frame.end_preceding);

    if (hasPartitionBy())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "PARTITION BY\n";
        getPartitionBy().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasOrderBy())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "ORDER BY\n";
        getOrderBy().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasFrameBeginOffset())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "FRAME BEGIN OFFSET\n";
        getFrameBeginOffsetNode()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasFrameEndOffset())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "FRAME END OFFSET\n";
        getFrameEndOffsetNode()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool WindowNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const WindowNode &>(rhs);

    return window_frame == rhs_typed.window_frame && parent_window_name == rhs_typed.parent_window_name;
}

void WindowNode::updateTreeHashImpl(HashState & hash_state, CompareOptions) const
{
    hash_state.update(window_frame.is_default);
    hash_state.update(window_frame.type);
    hash_state.update(window_frame.begin_type);
    hash_state.update(window_frame.begin_preceding);
    hash_state.update(window_frame.end_type);
    hash_state.update(window_frame.end_preceding);

    hash_state.update(parent_window_name);
}

QueryTreeNodePtr WindowNode::cloneImpl() const
{
    auto window_node = std::make_shared<WindowNode>(window_frame);
    window_node->parent_window_name = parent_window_name;

    return window_node;
}

ASTPtr WindowNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto window_definition = std::make_shared<ASTWindowDefinition>();

    window_definition->parent_window_name = parent_window_name;

    if (hasPartitionBy())
    {
        window_definition->children.push_back(getPartitionByNode()->toAST(options));
        window_definition->partition_by = window_definition->children.back();
    }

    if (hasOrderBy())
    {
        window_definition->children.push_back(getOrderByNode()->toAST(options));
        window_definition->order_by = window_definition->children.back();
    }

    window_definition->frame_is_default = window_frame.is_default;
    window_definition->frame_type = window_frame.type;
    window_definition->frame_begin_type = window_frame.begin_type;
    window_definition->frame_begin_preceding = window_frame.begin_preceding;

    if (hasFrameBeginOffset())
    {
        window_definition->children.push_back(getFrameBeginOffsetNode()->toAST(options));
        window_definition->frame_begin_offset = window_definition->children.back();
    }

    window_definition->frame_end_type = window_frame.end_type;
    window_definition->frame_end_preceding = window_frame.end_preceding;
    if (hasFrameEndOffset())
    {
        window_definition->children.push_back(getFrameEndOffsetNode()->toAST(options));
        window_definition->frame_end_offset = window_definition->children.back();
    }

    return window_definition;
}

}
