#include <Analyzer/ColumnNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnNode::ColumnNode(NameAndTypePair column_, QueryTreeNodePtr expression_node_, QueryTreeNodeWeakPtr column_source_)
    : IQueryTreeNode(children_size, weak_pointers_size)
    , column(std::move(column_))
{
    children[expression_child_index] = std::move(expression_node_);
    getSourceWeakPointer() = std::move(column_source_);
}

ColumnNode::ColumnNode(NameAndTypePair column_, QueryTreeNodeWeakPtr column_source_)
    : ColumnNode(std::move(column_), nullptr /*expression_node*/, std::move(column_source_))
{
}

QueryTreeNodePtr ColumnNode::getColumnSource() const
{
    auto lock = getSourceWeakPointer().lock();
    if (!lock)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Column {} {} query tree node does not have valid source node",
            column.name,
            column.type->getName());

    return lock;
}

QueryTreeNodePtr ColumnNode::getColumnSourceOrNull() const
{
    return getSourceWeakPointer().lock();
}

void ColumnNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "COLUMN id: " << state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", column_name: " << column.name << ", result_type: " << column.type->getName();

    auto column_source_ptr = getSourceWeakPointer().lock();
    if (column_source_ptr)
        buffer << ", source_id: " << state.getNodeId(column_source_ptr.get());

    const auto & expression = getExpression();

    if (expression)
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION\n";
        expression->dumpTreeImpl(buffer, state, indent + 4);
    }
}

bool ColumnNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const ColumnNode &>(rhs);
    return column == rhs_typed.column;
}

void ColumnNode::updateTreeHashImpl(HashState & hash_state) const
{
    hash_state.update(column.name.size());
    hash_state.update(column.name);

    const auto & column_type_name = column.type->getName();
    hash_state.update(column_type_name.size());
    hash_state.update(column_type_name);
}

QueryTreeNodePtr ColumnNode::cloneImpl() const
{
    return std::make_shared<ColumnNode>(column, getColumnSource());
}

ASTPtr ColumnNode::toASTImpl() const
{
    return std::make_shared<ASTIdentifier>(column.name);
}

}
