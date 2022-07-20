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

ColumnNode::ColumnNode(NameAndTypePair column_, QueryTreeNodeWeakPtr column_source_)
    : column(std::move(column_))
    , column_source(std::move(column_source_))
{
    children.resize(1);
}

ColumnNode::ColumnNode(NameAndTypePair column_, QueryTreeNodePtr alias_expression_node_, QueryTreeNodeWeakPtr column_source_)
    : column(std::move(column_))
    , column_source(std::move(column_source_))
{
    children.resize(1);
    children[0] = std::move(alias_expression_node_);
}

QueryTreeNodePtr ColumnNode::getColumnSource() const
{
    auto lock = column_source.lock();
    if (!lock)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Column {} {} query tree node does not have valid source node",
            column.name,
            column.type->getName());

    return lock;
}

void ColumnNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "COLUMN id: " << state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", column_name: " << column.name << ", result_type: " << column.type->getName();

    auto column_source_ptr = column_source.lock();
    if (column_source_ptr)
        buffer << ", source_id: " << state.getNodeId(column_source_ptr.get());

    const auto & alias_expression = getAliasExpression();

    if (alias_expression)
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "ALIAS EXPRESSION\n";
        alias_expression->dumpTreeImpl(buffer, state, indent + 4);
    }
}

bool ColumnNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const ColumnNode &>(rhs);
    if (column != rhs_typed.column)
        return false;

    auto source_ptr = column_source.lock();
    auto rhs_source_ptr = rhs_typed.column_source.lock();

    if (!source_ptr && !rhs_source_ptr)
        return true;
    else if (source_ptr && !rhs_source_ptr)
        return false;
    else if (!source_ptr && rhs_source_ptr)
        return false;

    return source_ptr->isEqualImpl(*rhs_source_ptr);
}

void ColumnNode::updateTreeHashImpl(HashState & hash_state) const
{
    hash_state.update(column.name.size());
    hash_state.update(column.name);

    const auto & column_type_name = column.type->getName();
    hash_state.update(column_type_name.size());
    hash_state.update(column_type_name);

    auto column_source_ptr = column_source.lock();
    if (column_source_ptr)
        column_source_ptr->updateTreeHashImpl(hash_state);
}

QueryTreeNodePtr ColumnNode::cloneImpl() const
{
    return std::make_shared<ColumnNode>(column, column_source);
}

void ColumnNode::getPointersToUpdateAfterClone(QueryTreePointersToUpdate & pointers_to_update)
{
    /** This method is called on node returned from `cloneImpl`. Check IQueryTreeNode.h interface.
      * old pointer is current column source pointer.
      * update place is address of column source.
      */
    const auto * old_pointer = getColumnSource().get();
    pointers_to_update.emplace_back(old_pointer, &column_source);
}

ASTPtr ColumnNode::toASTImpl() const
{
    return std::make_shared<ASTIdentifier>(column.name);
}

}
