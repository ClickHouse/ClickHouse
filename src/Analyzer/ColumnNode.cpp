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

void ColumnNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "COLUMN ";
    writePointerHex(this, buffer);
    buffer << ' ' << column.name << " : " << column.type->getName() << " source ";
    auto column_source_ptr = column_source.lock();
    writePointerHex(column_source_ptr.get(), buffer);
}

void ColumnNode::updateTreeHashImpl(HashState & hash_state) const
{
    hash_state.update(column.name.size());
    hash_state.update(column.name);

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
