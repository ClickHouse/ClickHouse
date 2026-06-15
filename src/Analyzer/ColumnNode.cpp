#include <Analyzer/ColumnNode.h>
#include <Analyzer/IColumnSourceNode.h>
#include <Analyzer/TableNode.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

ColumnSourceId extractColumnSourceId(const QueryTreeNodePtr & source, const NameAndTypePair & column)
{
    if (!source)
        return INVALID_COLUMN_SOURCE_ID;

    if (!source->isColumnSource())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Column {} {} source must be a column source node, got {}",
            column.name,
            column.type->getName(),
            source->getNodeTypeName());

    return static_cast<const IColumnSourceNode &>(*source).getColumnSourceId();
}

}

ColumnNode::ColumnNode(
    NameAndTypePair column_,
    QueryTreeNodePtr expression_node_,
    QueryTreeNodeWeakPtr column_source_
)
    : IQueryTreeNode(children_size)
    , column(std::move(column_))
    , column_source(std::move(column_source_))
{
    children[expression_child_index] = std::move(expression_node_);
    column_source_id = extractColumnSourceId(column_source.lock(), column);
}

ColumnNode::ColumnNode(
    NameAndTypePair column_,
    QueryTreeNodeWeakPtr column_source_
)
    : ColumnNode(std::move(column_), nullptr /*expression_node*/, std::move(column_source_))
{}

ColumnNode::ColumnNode(
    NameAndTypePair column_,
    QueryTreeNodeWeakPtr column_source_,
    ColumnSourceId column_source_id_
)
    : IQueryTreeNode(children_size)
    , column(std::move(column_))
    , column_source(std::move(column_source_))
    , column_source_id(column_source_id_)
{
}

void ColumnNode::setColumnSource(const QueryTreeNodePtr & source)
{
    column_source = source;
    column_source_id = extractColumnSourceId(source, column);
}

void ColumnNode::remapColumnSourceAfterClone(const ReplacementMap & old_pointer_to_new_pointer)
{
    auto it = old_pointer_to_new_pointer.find(column_source.lock().get());
    if (it == old_pointer_to_new_pointer.end())
        return;

    column_source = it->second;
    column_source_id = extractColumnSourceId(it->second, column);
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

QueryTreeNodePtr ColumnNode::getColumnSourceOrNull() const
{
    return column_source.lock();
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

    const auto & expression = getExpression();

    if (expression)
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION\n";
        expression->dumpTreeImpl(buffer, state, indent + 4);
    }
}

bool ColumnNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions /*compare_options*/) const
{
    const auto & rhs_typed = assert_cast<const ColumnNode &>(rhs);
    return column.name == rhs_typed.column.name && column.type->equals(*rhs_typed.column.type);
}

void ColumnNode::updateTreeHashImpl(HashState & hash_state, CompareOptions /*compare_options*/) const
{
    hash_state.update(column.name.size());
    hash_state.update(column.name);

    column.type->updateHash(hash_state);
}

QueryTreeNodePtr ColumnNode::cloneImpl() const
{
    return std::make_shared<ColumnNode>(column, column_source, column_source_id);
}

ASTPtr ColumnNode::toASTImpl(const ConvertToASTOptions & options) const
{
    std::vector<std::string> column_identifier_parts;

    auto column_source_node = getColumnSourceOrNull();
    if (column_source_node && options.fully_qualified_identifiers)
    {
        auto node_type = column_source_node->getNodeType();
        if (node_type == QueryTreeNodeType::TABLE ||
            node_type == QueryTreeNodeType::TABLE_FUNCTION ||
            node_type == QueryTreeNodeType::QUERY ||
            node_type == QueryTreeNodeType::UNION)
        {
            if (column_source_node->hasAlias())
            {
                column_identifier_parts = {column_source_node->getAlias()};
            }
            else if (auto * table_node = column_source_node->as<TableNode>())
            {
                if (!table_node->getTemporaryTableName().empty())
                {
                    column_identifier_parts = { table_node->getTemporaryTableName() };
                }
                else
                {
                    const auto & table_storage_id = table_node->getStorageID();
                    if (table_storage_id.hasDatabase() && options.qualify_indentifiers_with_database)
                        column_identifier_parts = { table_storage_id.getDatabaseName(), table_storage_id.getTableName() };
                    else
                        column_identifier_parts = { table_storage_id.getTableName() };
                }
            }
        }
    }

    column_identifier_parts.push_back(column.name);

    return make_intrusive<ASTIdentifier>(std::move(column_identifier_parts));
}

}
