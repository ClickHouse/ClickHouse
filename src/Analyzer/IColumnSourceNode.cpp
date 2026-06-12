#include <Analyzer/IColumnSourceNode.h>

#include <atomic>

namespace DB
{

namespace
{

ColumnSourceId getNextColumnSourceId()
{
    static std::atomic<ColumnSourceId> column_source_id_counter{INVALID_COLUMN_SOURCE_ID};
    return ++column_source_id_counter;
}

}

IColumnSourceNode::IColumnSourceNode(size_t children_size)
    : IQueryTreeNode(children_size)
    , column_source_id(getNextColumnSourceId())
{
    is_column_source = true;
}

}
