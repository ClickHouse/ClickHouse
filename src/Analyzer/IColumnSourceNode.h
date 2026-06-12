#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Base class for query tree nodes that can be a column source, i.e. referenced by ColumnNode.
  *
  * Column sources are table expressions (table, table function, query, union, array join,
  * join for columns from the USING clause), lambda (for lambda arguments) and interpolate
  * (for the fake column referenced by the interpolate expression).
  *
  * Each column source instance has an id unique within the process, assigned at construction.
  * Cloning a column source therefore produces a node with a fresh id: a clone is a new source
  * instance, not the same source. This matters for example for a self join of a materialized CTE,
  * where the resolved source subtree is deep-cloned per reference into the same query tree, and
  * the two clones must stay distinguishable as column sources. References from ColumnNode are kept
  * consistent under cloning by IQueryTreeNode::cloneAndReplace, which re-points them together with
  * the column source weak pointers.
  */
class IColumnSourceNode : public IQueryTreeNode
{
public:
    /// Get the id that uniquely identifies this column source instance within the process
    ColumnSourceId getColumnSourceId() const
    {
        return column_source_id;
    }

protected:
    explicit IColumnSourceNode(size_t children_size);

private:
    const ColumnSourceId column_source_id;
};

}
