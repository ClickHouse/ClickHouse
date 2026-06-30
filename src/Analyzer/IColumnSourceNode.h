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
  * By default cloning preserves the id: a clone is the same logical source (e.g. an expression
  * containing a lambda or a subquery is cloned for a GROUP BY key, and the clone must compare
  * equal to the original). Sites that mint a genuinely separate instance — most importantly CTE
  * expansion, where the resolved source subtree is deep-cloned per reference into the same query
  * tree and the clones must stay distinguishable — use IQueryTreeNode::cloneWithFreshColumnSourceIds.
  * References from ColumnNode are kept consistent under cloning by IQueryTreeNode::cloneAndReplace,
  * which re-points them and refreshes their id snapshot.
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
    friend class IQueryTreeNode;

    /// Mutable so cloneAndReplace can preserve the original id on a clone (a freshly minted id is
    /// assigned at construction and overwritten there unless fresh ids were explicitly requested).
    ColumnSourceId column_source_id;
};

}
