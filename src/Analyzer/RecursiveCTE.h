#pragma once

#include <Core/NamesAndTypes.h>

#include <Interpreters/DatabaseCatalog.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/TableNode.h>

namespace DB
{

/** Recursive CTEs allow to recursively evaluate UNION subqueries.
  *
  * Overview:
  * https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-RECURSIVE
  *
  * Current implementation algorithm:
  *
  * During query analysis, when we resolve UNION node that is inside WITH RECURSIVE section of parent query we:
  * 1. First resolve non recursive subquery.
  * 2. Create temporary table using projection columns of resolved subquery from step 1.
  * 3. Create temporary table expression node using storage from step 2.
  * 4. Create resolution scope for recursive subquery. In that scope we add node from step 3 as expression argument with UNION node CTE name.
  * 5. Resolve recursive subquery.
  * 6. If in resolved UNION node temporary table expression storage from step 2 is used, we update UNION query with recursive CTE table.
  *
  * During query planning if UNION node contains recursive CTE table, we add ReadFromRecursiveCTEStep to query plan. That step is responsible for whole
  * recursive CTE query execution.
  *
  * TODO: Improve locking in ReadFromRecursiveCTEStep.
  * TODO: Improve query analysis if query contains aggregates, JOINS, GROUP BY, ORDER BY, LIMIT, OFFSET.
  * TODO: Support SEARCH DEPTH FIRST BY, SEARCH BREADTH FIRST BY syntax.
  * TODO: Support CYCLE syntax.
  * TODO: Support UNION DISTINCT recursive CTE mode.
  */
class RecursiveCTETable
{
public:
    RecursiveCTETable(TemporaryTableHolderPtr holder_,
      StoragePtr storage_,
      NamesAndTypes columns_);

    StorageID getStorageID() const;

    TemporaryTableHolderPtr holder;
    StoragePtr storage;
    NamesAndTypes columns;
};

}
