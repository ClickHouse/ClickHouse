#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite `count()` over an ARRAY JOIN whose element value is never used into an aggregate over the
  * array lengths, so the whole array does not have to be read.
  *
  * The number of rows produced by ARRAY JOIN depends only on the array lengths, not on the values.
  * When nothing else in the query references the joined-over array, counting rows is equivalent to
  * summing the lengths:
  *     SELECT count() FROM t ARRAY JOIN arr       ->  SELECT sum(length(arr)) FROM t
  *     SELECT count() FROM t LEFT ARRAY JOIN arr  ->  SELECT sum(greatest(length(arr), 1)) FROM t
  * (LEFT ARRAY JOIN emits one row for an empty array.)
  *
  * The following FunctionToSubcolumnsPass then folds `length(arr)` into the `arr.size0` subcolumn, so
  * only the offsets are read from storage. This also removes the `arrayWithConstant`-based rewrite's
  * TOO_LARGE_ARRAY_SIZE regression, because no array is materialized at all.
  *
  * Applied only for a plain `count()` with no other projection, GROUP BY, DISTINCT, or other clause
  * that would change the row cardinality, and only when the ARRAY JOIN has a single surviving joined
  * expression that is a plain physical Array/Map column of the joined table.
  */
class RewriteArrayJoinCountPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteArrayJoinCount"; }

    String getDescription() override
    {
        return "Rewrite count() over an ARRAY JOIN with unused element values into sum() over array lengths.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
