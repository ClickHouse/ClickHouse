#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove redundant `LEFT SEMI` / `LEFT ANTI` joins when one candidate's right-side dataset
  * is a structural subset of another's (same base table, same right-side join columns, and
  * the right-side `WHERE` conjuncts of one are contained in the other's).
  *
  * ──────────────────────────────────────────────────────────────────────────────
  * Cases that are optimized
  * ──────────────────────────────────────────────────────────────────────────────
  *
  *   1. SEMI: keep the stricter, drop the looser.
  *
  *        SELECT u1.uid FROM users u1
  *          LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  *          LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid;
  *      → u3 ⊆ u2, every row passing u3 already passes u2 → drop the join with u2.
  *
  *   2. ANTI: keep the looser, drop the stricter (NULL-set semantics flip the direction).
  *
  *        SELECT o.id FROM orders o
  *          LEFT ANTI JOIN users u  ON o.uid = u.id
  *          LEFT ANTI JOIN (SELECT * FROM users WHERE banned) u2 ON o.uid = u2.id;
  *      → u2 ⊆ u, the ANTI on u already excludes everything u2 would, so drop the join with u2.
  *
  * Limitations:
  *   - Only the simple `SELECT [...] FROM table [WHERE ...]` shape on the right side is
  *     recognised; anything more (GROUP BY, DISTINCT, LIMIT, aggregates, joins, ...) is skipped.
  *   - A `RIGHT` or `FULL` outer join between two candidates blocks elimination, because
  *     its NULL-padding interacts differently with SEMI and ANTI.
  *   - A SEMI/ANTI join is kept if any of its right-side columns are still referenced
  *     elsewhere in the query — removing it would leave a dangling column source.
  */
class RemoveRedundantSemiJoinPass final : public IQueryTreePass
{
public:
    String getName() override { return "RemoveRedundantSemiJoin"; }

    String getDescription() override
    {
        return "Remove redundant SEMI/ANTI JOINs where one right-hand side is a subset of another";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
