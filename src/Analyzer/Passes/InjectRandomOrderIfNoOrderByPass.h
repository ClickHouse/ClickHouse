#pragma once
#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Injects a SHUFFLE clause into the top-level SELECT query when no explicit ORDER BY (or SHUFFLE) is present.
 *  It ensures non-deterministic result ordering for tests, so that queries relying on implicit row order
 *  do not accidentally pass. Controlled by the setting `inject_random_order_for_select_without_order_by`.
 */
class InjectRandomOrderIfNoOrderByPass : public IQueryTreePass
{
public:
    String getName() override { return "InjectRandomOrderIfNoOrderBy"; }
    String getDescription() override { return "Inject SHUFFLE at top-level SELECT when there is no ORDER BY (tests only)"; }
    void run(QueryTreeNodePtr & root, ContextPtr context) override;
};
}
