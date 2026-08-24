#pragma once
#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Injects an ORDER BY rand() clause into the top-level SELECT query when no explicit ORDER BY is present.
 *  It ensures non-deterministic result ordering for tests, so that queries relying on implicit row order
 *  do not accidentally pass. Controlled by the setting `inject_random_order_for_select_without_order_by`.
 */
class InjectRandomOrderIfNoOrderByPass : public IQueryTreePass
{
public:
    String getName() override { return "InjectRandomOrderIfNoOrderBy"; }
    String getDescription() override { return "Inject ORDER BY rand() at top-level SELECT when missing (tests only)"; }
    void run(QueryTreeNodePtr & root, ContextPtr context) override;
};
}
