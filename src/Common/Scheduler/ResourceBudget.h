#pragma once

#include <Common/Scheduler/ResourceRequest.h>
#include <atomic>

namespace DB
{

/*
 * Helper class to keep track of requested and consumed amount of resource.
 * Useful if real amount of consumed resource can differ from requested amount of resource (e.g. in case of failures).
 * Can be safely used from multiple threads.
 * Usage example:
 *     ResourceBudget budget;
 *     while (!stop) {
 *         ResourceCost est_cost = myEstimateOfCostOrJustUseOne();
 *         myAllocateResource(budget.ask(est_cost)); // Ask external system to allocate resource for you
 *         ResourceCost real_cost = mySynchronousConsumptionOfResource(); // Real consumption can differ from est_cost
 *         budget.adjust(est_cost, real_cost); // Adjust balance according to the actual cost, may affect the next iteration
 *     }
 */
class ResourceBudget
{
public:
    // Returns amount of resource to be requested according to current balance and estimated cost of new consumption
    ResourceCost ask(ResourceCost estimated_cost)
    {
        ResourceCost budget = available.load();
        while (true)
        {
            // Valid resource request must have positive `cost`. Also takes consumption history into account.
            ResourceCost cost = std::max<ResourceCost>(1ll, estimated_cost - budget);

            // Assume every request is satisfied (no resource request cancellation is possible now)
            // So we requested additional `cost` units and are going to consume `estimated_cost`
            ResourceCost new_budget = budget + cost - estimated_cost;

            // Try to commit this transaction
            if (new_budget == budget || available.compare_exchange_strong(budget, new_budget))
                return cost;
        }
    }

    // Should be called to account for difference between real and estimated costs
    // Optional. May be skipped if `real_cost` is known in advance (equals `estimated_cost`).
    void adjust(ResourceCost estimated_cost, ResourceCost real_cost)
    {
        available.fetch_add(estimated_cost - real_cost);
    }

    ResourceCost get() const
    {
        return available.load();
    }

private:
    std::atomic<ResourceCost> available = 0; // requested - consumed
};

}
