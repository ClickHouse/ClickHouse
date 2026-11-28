#pragma once

#include <memory>

namespace DB
{

class StatelessWorkerAllocation;
/// Held by the query context and by each of its distributed plans; the last holder releases the workers.
using StatelessWorkerAllocationPtr = std::shared_ptr<const StatelessWorkerAllocation>;

}
