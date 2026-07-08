#pragma once

#include <base/types.h>
#include <vector>

namespace DB
{

class IQueryPlanStep;
class IProcessor;

/// Rows entering a step from its child steps, by ordinal of the boundary-crossing input port.
/// second_port_rows is used for a merge (YShaped) join, whose single processor reads both the
/// left port (first_port_rows) and the right port (second_port_rows).
struct BoundaryInputRows
{
    UInt64 first_port_rows = 0;
    UInt64 second_port_rows = 0;
};

BoundaryInputRows boundaryInputRows(const IQueryPlanStep * step, const std::vector<IProcessor *> & processors);

}
