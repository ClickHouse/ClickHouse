#include <Processors/QueryPlan/StepPortStats.h>

#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <base/defines.h>

namespace DB
{

BoundaryInputRows boundaryInputRows(const IQueryPlanStep * step, const std::vector<IProcessor *> & processors)
{
    BoundaryInputRows result;

    for (const auto * processor : processors)
    {
        size_t boundary_port_index = 0;

        for (const auto & input_port : processor->getInputs())
        {
            if (!input_port.isConnected() || input_port.getOutputPort().getProcessor().getQueryPlanStep() == step)
                continue;

            const UInt64 rows = processor->getPortDataCounters(input_port).rows;
            if (boundary_port_index == 0)
                result.first_port_rows += rows;
            else
            {
                chassert(boundary_port_index == 1); /// a join reads from at most two sources
                result.second_port_rows += rows;
            }
            ++boundary_port_index;
        }
    }

    return result;
}

}
