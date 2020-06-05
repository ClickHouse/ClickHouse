#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

const DataStream & IQueryPlanStep::getOutputStream() const
{
    if (!hasOutputStream())
        throw Exception("QueryPlanStep " + getName() + " does not have output stream.", ErrorCodes::LOGICAL_ERROR);

    return *output_stream;
}

}
