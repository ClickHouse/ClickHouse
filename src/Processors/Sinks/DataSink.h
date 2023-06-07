#pragma once

#include <QueryCoordination/PlanFragment.h>

namespace DB
{

class DataSink : public IQueryPlanStep
{
public:
    DataSink(UInt32 exchange_id) : exchange_plan_id(exchange_id) {}

    UInt32 getExchNodeId()
    {
        return exchange_plan_id;
    }

    DataPartition getOutputPartition()
    {
        return output_partition;
    }

    void setPartition(DataPartition partition)
    {
        output_partition = partition;
    }

private:
    UInt32 exchange_plan_id;
    DataPartition output_partition;
};

}
