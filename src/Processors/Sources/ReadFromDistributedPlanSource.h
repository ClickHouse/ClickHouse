#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ISource.h>

namespace DB
{

/// Wraps distributed executor
class ReadFromDistributedPlanSource final : public ISource
{
public:
    ReadFromDistributedPlanSource(Block header_, DistributedQueryPlan distributed_query_plan_)
        : ISource(std::move(header_))
        , distributed_query_plan(std::move(distributed_query_plan_))
    {
    }

    String getName() const override { return "ReadFromDistributedPlanSource"; }

private:
    Chunk generate() override;

    const DistributedQueryPlan distributed_query_plan;
};

}
