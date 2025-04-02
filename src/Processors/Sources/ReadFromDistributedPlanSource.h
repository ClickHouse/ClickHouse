#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ISource.h>
#include <Core/Types_fwd.h>

namespace DB
{

/// Wraps distributed executor
class ReadFromDistributedPlanSource final : public ISource
{
public:
    ReadFromDistributedPlanSource(Block header_, const UUID & unique_query_id_, DistributedQueryPlan distributed_query_plan_)
        : ISource(std::move(header_))
        , unique_query_id(unique_query_id_)
        , distributed_query_plan(std::move(distributed_query_plan_))
    {
    }

    String getName() const override { return "ReadFromDistributedPlanSource"; }

private:
    Chunk generate() override;
    void onCancel() noexcept override;

    const UUID unique_query_id;
    const DistributedQueryPlan distributed_query_plan;

    bool started = false;
    std::shared_ptr<std::atomic<bool>> cancellation_flag = std::make_shared<std::atomic<bool>>(false);
};

}
