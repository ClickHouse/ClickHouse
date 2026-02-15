#pragma once

#include <memory>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ISource.h>
#include <Core/Types_fwd.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

namespace DB
{

class TaskToHostMap;
using TaskToHostMapPtr = std::shared_ptr<const TaskToHostMap>;

/// Wraps distributed plan execution.
/// It is used in pipeline on distributed query initiator.
class ReadFromDistributedPlanSource final : public ISource
{
public:
    ReadFromDistributedPlanSource(
        SharedHeader header_,
        const UUID & unique_query_id_,
        DistributedQueryPlan distributed_query_plan_,
        TaskToHostMapPtr task_to_host_map_)
        : ISource(std::move(header_))
        , unique_query_id(unique_query_id_)
        , distributed_query_plan(std::move(distributed_query_plan_))
        , task_to_host_map(std::move(task_to_host_map_))
    {
    }

    String getName() const override { return "ReadFromDistributedPlanSource"; }

private:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() noexcept override;

    const UUID unique_query_id;
    const DistributedQueryPlan distributed_query_plan;
    TaskToHostMapPtr task_to_host_map;
    std::unique_ptr<DistributedQueryPlanExecutor> distributed_query_executor;

    bool started = false;
    std::shared_ptr<std::atomic<bool>> cancellation_flag = std::make_shared<std::atomic<bool>>(false);
};

}
