#pragma once

#include <list>
#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class ExchangeDataReceiver final : public ISource, public std::enable_shared_from_this<ExchangeDataReceiver>
{
public:
    ExchangeDataReceiver(const DataStream & data_stream, UInt32 plan_id_) : ISource(data_stream.header), plan_id(plan_id_) { }

    ~ExchangeDataReceiver() override = default;

    void receive(Block & block)
    {
        // TODO lock
        block_list.push_back(block);
    }

    Status prepare() override;
    String getName() const override { return "ExchangeData"; }

    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr /*counter*/) override { }

    /// Stop reading from stream if output port is finished.
    void onUpdatePorts() override;

    int schedule() override { return 1; }

    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;

    Int32 getPlanId() const
    {
        return plan_id;
    }

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    std::list<Block> block_list;

    Int32 plan_id;
};

}
