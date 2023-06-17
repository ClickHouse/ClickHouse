#pragma once

#include <list>
#include <mutex>
#include <condition_variable>

#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

class ExchangeDataReceiver final : public ISource, public std::enable_shared_from_this<ExchangeDataReceiver>
{
public:
    ExchangeDataReceiver(const DataStream & data_stream, UInt32 plan_id_, const String & source_)
        : ISource(data_stream.header), plan_id(plan_id_), source(source_)
    {
        const auto & sample = getPort().getHeader();
        for (auto & type : sample.getDataTypes())
            if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
                add_aggregation_info = true;
    }

    ~ExchangeDataReceiver() override = default;

    void receive(Block block)
    {
        {
            std::unique_lock lk(mutex);
            block_list.push_back(std::move(block));
        }
        cv.notify_one();
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

    String getSource() const
    {
        return source;
    }

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    std::condition_variable cv;
    std::mutex mutex;

    BlocksList block_list;

    Int32 plan_id;

    String source;

    bool add_aggregation_info = false;

    size_t num_rows = 0;
};

}
