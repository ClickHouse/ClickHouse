#pragma once

#include <condition_variable>
#include <list>
#include <mutex>

#include <Core/Block.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

class ExchangeDataSource;
using ExchangeDataSourcePtr = std::shared_ptr<ExchangeDataSource>;

class ExchangeDataSource final : public ISource, public std::enable_shared_from_this<ExchangeDataSource>
{
public:
    ExchangeDataSource(const DataStream & data_stream, UInt32 fragment_id_, UInt32 plan_id_, const String & source_)
        : ISource(data_stream.header, false), fragment_id(fragment_id_), plan_id(plan_id_), source(source_)
    {
        /// default it's aggregate chunk. It cannot be judged based on the datatype is DataTypeAggregateFunction
        /// E.g select id,name from aaa_all group by id,name order by id,name SETTINGS allow_experimental_query_coordination = 1;
        add_aggregation_info = true;
    }

    ~ExchangeDataSource() override = default;

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
    void setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_) override;

    /// Stop reading from stream if output port is finished.
    void onUpdatePorts() override;

    int schedule() override { return 1; }

    UInt32 getPlanId() const { return plan_id; }

    String getSource() const { return source; }
    Block getHeader() const { return getPort().getHeader(); }

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    std::condition_variable cv;
    std::mutex mutex;

    BlocksList block_list;

    UInt32 fragment_id;
    UInt32 plan_id;

    String source;

    bool add_aggregation_info = false;
    size_t num_rows = 0;
};

}
