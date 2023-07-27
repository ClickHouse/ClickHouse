#pragma once

#include <Coordination/TinyContext.h>
#include <Common/AsynchronousMetrics.h>

namespace DB
{

class KeeperDispatcher;
void updateKeeperInformation(KeeperDispatcher & keeper_dispatcher, AsynchronousMetricValues & new_values);

class KeeperAsynchronousMetrics : public AsynchronousMetrics
{
public:
    KeeperAsynchronousMetrics(
        TinyContextPtr tiny_context_, int update_period_seconds, const ProtocolServerMetricsFunc & protocol_server_metrics_func_);

private:
    TinyContextPtr tiny_context;

    void updateImpl(AsynchronousMetricValues & new_values, TimePoint update_time, TimePoint current_time) override;
};


}
