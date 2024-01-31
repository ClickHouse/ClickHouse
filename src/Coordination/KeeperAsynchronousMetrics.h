#pragma once

#include <Interpreters/Context.h>
#include <Common/AsynchronousMetrics.h>

namespace DB
{

class KeeperDispatcher;
void updateKeeperInformation(KeeperDispatcher & keeper_dispatcher, AsynchronousMetricValues & new_values);

class KeeperAsynchronousMetrics : public AsynchronousMetrics
{
public:
    KeeperAsynchronousMetrics(
        ContextPtr context_, int update_period_seconds, const ProtocolServerMetricsFunc & protocol_server_metrics_func_);

private:
    ContextPtr context;

    void updateImpl(AsynchronousMetricValues & new_values, TimePoint update_time, TimePoint current_time) override;
};


}
