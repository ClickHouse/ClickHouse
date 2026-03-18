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
        ContextPtr context_,
        unsigned update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
        bool update_jemalloc_epoch_,
        bool update_rss_);

    ~KeeperAsynchronousMetrics() override;
private:
    ContextPtr context;

    void updateImpl(TimePoint update_time, TimePoint current_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values) override;
};


}
