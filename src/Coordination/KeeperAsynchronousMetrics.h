#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/AsynchronousMetrics.h>

#include <base/types.h>

#include <optional>

namespace DB
{

class KeeperDispatcher;
void updateKeeperInformation(KeeperDispatcher & keeper_dispatcher, AsynchronousMetricValues & new_values);

/// Fills `KeeperOpenFileDescriptorCount` and `KeeperMaxFileDescriptorCount`.
/// An undetermined count is reported as `-1`: it is signed on purpose, so that the sentinel does not
/// wrap around to 2^64 - 1, which is indistinguishable from an unlimited `RLIMIT_NOFILE`.
/// Exposed separately from `updateKeeperInformation` to make that contract testable.
void setKeeperFileDescriptorMetrics(
    AsynchronousMetricValues & new_values, Int64 open_file_descriptor_count, std::optional<size_t> max_file_descriptor_count);

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
    void logImpl(AsynchronousMetricValues & new_values) override;
};


}
