#pragma once

#include <Common/AsynchronousMetrics.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context_fwd.h>

#include <optional>


namespace DB
{

class ServerAsynchronousMetrics : WithContext, public AsynchronousMetrics
{
public:
    ServerAsynchronousMetrics(
        ContextPtr global_context_,
        unsigned update_period_seconds,
        bool update_heavy_metrics_,
        unsigned heavy_metrics_update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
        bool update_jemalloc_epoch_,
        bool update_rss_);

    ~ServerAsynchronousMetrics() override;

private:
    void updateImpl(TimePoint update_time, TimePoint current_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values) override;
    void logImpl(AsynchronousMetricValues & new_values) override;

    bool update_heavy_metrics;
    const Duration heavy_metric_update_period;
    TimePoint heavy_metric_previous_update_time;
    double heavy_update_interval = 0.;

    struct DetachedPartsStats
    {
        size_t count;
        size_t detached_by_user;
    };

    struct MutationStats
    {
        /// For keeping track of the number of pending mutations that are over the maximum execution time
        /// which is controlled by the max_pending_mutations_execution_time_to_warn setting.
        size_t pending_mutations_over_execution_time;
        size_t pending_mutations;
    };

    DetachedPartsStats detached_parts_stats{};
    MutationStats mutation_stats{};

    /// /proc/self/smaps is walked at the slower heavy-metrics cadence rather
    /// than on every scrape, because it can be expensive on servers with many
    /// VMAs: the kernel walks page tables for every mapping to compute Rss,
    /// Pss, etc. Kept here (not in the base class) so the cost is gated.
    std::optional<ReadBufferFromFilePRead> vm_smaps;

    void updateMutationAndDetachedPartsStats();
    void updateThreadStackMetrics(AsynchronousMetricValues & new_values);
    void updateHeavyMetricsIfNeeded(TimePoint current_time, TimePoint update_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values);
};

}
