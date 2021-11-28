#pragma once

#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/CrashLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/ZooKeeperLog.h>


namespace DB
{

class IMergeTreeDataPart;

class AsynchronousMetricLog : public SystemLog<AsynchronousMetricLogElement>
{
public:
    using SystemLog<AsynchronousMetricLogElement>::SystemLog;

    void addValues(const AsynchronousMetricValues &);
};


class CrashLog : public SystemLog<CrashLogElement>
{
    using SystemLog<CrashLogElement>::SystemLog;
    friend void ::collectCrashLog(Int32, UInt64, const String &, const StackTrace &);

    static std::weak_ptr<CrashLog> crash_log;

public:
    static void initialize(std::shared_ptr<CrashLog> crash_log_) { crash_log = std::move(crash_log_); }
};


class MetricLog : public SystemLog<MetricLogElement>
{
    using SystemLog<MetricLogElement>::SystemLog;

public:
    void shutdown() override;

    /// Launches a background thread to collect metrics with interval
    void startCollectMetric(size_t collect_interval_milliseconds_);

    /// Stop background thread. Call before shutdown.
    void stopCollectMetric();

private:
    void metricThreadFunction();

    ThreadFromGlobalPool metric_flush_thread;
    size_t collect_interval_milliseconds;
    std::atomic<bool> is_shutdown_metric_thread{false};
};


// OpenTelemetry standardizes some Log data as well, so it's not just
// OpenTelemetryLog to avoid confusion.
class OpenTelemetrySpanLog : public SystemLog<OpenTelemetrySpanLogElement>
{
public:
    using SystemLog<OpenTelemetrySpanLogElement>::SystemLog;
};


class PartLog : public SystemLog<PartLogElement>
{
    using SystemLog<PartLogElement>::SystemLog;

    using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;

public:
    /// Add a record about creation of new part.
    static bool
    addNewPart(ContextPtr context, const MutableDataPartPtr & part, UInt64 elapsed_ns, const ExecutionStatus & execution_status = {});
    static bool
    addNewParts(ContextPtr context, const MutableDataPartsVector & parts, UInt64 elapsed_ns, const ExecutionStatus & execution_status = {});
};


class QueryLog : public SystemLog<QueryLogElement>
{
    using SystemLog<QueryLogElement>::SystemLog;
};


class QueryThreadLog : public SystemLog<QueryThreadLogElement>
{
    using SystemLog<QueryThreadLogElement>::SystemLog;
};


class QueryViewsLog : public SystemLog<QueryViewsLogElement>
{
    using SystemLog<QueryViewsLogElement>::SystemLog;
};


class SessionLog : public SystemLog<SessionLogElement>
{
    using SystemLog<SessionLogElement>::SystemLog;

public:
    void addLoginSuccess(const UUID & auth_id, std::optional<String> session_id, const Context & login_context);
    void addLoginFailure(const UUID & auth_id, const ClientInfo & info, const String & user, const Exception & reason);
    void addLogOut(const UUID & auth_id, const String & user, const ClientInfo & client_info);
};


class TextLog : public SystemLog<TextLogElement>
{
public:
    TextLog(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);
};


class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
};

class ZooKeeperLog : public SystemLog<ZooKeeperLogElement>
{
    using SystemLog<ZooKeeperLogElement>::SystemLog;
};


}
