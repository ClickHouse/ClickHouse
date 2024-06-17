#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ErrorCodes.h>
#include <Common/ThreadPool_fwd.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <atomic>
#include <ctime>


namespace DB
{

/** ErrorLog is a log of error values measured at regular time interval.
  */

struct ErrorLogElement
{
    time_t event_time{};
    ErrorCodes::ErrorCode code{};
    ErrorCodes::Value value{};
    bool remote{};

    static std::string name() { return "ErrorLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


class ErrorLog : public SystemLog<ErrorLogElement>
{
    using SystemLog<ErrorLogElement>::SystemLog;

public:
    void shutdown() override;

    /// Launches a background thread to collect errors with interval
    void startCollectError(size_t collect_interval_milliseconds_);

    /// Stop background thread. Call before shutdown.
    void stopCollect();

private:
    struct PreviousValue {
        UInt64 local = 0;
        UInt64 remote = 0;
    };

    void threadFunction();

    std::unique_ptr<ThreadFromGlobalPool> flush_thread;
    size_t collect_interval_milliseconds;
    std::atomic<bool> is_shutdown_error_thread{false};
};

}
