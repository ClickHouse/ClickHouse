#pragma once

#include <string>
#include <Common/StackTrace.h>


namespace Poco { namespace Util { class LayeredConfiguration; }}


/// Sends crash reports and LOGICAL_ERRORs (if "send_logical_errors" is enabled)
/// to ClickHouse core developer team.
///
/// This feature can be enabled with "send_crash_reports.enabled" server setting.
///
/// It is possible to send those reports to your own endpoint
/// by overriding the "send_crash_reports.endpoint" setting.
class CrashWriter
{
public:
    using FramePointers = StackTrace::FramePointers;

    static void initialize(Poco::Util::LayeredConfiguration & config);
    static bool initialized();

    /// Can't be called from a signal handler. Call from a separate thread when a signal happens.
    static void onSignal(
        int sig,
        std::string_view error_message,
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size);

    static void onException(
        int code,
        std::string_view format_string,
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size);

private:
    explicit CrashWriter(Poco::Util::LayeredConfiguration & config);

    static std::unique_ptr<CrashWriter> instance;

    std::string endpoint;
    std::string server_data_path;

    enum Type
    {
        SIGNAL,
        EXCEPTION,
    };

    void sendError(
        Type type,
        int sig_or_error,
        std::string_view error_message,
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size);
};
