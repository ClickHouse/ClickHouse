#pragma once

#include <string>
#include <Common/StackTrace.h>


namespace Poco { namespace Util { class LayeredConfiguration; }}


/// \brief Sends crash reports and LOGICAL_ERRORs (if "send_logical_errors" is
/// enabled) to ClickHouse core developer team via https://sentry.io
///
/// This feature can enabled with "send_crash_reports.enabled" server setting,
/// in this case reports are sent only for official ClickHouse builds.
///
/// It is possible to send those reports to your own sentry account or account of consulting company you hired
/// by overriding "send_crash_reports.endpoint" setting. "send_crash_reports.debug" setting will allow to do that for
class SentryWriter
{
public:
    using FramePointers = StackTrace::FramePointers;

    /// Initialize static SentryWriter instance
    static void initializeInstance(Poco::Util::LayeredConfiguration & config);

    /// @return nullptr if initializeInstance() was not called (i.e. for non-server) or SentryWriter object
    static SentryWriter * getInstance();

    /// SentryWriter static instance should be reset explicitly to avoid
    /// possible use-after-free, since it may use some global objects (i.e.
    /// OpenSSL), while sending final statistics
    /// (SENTRY_SESSION_STATUS_EXITED).
    static void resetInstance();

    void onSignal(
        int sig,
        const std::string & error_message,
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size);

    void onException(
        int code,
        const std::string & error_message,
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size);

    ~SentryWriter();

private:
    static std::unique_ptr<SentryWriter> instance;
    bool initialized = false;
    bool anonymize = false;
    std::string server_data_path;

    explicit SentryWriter(Poco::Util::LayeredConfiguration & config);

    enum Type
    {
        SIGNAL,
        EXCEPTION,
    };

    /// Not signal safe and can't be called from a signal handler
    /// @param sig_or_error - signal if >= 0, otherwise exception code
    void sendError(
        Type type,
        int sig_or_error,
        const std::string & error_message,
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size);
};
