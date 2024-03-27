#pragma once

#include <string>
#include <Common/StackTrace.h>


namespace Poco { namespace Util { class LayeredConfiguration; }}
class StackTrace;


/// \brief Sends crash reports to ClickHouse core developer team via https://sentry.io
///
/// This feature can enabled with "send_crash_reports.enabled" server setting,
/// in this case reports are sent only for official ClickHouse builds.
///
/// It is possible to send those reports to your own sentry account or account of consulting company you hired
/// by overriding "send_crash_reports.endpoint" setting. "send_crash_reports.debug" setting will allow to do that for
namespace SentryWriter
{
    void initialize(Poco::Util::LayeredConfiguration & config);
    void shutdown();

    using FramePointers = StackTrace::FramePointers;

    /// Not signal safe and can't be called from a signal handler
    /// @param sig_or_error - signal if >= 0, otherwise exception code
    void onFault(
        int sig_or_error,
        const std::string & error_message,
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size);
}
