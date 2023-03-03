#pragma once

#include <string>


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

    /// Not signal safe and can't be called from a signal handler
    void onFault(
        int sig,
        const std::string & error_message,
        const StackTrace & stack_trace);
};
