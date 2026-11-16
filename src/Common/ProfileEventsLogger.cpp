#include <Common/ProfileEventsLogger.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{

extern const Event LoggerElapsedNanoseconds; /// NOLINT(used here)
extern const Event LogTest; /// NOLINT(used here)
extern const Event LogTrace; /// NOLINT(used here)
extern const Event LogDebug; /// NOLINT(used here)
extern const Event LogInfo; /// NOLINT(used here)
extern const Event LogWarning; /// NOLINT(used here)
extern const Event LogError; /// NOLINT(used here)
extern const Event LogFatal; /// NOLINT(used here)

void incrementForLogMessage(Poco::Message::Priority priority)
{
    switch (priority)
    {
        case Poco::Message::PRIO_TEST: increment(LogTest); break;
        case Poco::Message::PRIO_TRACE: increment(LogTrace); break;
        case Poco::Message::PRIO_DEBUG: increment(LogDebug); break;
        case Poco::Message::PRIO_INFORMATION: increment(LogInfo); break;
        case Poco::Message::PRIO_WARNING: increment(LogWarning); break;
        case Poco::Message::PRIO_ERROR: increment(LogError); break;
        case Poco::Message::PRIO_FATAL: increment(LogFatal); break;
        default: break;
    }
}

void incrementLoggerElapsedNanoseconds(UInt64 ns)
{
    increment(LoggerElapsedNanoseconds, ns);
}

}
