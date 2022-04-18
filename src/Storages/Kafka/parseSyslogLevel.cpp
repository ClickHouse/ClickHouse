#include "parseSyslogLevel.h"
#include <sys/syslog.h>

/// Must be in a separate compilation unit due to macros overlaps:
/// - syslog (LOG_DEBUG/...)
/// - logger_useful.h (LOG_DEBUG(...)/...)
std::pair<Poco::Message::Priority, DB::LogsLevel> parseSyslogLevel(const int level)
{
    using DB::LogsLevel;
    using Poco::Message;

    switch (level)
    {
        case LOG_EMERG: [[fallthrough]];
        case LOG_ALERT:   return {Message::PRIO_FATAL, LogsLevel::error};
        case LOG_CRIT:    return {Message::PRIO_CRITICAL, LogsLevel::error};
        case LOG_ERR:     return {Message::PRIO_ERROR, LogsLevel::error};
        case LOG_WARNING: return {Message::PRIO_WARNING, LogsLevel::warning};
        case LOG_NOTICE:  return {Message::PRIO_NOTICE, LogsLevel::information};
        case LOG_INFO:    return {Message::PRIO_INFORMATION, LogsLevel::information};
        case LOG_DEBUG:   return {Message::PRIO_DEBUG, LogsLevel::debug};
        default:
            return {Message::PRIO_TRACE, LogsLevel::trace};
    }
}
