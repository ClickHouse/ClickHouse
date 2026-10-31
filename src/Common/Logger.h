#pragma once

#include <base/defines.h>

#include <algorithm>
#include <memory>
#include <string>

#include <Poco/Logger.h>
#include <Poco/Message.h>

namespace Poco
{
class Channel;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
}

using LoggerPtr = Poco::LoggerPtr;
using LoggerRawPtr = Poco::Logger *;

/** RAII wrappers around Poco/Logger.h.
  *
  * You should use this functions in case Logger instance lifetime needs to be properly
  * managed, because otherwise it will leak memory.
  *
  * For example when Logger is created when table is created and Logger contains table name.
  * Then it must be destroyed when underlying table is destroyed.
  */

/** Get Logger with specified name. If the Logger does not exist, it is created.
  * Logger is destroyed, when last shared ptr that refers to Logger with specified name is destroyed.
  */
LoggerPtr getLogger(const std::string & name);

/** Get Logger with specified name. If the Logger does not exist, it is created.
  * This overload was added for specific purpose, when logger is constructed from constexpr string.
  * Logger is destroyed only during program shutdown.
  */
template <size_t n>
ALWAYS_INLINE LoggerPtr getLogger(const char (&name)[n])
{
    return Poco::Logger::getShared(name, false /*should_be_owned_by_shared_ptr_if_created*/);
}

/** Create Logger with specified name, channel and logging level.
  * If Logger already exists, throws exception.
  * Logger is destroyed, when last shared ptr that refers to Logger with specified name is destroyed.
  */
LoggerPtr createLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level = Poco::Message::PRIO_INFORMATION);

/** Create raw Poco::Logger that will not be destroyed before program termination.
  * This can be used in cases when specific Logger instance can be singletone.
  *
  * For example you need to pass Logger into low-level libraries as raw pointer, and using
  * RAII wrapper is inconvenient.
  *
  * Generally you should always use getLogger functions.
  */

LoggerRawPtr getRawLogger(const std::string & name);

LoggerRawPtr createRawLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level = Poco::Message::PRIO_INFORMATION);

/** Returns true, if currently Logger with specified name is created.
  * Otherwise, returns false.
  */
bool hasLogger(const std::string & name);

/// Escape a free-form audit field (a username, an object name, or the query text) so that a
/// single audit record both occupies exactly one physical log line AND remains unambiguously
/// parseable. An audit record is a list of comma-separated fields, so a literal comma inside a
/// field would otherwise be indistinguishable from a field separator; a backslash is the escape
/// character and must therefore be escaped first. `CR`/`LF` become the two-character sequences
/// `\r`/`\n` so the record never spans multiple physical lines. A downstream parser can split on
/// an unescaped comma and reverse the escaping.
inline std::string escapeForAuditField(const std::string & s)
{
    std::string result;
    result.reserve(s.size());
    for (char c : s)
    {
        switch (c)
        {
            case '\\': result += "\\\\"; break;
            case ',':  result += "\\,";  break;
            case '\n': result += "\\n";  break;
            case '\r': result += "\\r";  break;
            default:   result += c;      break;
        }
    }
    return result;
}
