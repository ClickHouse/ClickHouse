//
// Logger.h
//
// Library: Foundation
// Package: Logging
// Module:  Logger
//
// Definition of the Logger class.
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#ifndef Foundation_Logger_INCLUDED
#define Foundation_Logger_INCLUDED

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

#include "Poco/Channel.h"
#include "Poco/Foundation.h"
#include "Poco/Message.h"

namespace Poco
{

class Exception;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

class Foundation_API Logger : public Channel
/// Logger is a special Channel that acts as the main
/// entry point into the logging framework.
///
/// An application uses instances of the Logger class to generate its log messages
/// and send them on their way to their final destination. Logger instances
/// are organized in a hierarchical, tree-like manner and are maintained by
/// the framework. Every Logger object has exactly one direct ancestor, with
/// the exception of the root logger. A newly created logger inherits its properties -
/// channel and level - from its direct ancestor. Every logger is connected
/// to a channel, to which it passes on its messages. Furthermore, every logger
/// has a log level, which is used for filtering messages based on their priority.
/// Only messages with a priority equal to or higher than the specified level
/// are passed on. For example, if the level of a logger is set to three (PRIO_ERROR),
/// only messages with priority PRIO_ERROR, PRIO_CRITICAL and PRIO_FATAL will
/// propagate. If the level is set to zero, the logger is effectively disabled.
///
/// The name of a logger determines the logger's place within the logger hierarchy.
/// The name of the root logger is always "", the empty string. For all other
/// loggers, the name is made up of one or more components, separated by a period.
/// For example, the loggers with the name HTTPServer.RequestHandler and HTTPServer.Listener
/// are descendants of the logger HTTPServer, which itself is a descendant of
/// the root logger. There is not limit as to how deep
/// the logger hierarchy can become. Once a logger has been created and it has
/// inherited the channel and level from its ancestor, it loses the connection
/// to it. So changes to the level or channel of a logger do not affect its
/// descendants. This greatly simplifies the implementation of the framework
/// and is no real restriction, because almost always levels and channels are
/// set up at application startup and never changed afterwards. Nevertheless,
/// there are methods to simultaneously change the level and channel of all
/// loggers in a certain hierarchy.
///
/// There are also convenience macros available that wrap the actual
/// logging statement into a check whether the Logger's log level
/// is sufficient to actually log the message. This allows to increase
/// the application performance if many complex log statements
/// are used. The macros also add the source file path and line
/// number into the log message so that it is available to formatters.
/// Examples:
///     poco_warning(logger, "This is a warning");
{
public:
    const std::string & name() const;
    /// Returns the name of the logger, which is set as the
    /// message source on all messages created by the logger.

    void setChannel(Channel * pChannel);
    /// Attaches the given Channel to the Logger.

    Channel * getChannel() const;
    /// Returns the Channel attached to the logger.

    void setLevel(int level);
    /// Sets the Logger's log level.
    ///
    /// See Message::Priority for valid log levels.
    /// Setting the log level to zero turns off
    /// logging for that Logger.

    int getLevel() const;
    /// Returns the Logger's log level.

    void setLevel(const std::string & level);
    /// Sets the Logger's log level using a symbolic value.
    ///
    /// Valid values are:
    ///   - none (turns off logging)
    ///   - fatal
    ///   - critical
    ///   - error
    ///   - warning
    ///   - notice
    ///   - information
    ///   - debug
    ///   - trace

    void setProperty(const std::string & name, const std::string & value);
    /// Sets or changes a configuration property.
    ///
    /// Only the "channel" and "level" properties are supported, which allow
    /// setting the target channel and log level, respectively, via the LoggingRegistry.
    /// The "channel" and "level" properties are set-only.

    void log(const Message & msg);
    void log(Message && msg);
    /// Logs the given message if its priority is
    /// greater than or equal to the Logger's log level.

    void log(const Exception & exc);
    /// Logs the given exception with priority PRIO_ERROR.

    void log(const Exception & exc, const char * file, int line);
    /// Logs the given exception with priority PRIO_ERROR.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void fatal(const std::string & msg);
    /// If the Logger's log level is at least PRIO_FATAL,
    /// creates a Message with priority PRIO_FATAL
    /// and the given message text and sends it
    /// to the attached channel.

    void fatal(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_FATAL,
    /// creates a Message with priority PRIO_FATAL
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void error(const std::string & msg);
    /// If the Logger's log level is at least PRIO_ERROR,
    /// creates a Message with priority PRIO_ERROR
    /// and the given message text and sends it
    /// to the attached channel.

    void error(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_ERROR,
    /// creates a Message with priority PRIO_ERROR
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void warning(const std::string & msg);
    /// If the Logger's log level is at least PRIO_WARNING,
    /// creates a Message with priority PRIO_WARNING
    /// and the given message text and sends it
    /// to the attached channel.

    void warning(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_WARNING,
    /// creates a Message with priority PRIO_WARNING
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void information(const std::string & msg);
    /// If the Logger's log level is at least PRIO_INFORMATION,
    /// creates a Message with priority PRIO_INFORMATION
    /// and the given message text and sends it
    /// to the attached channel.

    void information(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_INFORMATION,
    /// creates a Message with priority PRIO_INFORMATION
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void debug(const std::string & msg);
    /// If the Logger's log level is at least PRIO_DEBUG,
    /// creates a Message with priority PRIO_DEBUG
    /// and the given message text and sends it
    /// to the attached channel.

    void debug(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_DEBUG,
    /// creates a Message with priority PRIO_DEBUG
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    bool is(int level) const;
    /// Returns true if at least the given log level is set.

    static void setLevel(const std::string & name, int level);
    /// Sets the given log level on all loggers that are
    /// descendants of the Logger with the given name.

    static void setChannel(const std::string & name, Channel * pChannel);
    /// Attaches the given Channel to all loggers that are
    /// descendants of the Logger with the given name.

    static void setProperty(const std::string & loggerName, const std::string & propertyName, const std::string & value);
    /// Sets or changes a configuration property for all loggers
    /// that are descendants of the Logger with the given name.

    static Logger & get(const std::string & name);
    /// Returns a reference to the Logger with the given name.
    /// If the Logger does not yet exist, it is created, based
    /// on its parent logger.

    static LoggerPtr getShared(const std::string & name, bool should_be_owned_by_shared_ptr_if_created = true);
    /// Returns a shared pointer to the Logger with the given name.
    /// If the Logger does not yet exist, it is created, based
    /// on its parent logger.

    static Logger & create(const std::string & name, Channel * pChannel, int level = Message::PRIO_INFORMATION);
    /// Creates and returns a reference to a Logger with the
    /// given name. The Logger's Channel and log level as set as
    /// specified.

    static LoggerPtr createShared(const std::string & name, Channel * pChannel, int level = Message::PRIO_INFORMATION);
    /// Creates and returns a shared pointer to a Logger with the
    /// given name. The Logger's Channel and log level as set as
    /// specified.

    static Logger & root();
    /// Returns a reference to the root logger, which is the ultimate
    /// ancestor of all Loggers.

    static Logger * has(const std::string & name);
    /// Returns a pointer to the Logger with the given name if it
    /// exists, or a null pointer otherwise.

    static void shutdown();
    /// Shuts down the logging framework and releases all
    /// Loggers.

    static void names(std::vector<std::string> & names);
    /// Fills the given vector with the names
    /// of all currently defined loggers.

    static int parseLevel(const std::string & level);
    /// Parses a symbolic log level from a string and
    /// returns the resulting numeric level.
    ///
    /// Valid symbolic levels are:
    ///   - none (turns off logging)
    ///   - fatal
    ///   - critical
    ///   - error
    ///   - warning
    ///   - notice
    ///   - information
    ///   - debug
    ///   - trace
    ///   - test
    ///
    /// The level is not case sensitive.

    static const std::string ROOT; /// The name of the root logger ("").

public:
    struct LoggerEntry
    {
        Poco::Logger * logger;
        bool owned_by_shared_ptr = false;
    };

    using LoggerMap = std::unordered_map<std::string, LoggerEntry>;
    using LoggerMapIterator = LoggerMap::iterator;

    Logger(const std::string & name, Channel * pChannel, int level);

protected:
    ~Logger();

    void log(const std::string & text, Message::Priority prio);
    void log(const std::string & text, Message::Priority prio, const char * file, int line);

private:
    Logger();
    Logger(const Logger &);
    Logger & operator=(const Logger &);

    std::string _name;
    Channel * _pChannel;
    std::atomic_int _level;
};

//
// inlines
//
inline const std::string & Logger::name() const
{
    return _name;
}

inline int Logger::getLevel() const
{
    return _level;
}

inline void Logger::log(const std::string & text, Message::Priority prio)
{
    if (_level >= prio && _pChannel)
    {
        _pChannel->log(Message(_name, text, prio));
    }
}

inline void Logger::log(const std::string & text, Message::Priority prio, const char * file, int line)
{
    if (_level >= prio && _pChannel)
    {
        _pChannel->log(Message(_name, text, prio, file, line));
    }
}

inline void Logger::fatal(const std::string & msg)
{
    log(msg, Message::PRIO_FATAL);
}

inline void Logger::fatal(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_FATAL, file, line);
}

inline void Logger::error(const std::string & msg)
{
    log(msg, Message::PRIO_ERROR);
}

inline void Logger::error(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_ERROR, file, line);
}

inline void Logger::warning(const std::string & msg)
{
    log(msg, Message::PRIO_WARNING);
}

inline void Logger::warning(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_WARNING, file, line);
}

inline void Logger::information(const std::string & msg)
{
    log(msg, Message::PRIO_INFORMATION);
}

inline void Logger::information(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_INFORMATION, file, line);
}

inline void Logger::debug(const std::string & msg)
{
    log(msg, Message::PRIO_DEBUG);
}

inline void Logger::debug(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_DEBUG, file, line);
}

inline bool Logger::is(int level) const
{
    return _level >= level;
}

} // namespace Poco

#endif // Foundation_Logger_INCLUDED
