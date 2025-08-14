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
#include <cstddef>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include "Poco/Channel.h"
#include "Poco/Format.h"
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
/// Variants of these macros that allow message formatting with Poco::format()
/// are also available. Up to four arguments are supported.
///
/// Examples:
///     poco_warning(logger, "This is a warning");
///     poco_information_f2(logger, "An informational message with args: %d, %d", 1, 2);
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

    void fatal(const std::string & fmt, const Any & value1);
    void fatal(const std::string & fmt, const Any & value1, const Any & value2);
    void fatal(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void fatal(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void fatal(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void fatal(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void fatal(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void fatal(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void fatal(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void fatal(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

    void critical(const std::string & msg);
    /// If the Logger's log level is at least PRIO_CRITICAL,
    /// creates a Message with priority PRIO_CRITICAL
    /// and the given message text and sends it
    /// to the attached channel.

    void critical(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_CRITICAL,
    /// creates a Message with priority PRIO_CRITICAL
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void critical(const std::string & fmt, const Any & value1);
    void critical(const std::string & fmt, const Any & value1, const Any & value2);
    void critical(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void critical(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void
    critical(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void critical(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void critical(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void critical(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void critical(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void critical(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

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

    void error(const std::string & fmt, const Any & value1);
    void error(const std::string & fmt, const Any & value1, const Any & value2);
    void error(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void error(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void error(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void error(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void error(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void error(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void error(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void error(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

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

    void warning(const std::string & fmt, const Any & value1);
    void warning(const std::string & fmt, const Any & value1, const Any & value2);
    void warning(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void warning(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void
    warning(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void warning(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void warning(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void warning(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void warning(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void warning(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

    void notice(const std::string & msg);
    /// If the Logger's log level is at least PRIO_NOTICE,
    /// creates a Message with priority PRIO_NOTICE
    /// and the given message text and sends it
    /// to the attached channel.

    void notice(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_NOTICE,
    /// creates a Message with priority PRIO_NOTICE
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void notice(const std::string & fmt, const Any & value1);
    void notice(const std::string & fmt, const Any & value1, const Any & value2);
    void notice(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void notice(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void
    notice(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void notice(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void notice(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void notice(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void notice(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void notice(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

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

    void information(const std::string & fmt, const Any & value1);
    void information(const std::string & fmt, const Any & value1, const Any & value2);
    void information(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void information(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void information(
        const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void information(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void information(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void information(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void information(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void information(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

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

    void debug(const std::string & fmt, const Any & value1);
    void debug(const std::string & fmt, const Any & value1, const Any & value2);
    void debug(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void debug(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void debug(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void debug(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void debug(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void debug(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void debug(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void debug(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

    void trace(const std::string & msg);
    /// If the Logger's log level is at least PRIO_TRACE,
    /// creates a Message with priority PRIO_TRACE
    /// and the given message text and sends it
    /// to the attached channel.

    void trace(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_TRACE,
    /// creates a Message with priority PRIO_TRACE
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void trace(const std::string & fmt, const Any & value1);
    void trace(const std::string & fmt, const Any & value1, const Any & value2);
    void trace(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void trace(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void trace(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void trace(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void trace(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void trace(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void trace(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void trace(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);

    void test(const std::string & msg);
    /// If the Logger's log level is at least PRIO_TEST,
    /// creates a Message with priority PRIO_TEST
    /// and the given message text and sends it
    /// to the attached channel.

    void test(const std::string & msg, const char * file, int line);
    /// If the Logger's log level is at least PRIO_TEST,
    /// creates a Message with priority PRIO_TEST
    /// and the given message text and sends it
    /// to the attached channel.
    ///
    /// File must be a static string, such as the value of
    /// the __FILE__ macro. The string is not copied
    /// internally for performance reasons.

    void test(const std::string & fmt, const Any & value1);
    void test(const std::string & fmt, const Any & value1, const Any & value2);
    void test(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3);
    void test(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4);
    void test(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5);
    void test(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6);
    void test(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7);
    void test(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8);
    void test(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9);
    void test(
        const std::string & fmt,
        const Any & value1,
        const Any & value2,
        const Any & value3,
        const Any & value4,
        const Any & value5,
        const Any & value6,
        const Any & value7,
        const Any & value8,
        const Any & value9,
        const Any & value10);


    void dump(const std::string & msg, const void * buffer, std::size_t length, Message::Priority prio = Message::PRIO_DEBUG);
    /// Logs the given message, followed by the data in buffer.
    ///
    /// The data in buffer is written in canonical hex+ASCII form:
    /// Offset (4 bytes) in hexadecimal, followed by sixteen
    /// space-separated, two column, hexadecimal bytes,
    /// followed by the same sixteen bytes as ASCII characters.
    /// For bytes outside the range 32 .. 127, a dot is printed.

    bool is(int level) const;
    /// Returns true if at least the given log level is set.

    bool fatal() const;
    /// Returns true if the log level is at least PRIO_FATAL.

    bool critical() const;
    /// Returns true if the log level is at least PRIO_CRITICAL.

    bool error() const;
    /// Returns true if the log level is at least PRIO_ERROR.

    bool warning() const;
    /// Returns true if the log level is at least PRIO_WARNING.

    bool notice() const;
    /// Returns true if the log level is at least PRIO_NOTICE.

    bool information() const;
    /// Returns true if the log level is at least PRIO_INFORMATION.

    bool debug() const;
    /// Returns true if the log level is at least PRIO_DEBUG.

    bool trace() const;
    /// Returns true if the log level is at least PRIO_TRACE.

    bool test() const;
    /// Returns true if the log level is at least PRIO_TEST.

    static std::string format(const std::string & fmt, const std::string & arg);
    /// Replaces all occurrences of $0 in fmt with the string given in arg and
    /// returns the result. To include a dollar sign in the result string,
    /// specify two dollar signs ($$) in the format string.

    static std::string format(const std::string & fmt, const std::string & arg0, const std::string & arg1);
    /// Replaces all occurrences of $<n> in fmt with the string given in arg<n> and
    /// returns the result. To include a dollar sign in the result string,
    /// specify two dollar signs ($$) in the format string.

    static std::string format(const std::string & fmt, const std::string & arg0, const std::string & arg1, const std::string & arg2);
    /// Replaces all occurrences of $<n> in fmt with the string given in arg<n> and
    /// returns the result. To include a dollar sign in the result string,
    /// specify two dollar signs ($$) in the format string.

    static std::string
    format(const std::string & fmt, const std::string & arg0, const std::string & arg1, const std::string & arg2, const std::string & arg3);
    /// Replaces all occurrences of $<n> in fmt with the string given in arg<n> and
    /// returns the result. To include a dollar sign in the result string,
    /// specify two dollar signs ($$) in the format string.

    static void formatDump(std::string & message, const void * buffer, std::size_t length);
    /// Creates a hex-dump of the given buffer and appends it to the
    /// given message string.

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

protected:
    Logger(const std::string & name, Channel * pChannel, int level);
    ~Logger();

    void log(const std::string & text, Message::Priority prio);
    void log(const std::string & text, Message::Priority prio, const char * file, int line);

    static std::string format(const std::string & fmt, int argc, std::string argv[]);

private:
    static std::pair<Logger::LoggerMapIterator, bool> unsafeGet(const std::string & name, bool get_shared);
    static Logger * unsafeGetRawPtr(const std::string & name);
    static std::pair<LoggerMapIterator, bool> unsafeCreate(const std::string & name, Channel * pChannel, int level = Message::PRIO_INFORMATION);
    static Logger & parent(const std::string & name);
    static std::pair<LoggerMapIterator, bool> add(Logger * pLogger);
    static std::optional<LoggerMapIterator> find(const std::string & name);
    static Logger * findRawPtr(const std::string & name);

    Logger();
    Logger(const Logger &);
    Logger & operator=(const Logger &);

    std::string _name;
    Channel * _pChannel;
    std::atomic_int _level;
};


//
// convenience macros
//
#define poco_fatal(logger, msg) \
    if ((logger).fatal()) \
        (logger).fatal(msg, __FILE__, __LINE__); \
    else \
        (void)0

#define poco_fatal_f1(logger, fmt, arg1) \
    if ((logger).fatal()) \
        (logger).fatal(Poco::format((fmt), arg1), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_fatal_f2(logger, fmt, arg1, arg2) \
    if ((logger).fatal()) \
        (logger).fatal(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_fatal_f3(logger, fmt, arg1, arg2, arg3) \
    if ((logger).fatal()) \
        (logger).fatal(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_fatal_f4(logger, fmt, arg1, arg2, arg3, arg4) \
    if ((logger).fatal()) \
        (logger).fatal(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_critical(logger, msg) \
    if ((logger).critical()) \
        (logger).critical(msg, __FILE__, __LINE__); \
    else \
        (void)0

#define poco_critical_f1(logger, fmt, arg1) \
    if ((logger).critical()) \
        (logger).critical(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_critical_f2(logger, fmt, arg1, arg2) \
    if ((logger).critical()) \
        (logger).critical(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_critical_f3(logger, fmt, arg1, arg2, arg3) \
    if ((logger).critical()) \
        (logger).critical(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_critical_f4(logger, fmt, arg1, arg2, arg3, arg4) \
    if ((logger).critical()) \
        (logger).critical(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_error(logger, msg) \
    if ((logger).error()) \
        (logger).error(msg, __FILE__, __LINE__); \
    else \
        (void)0

#define poco_error_f1(logger, fmt, arg1) \
    if ((logger).error()) \
        (logger).error(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_error_f2(logger, fmt, arg1, arg2) \
    if ((logger).error()) \
        (logger).error(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_error_f3(logger, fmt, arg1, arg2, arg3) \
    if ((logger).error()) \
        (logger).error(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_error_f4(logger, fmt, arg1, arg2, arg3, arg4) \
    if ((logger).error()) \
        (logger).error(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_warning(logger, msg) \
    if ((logger).warning()) \
        (logger).warning(msg, __FILE__, __LINE__); \
    else \
        (void)0

#define poco_warning_f1(logger, fmt, arg1) \
    if ((logger).warning()) \
        (logger).warning(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_warning_f2(logger, fmt, arg1, arg2) \
    if ((logger).warning()) \
        (logger).warning(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_warning_f3(logger, fmt, arg1, arg2, arg3) \
    if ((logger).warning()) \
        (logger).warning(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_warning_f4(logger, fmt, arg1, arg2, arg3, arg4) \
    if ((logger).warning()) \
        (logger).warning(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_notice(logger, msg) \
    if ((logger).notice()) \
        (logger).notice(msg, __FILE__, __LINE__); \
    else \
        (void)0

#define poco_notice_f1(logger, fmt, arg1) \
    if ((logger).notice()) \
        (logger).notice(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_notice_f2(logger, fmt, arg1, arg2) \
    if ((logger).notice()) \
        (logger).notice(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_notice_f3(logger, fmt, arg1, arg2, arg3) \
    if ((logger).notice()) \
        (logger).notice(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_notice_f4(logger, fmt, arg1, arg2, arg3, arg4) \
    if ((logger).notice()) \
        (logger).notice(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_information(logger, msg) \
    if ((logger).information()) \
        (logger).information(msg, __FILE__, __LINE__); \
    else \
        (void)0

#define poco_information_f1(logger, fmt, arg1) \
    if ((logger).information()) \
        (logger).information(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_information_f2(logger, fmt, arg1, arg2) \
    if ((logger).information()) \
        (logger).information(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_information_f3(logger, fmt, arg1, arg2, arg3) \
    if ((logger).information()) \
        (logger).information(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
    else \
        (void)0

#define poco_information_f4(logger, fmt, arg1, arg2, arg3, arg4) \
    if ((logger).information()) \
        (logger).information(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
    else \
        (void)0

#if defined(_DEBUG) || defined(POCO_LOG_DEBUG)
#    define poco_debug(logger, msg) \
        if ((logger).debug()) \
            (logger).debug(msg, __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_debug_f1(logger, fmt, arg1) \
        if ((logger).debug()) \
            (logger).debug(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_debug_f2(logger, fmt, arg1, arg2) \
        if ((logger).debug()) \
            (logger).debug(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_debug_f3(logger, fmt, arg1, arg2, arg3) \
        if ((logger).debug()) \
            (logger).debug(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_debug_f4(logger, fmt, arg1, arg2, arg3, arg4) \
        if ((logger).debug()) \
            (logger).debug(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_trace(logger, msg) \
        if ((logger).trace()) \
            (logger).trace(msg, __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_trace_f1(logger, fmt, arg1) \
        if ((logger).trace()) \
            (logger).trace(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_trace_f2(logger, fmt, arg1, arg2) \
        if ((logger).trace()) \
            (logger).trace(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_trace_f3(logger, fmt, arg1, arg2, arg3) \
        if ((logger).trace()) \
            (logger).trace(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_trace_f4(logger, fmt, arg1, arg2, arg3, arg4) \
        if ((logger).trace()) \
            (logger).trace(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_test(logger, msg) \
        if ((logger).test()) \
            (logger).test(msg, __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_test_f1(logger, fmt, arg1) \
        if ((logger).test()) \
            (logger).test(Poco::format((fmt), (arg1)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_test_f2(logger, fmt, arg1, arg2) \
        if ((logger).test()) \
            (logger).test(Poco::format((fmt), (arg1), (arg2)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_test_f3(logger, fmt, arg1, arg2, arg3) \
        if ((logger).test()) \
            (logger).test(Poco::format((fmt), (arg1), (arg2), (arg3)), __FILE__, __LINE__); \
        else \
            (void)0

#    define poco_test_f4(logger, fmt, arg1, arg2, arg3, arg4) \
        if ((logger).test()) \
            (logger).test(Poco::format((fmt), (arg1), (arg2), (arg3), (arg4)), __FILE__, __LINE__); \
        else \
            (void)0
#else
#    define poco_debug(logger, msg)
#    define poco_debug_f1(logger, fmt, arg1)
#    define poco_debug_f2(logger, fmt, arg1, arg2)
#    define poco_debug_f3(logger, fmt, arg1, arg2, arg3)
#    define poco_debug_f4(logger, fmt, arg1, arg2, arg3, arg4)
#    define poco_trace(logger, msg)
#    define poco_trace_f1(logger, fmt, arg1)
#    define poco_trace_f2(logger, fmt, arg1, arg2)
#    define poco_trace_f3(logger, fmt, arg1, arg2, arg3)
#    define poco_trace_f4(logger, fmt, arg1, arg2, arg3, arg4)
#    define poco_test(logger, msg)
#    define poco_test_f1(logger, fmt, arg1)
#    define poco_test_f2(logger, fmt, arg1, arg2)
#    define poco_test_f3(logger, fmt, arg1, arg2, arg3)
#    define poco_test_f4(logger, fmt, arg1, arg2, arg3, arg4)
#endif


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


inline void Logger::fatal(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_FATAL);
}


inline void Logger::fatal(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_FATAL);
}


inline void Logger::fatal(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_FATAL);
}


inline void Logger::fatal(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_FATAL);
}


inline void
Logger::fatal(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_FATAL);
}


inline void Logger::fatal(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_FATAL);
}


inline void Logger::fatal(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_FATAL);
}


inline void Logger::fatal(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_FATAL);
}


inline void Logger::fatal(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_FATAL);
}


inline void Logger::fatal(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_FATAL);
}


inline void Logger::critical(const std::string & msg)
{
    log(msg, Message::PRIO_CRITICAL);
}


inline void Logger::critical(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_CRITICAL, file, line);
}


inline void Logger::critical(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_CRITICAL);
}


inline void Logger::critical(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_CRITICAL);
}


inline void Logger::critical(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_CRITICAL);
}


inline void Logger::critical(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_CRITICAL);
}


inline void Logger::critical(
    const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_CRITICAL);
}


inline void Logger::critical(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_CRITICAL);
}


inline void Logger::critical(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_CRITICAL);
}


inline void Logger::critical(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_CRITICAL);
}


inline void Logger::critical(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_CRITICAL);
}


inline void Logger::critical(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_CRITICAL);
}


inline void Logger::error(const std::string & msg)
{
    log(msg, Message::PRIO_ERROR);
}


inline void Logger::error(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_ERROR, file, line);
}


inline void Logger::error(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_ERROR);
}


inline void Logger::error(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_ERROR);
}


inline void Logger::error(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_ERROR);
}


inline void Logger::error(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_ERROR);
}


inline void
Logger::error(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_ERROR);
}


inline void Logger::error(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_ERROR);
}


inline void Logger::error(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_ERROR);
}


inline void Logger::error(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_ERROR);
}


inline void Logger::error(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_ERROR);
}


inline void Logger::error(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_ERROR);
}


inline void Logger::warning(const std::string & msg)
{
    log(msg, Message::PRIO_WARNING);
}


inline void Logger::warning(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_WARNING, file, line);
}


inline void Logger::warning(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_WARNING);
}


inline void Logger::warning(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_WARNING);
}


inline void Logger::warning(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_WARNING);
}


inline void Logger::warning(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_WARNING);
}


inline void
Logger::warning(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_WARNING);
}


inline void Logger::warning(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_WARNING);
}


inline void Logger::warning(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_WARNING);
}


inline void Logger::warning(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_WARNING);
}


inline void Logger::warning(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_WARNING);
}


inline void Logger::warning(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_WARNING);
}


inline void Logger::notice(const std::string & msg)
{
    log(msg, Message::PRIO_NOTICE);
}


inline void Logger::notice(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_NOTICE, file, line);
}


inline void Logger::notice(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_NOTICE);
}


inline void Logger::notice(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_NOTICE);
}


inline void Logger::notice(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_NOTICE);
}


inline void Logger::notice(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_NOTICE);
}


inline void
Logger::notice(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_NOTICE);
}


inline void Logger::notice(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_NOTICE);
}


inline void Logger::notice(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_NOTICE);
}


inline void Logger::notice(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_NOTICE);
}


inline void Logger::notice(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_NOTICE);
}


inline void Logger::notice(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_NOTICE);
}


inline void Logger::information(const std::string & msg)
{
    log(msg, Message::PRIO_INFORMATION);
}


inline void Logger::information(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_INFORMATION, file, line);
}


inline void Logger::information(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_INFORMATION);
}


inline void Logger::information(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_INFORMATION);
}


inline void Logger::information(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_INFORMATION);
}


inline void Logger::information(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_INFORMATION);
}


inline void Logger::information(
    const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_INFORMATION);
}


inline void Logger::information(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_INFORMATION);
}


inline void Logger::information(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_INFORMATION);
}


inline void Logger::information(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_INFORMATION);
}


inline void Logger::information(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_INFORMATION);
}


inline void Logger::information(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_INFORMATION);
}


inline void Logger::debug(const std::string & msg)
{
    log(msg, Message::PRIO_DEBUG);
}


inline void Logger::debug(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_DEBUG, file, line);
}


inline void Logger::debug(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_DEBUG);
}


inline void Logger::debug(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_DEBUG);
}


inline void Logger::debug(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_DEBUG);
}


inline void Logger::debug(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_DEBUG);
}


inline void
Logger::debug(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_DEBUG);
}


inline void Logger::debug(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_DEBUG);
}


inline void Logger::debug(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_DEBUG);
}


inline void Logger::debug(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_DEBUG);
}


inline void Logger::debug(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_DEBUG);
}


inline void Logger::debug(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_DEBUG);
}


inline void Logger::trace(const std::string & msg)
{
    log(msg, Message::PRIO_TRACE);
}


inline void Logger::trace(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_TRACE, file, line);
}


inline void Logger::trace(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_TRACE);
}


inline void Logger::trace(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_TRACE);
}


inline void Logger::trace(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_TRACE);
}


inline void Logger::trace(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_TRACE);
}


inline void
Logger::trace(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_TRACE);
}


inline void Logger::trace(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_TRACE);
}


inline void Logger::trace(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_TRACE);
}


inline void Logger::trace(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_TRACE);
}


inline void Logger::trace(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_TRACE);
}


inline void Logger::trace(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_TRACE);
}

inline void Logger::test(const std::string & msg)
{
    log(msg, Message::PRIO_TEST);
}


inline void Logger::test(const std::string & msg, const char * file, int line)
{
    log(msg, Message::PRIO_TEST, file, line);
}


inline void Logger::test(const std::string & fmt, const Any & value1)
{
    log(Poco::format(fmt, value1), Message::PRIO_TEST);
}


inline void Logger::test(const std::string & fmt, const Any & value1, const Any & value2)
{
    log(Poco::format(fmt, value1, value2), Message::PRIO_TEST);
}


inline void Logger::test(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3)
{
    log(Poco::format(fmt, value1, value2, value3), Message::PRIO_TEST);
}


inline void Logger::test(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4)
{
    log(Poco::format(fmt, value1, value2, value3, value4), Message::PRIO_TEST);
}


inline void
Logger::test(const std::string & fmt, const Any & value1, const Any & value2, const Any & value3, const Any & value4, const Any & value5)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5), Message::PRIO_TEST);
}


inline void Logger::test(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6), Message::PRIO_TEST);
}


inline void Logger::test(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7), Message::PRIO_TEST);
}


inline void Logger::test(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8), Message::PRIO_TEST);
}


inline void Logger::test(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9), Message::PRIO_TEST);
}


inline void Logger::test(
    const std::string & fmt,
    const Any & value1,
    const Any & value2,
    const Any & value3,
    const Any & value4,
    const Any & value5,
    const Any & value6,
    const Any & value7,
    const Any & value8,
    const Any & value9,
    const Any & value10)
{
    log(Poco::format(fmt, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10), Message::PRIO_TEST);
}


inline bool Logger::is(int level) const
{
    return _level >= level;
}


inline bool Logger::fatal() const
{
    return _level >= Message::PRIO_FATAL;
}


inline bool Logger::critical() const
{
    return _level >= Message::PRIO_CRITICAL;
}


inline bool Logger::error() const
{
    return _level >= Message::PRIO_ERROR;
}


inline bool Logger::warning() const
{
    return _level >= Message::PRIO_WARNING;
}


inline bool Logger::notice() const
{
    return _level >= Message::PRIO_NOTICE;
}


inline bool Logger::information() const
{
    return _level >= Message::PRIO_INFORMATION;
}


inline bool Logger::debug() const
{
    return _level >= Message::PRIO_DEBUG;
}


inline bool Logger::trace() const
{
    return _level >= Message::PRIO_TRACE;
}

inline bool Logger::test() const
{
    return _level >= Message::PRIO_TEST;
}


} // namespace Poco


#endif // Foundation_Logger_INCLUDED
