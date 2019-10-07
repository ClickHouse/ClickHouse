#pragma once

#include <common/Types.h>

#include <boost/type_index.hpp>
#include <Poco/Logger.h>

#include <sstream>

class Logger {
public:
    enum Level: UInt8 {
        NONE   = 0,
        FATAL  = 1,  // TODO: should kill the server.
        ABORT  = 2,  // TODO: should call `abort()`.
        EXCEPT = 9,  // TODO: will replace tryLogCurrentException() - with inbound message as prefix.
        ERROR  = 10,
        WARN   = 20,
        INFO   = 30,
        DEBUG  = 40,
        TRACE  = 50,
    };

    Logger(UInt8 level_, const char * filename, const char * funcname, int linenum, Poco::Logger & log_);
    ~Logger();

    Logger(Logger &&) = default;

    Logger(const Logger &) = delete;
    Logger & operator=(const Logger &) = delete;

    template <class T>
    Logger & operator<<(const T & message)
    {
        stream << message;
        return *this;
    }

    Logger & operator<<(std::ostream& (*func)(std::ostream&));  // for |std::endl|

private:
    const UInt8 level;
    const char * file, * func;
    const int line;
    Poco::Logger & log;

    std::stringstream stream;
};

/// Use it as a base class to have a named logger
template <typename T = void>
class WithLogger {
protected:
    WithLogger() : log(Poco::Logger::get(boost::typeindex::type_id<T>().pretty_name())) { static_assert(!std::is_same_v<T, void>); }
    explicit WithLogger(const String & name) : log(Poco::Logger::get(name)) {}

    Logger getLogger(UInt8 level, const char * filename, const char * funcname, int linenum) const
    {
        return Logger(level, filename, funcname, linenum, log);
    }

private:
    Poco::Logger & log;
};

static inline Logger getLogger(UInt8 level, const char * filename, const char * funcname, int linenum)
{
    return Logger(level, filename, funcname, linenum, Poco::Logger::root());
}

#define LOG(LEVEL) (getLogger(Logger::LEVEL, __FILE__, __PRETTY_FUNCTION__, __LINE__))
