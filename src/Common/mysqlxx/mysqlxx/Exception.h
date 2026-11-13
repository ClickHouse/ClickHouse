#pragma once

#include <sstream>
#include <Poco/Exception.h>
#include <mysqlxx/Types.h>


namespace mysqlxx
{
/// Common exception class for MySQL library. Functions code() and errnum() return error numbers from MySQL, for details see mysqld_error.h
struct Exception : public Poco::Exception
{
    explicit Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    [[nodiscard]] int errnum() const { return code(); }
    [[nodiscard]] const char * name() const noexcept override { return "mysqlxx::Exception"; }
    [[nodiscard]] const char * className() const noexcept override { return "mysqlxx::Exception"; }

    [[nodiscard]] Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
};


/// Cannot connect to MySQL server
struct ConnectionFailed : public Exception
{
    explicit ConnectionFailed(const std::string & msg, int code = 0) : Exception(msg, code) {}
    [[nodiscard]] const char * name() const noexcept override { return "mysqlxx::ConnectionFailed"; }
    [[nodiscard]] const char * className() const noexcept override { return "mysqlxx::ConnectionFailed"; }

    [[nodiscard]] ConnectionFailed * clone() const override { return new ConnectionFailed(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
};


/// Connection to MySQL server was lost
struct ConnectionLost : public Exception
{
    explicit ConnectionLost(const std::string & msg, int code = 0) : Exception(msg, code) {}
    [[nodiscard]] const char * name() const noexcept override { return "mysqlxx::ConnectionLost"; }
    [[nodiscard]] const char * className() const noexcept override { return "mysqlxx::ConnectionLost"; }

    [[nodiscard]] ConnectionLost * clone() const override { return new ConnectionLost(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
};


/// Erroneous query.
struct BadQuery : public Exception
{
    explicit BadQuery(const std::string & msg, int code = 0) : Exception(msg, code) {}
    [[nodiscard]] const char * name() const noexcept override { return "mysqlxx::BadQuery"; }
    [[nodiscard]] const char * className() const noexcept override { return "mysqlxx::BadQuery"; }

    [[nodiscard]] BadQuery * clone() const override { return new BadQuery(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
};


/// Value parsing failure
struct CannotParseValue : public Exception
{
    explicit CannotParseValue(const std::string & msg, int code = 0) : Exception(msg, code) {}
    [[nodiscard]] const char * name() const noexcept override { return "mysqlxx::CannotParseValue"; }
    [[nodiscard]] const char * className() const noexcept override { return "mysqlxx::CannotParseValue"; }

    [[nodiscard]] CannotParseValue * clone() const override { return new CannotParseValue(*this); }
    void rethrow() const override { throw *this; } /// NOLINT(bugprone-exception-copy-constructor-throws,cert-err60-cpp)
};


std::string errorMessage(MYSQL * driver, const std::string & query = "");

/// For internal need of library.
void checkError(MYSQL * driver);
[[noreturn]] void onError(MYSQL * driver);

}
