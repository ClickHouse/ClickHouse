#pragma once

#include <sstream>
#include <Poco/Exception.h>
#include <mysqlxx/Types.h>


namespace mysqlxx
{
/// Common exception class for MySQL library. Functions code() and errnum() return error numbers from MySQL, for details see mysqld_error.h
struct Exception : public Poco::Exception
{
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    int errnum() const { return code(); }
    const char * name() const throw() override { return "mysqlxx::Exception"; }
    const char * className() const throw() override { return "mysqlxx::Exception"; }
};


/// Cannot connect to MySQL server
struct ConnectionFailed : public Exception
{
    ConnectionFailed(const std::string & msg, int code = 0) : Exception(msg, code) {}
    const char * name() const throw() override { return "mysqlxx::ConnectionFailed"; }
    const char * className() const throw() override { return "mysqlxx::ConnectionFailed"; }
};


/// Erroneous query.
struct BadQuery : public Exception
{
    BadQuery(const std::string & msg, int code = 0) : Exception(msg, code) {}
    const char * name() const throw() override { return "mysqlxx::BadQuery"; }
    const char * className() const throw() override { return "mysqlxx::BadQuery"; }
};


/// Value parsing failure
struct CannotParseValue : public Exception
{
    CannotParseValue(const std::string & msg, int code = 0) : Exception(msg, code) {}
    const char * name() const throw() override { return "mysqlxx::CannotParseValue"; }
    const char * className() const throw() override { return "mysqlxx::CannotParseValue"; }
};


std::string errorMessage(MYSQL * driver);

/// For internal need of library.
void checkError(MYSQL * driver);
void onError(MYSQL * driver);

}
