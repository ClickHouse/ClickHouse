#pragma once

#include <sstream>
#include <Poco/Exception.h>
#include <mysqlxx/Types.h>


namespace mysqlxx
{
/** Common exception that can be thrown by various library functions
 * code() and errnum() return MySQL error codes, see mysqld_error.h
  */
struct Exception : public Poco::Exception
{
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
    int errnum() const { return code(); }
    const char * name() const throw() override { return "mysqlxx::Exception"; }
    const char * className() const throw() override { return "mysqlxx::Exception"; }
};


/// Cant connect to the server
struct ConnectionFailed : public Exception
{
    ConnectionFailed(const std::string & msg, int code = 0) : Exception(msg, code) {}
    const char * name() const throw() override { return "mysqlxx::ConnectionFailed"; }
    const char * className() const throw() override { return "mysqlxx::ConnectionFailed"; }
};


/// Query contains error.
struct BadQuery : public Exception
{
    BadQuery(const std::string & msg, int code = 0) : Exception(msg, code) {}
    const char * name() const throw() override { return "mysqlxx::BadQuery"; }
    const char * className() const throw() override { return "mysqlxx::BadQuery"; }
};


/// Self descriptive.
struct CannotParseValue : public Exception
{
    CannotParseValue(const std::string & msg, int code = 0) : Exception(msg, code) {}
    const char * name() const throw() override { return "mysqlxx::CannotParseValue"; }
    const char * className() const throw() override { return "mysqlxx::CannotParseValue"; }
};


std::string errorMessage(MYSQL * driver);

/// Intended for internal library use.
void checkError(MYSQL * driver);
void onError(MYSQL * driver);

}
