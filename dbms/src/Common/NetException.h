#pragma once

#include <Common/Exception.h>


namespace DB
{

class NetException : public DB::Exception
{
public:
    NetException(const std::string & msg, int code = 0) : DB::Exception(msg, code) {}
    NetException(const std::string & msg, const std::string & arg, int code = 0) : DB::Exception(msg, arg, code) {}
    NetException(const std::string & msg, const DB::Exception & exc, int code = 0) : DB::Exception(msg, exc, code) {}

    explicit NetException(const DB::Exception & exc) : DB::Exception(exc) {}
    explicit NetException(const Poco::Exception & exc) : DB::Exception(exc.displayText()) {}

    const char * name() const throw() override { return "DB::NetException"; }
    const char * className() const throw() override { return "DB::NetException"; }
    DB::NetException * clone() const override { return new DB::NetException(*this); }
    void rethrow() const override { throw *this; }
};

}
