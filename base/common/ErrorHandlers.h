#pragma once

#include <Poco/ErrorHandler.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>


/** ErrorHandler for Poco::Thread,
  *  that in case of unhandled exception,
  *  logs exception message and terminates the process.
  */
class KillingErrorHandler : public Poco::ErrorHandler
{
public:
    void exception(const Poco::Exception &) override { std::terminate(); }
    void exception(const std::exception &)  override { std::terminate(); }
    void exception()                        override { std::terminate(); }
};


/** Log exception message.
  */
class ServerErrorHandler : public Poco::ErrorHandler
{
public:
    void exception(const Poco::Exception &) override { logException(); }
    void exception(const std::exception &)  override { logException(); }
    void exception()                        override { logException(); }

private:
    Poco::Logger * log = &Poco::Logger::get("ServerErrorHandler");

    void logException()
    {
        DB::tryLogCurrentException(log);
    }
};
