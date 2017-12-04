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
    void exception(const Poco::Exception &) { std::terminate(); }
    void exception(const std::exception &)  { std::terminate(); }
    void exception()                        { std::terminate(); }
};


/** Log exception message.
  */
class ServerErrorHandler : public Poco::ErrorHandler
{
public:
    void exception(const Poco::Exception &) { logException(); }
    void exception(const std::exception &)  { logException(); }
    void exception()                        { logException(); }

private:
    Logger * log = &Logger::get("ServerErrorHandler");

    void logException()
    {
        DB::tryLogCurrentException(log);
    }
};
