#pragma once

#include <Poco/ErrorHandler.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


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

    void logMessageImpl(Poco::Message::Priority priority, const std::string & msg) override
    {
        switch (priority)
        {
            case Poco::Message::PRIO_FATAL: [[fallthrough]];
            case Poco::Message::PRIO_CRITICAL:
                LOG_FATAL(trace_log, fmt::runtime(msg)); break;
            case Poco::Message::PRIO_ERROR:
                LOG_ERROR(trace_log, fmt::runtime(msg)); break;
            case Poco::Message::PRIO_WARNING:
                LOG_WARNING(trace_log, fmt::runtime(msg)); break;
            case Poco::Message::PRIO_NOTICE: [[fallthrough]];
            case Poco::Message::PRIO_INFORMATION:
                LOG_INFO(trace_log, fmt::runtime(msg)); break;
            case Poco::Message::PRIO_DEBUG:
                LOG_DEBUG(trace_log, fmt::runtime(msg)); break;
            case Poco::Message::PRIO_TRACE:
                LOG_TRACE(trace_log, fmt::runtime(msg)); break;
            case Poco::Message::PRIO_TEST:
                LOG_TEST(trace_log, fmt::runtime(msg)); break;
        }
    }

private:
    LoggerPtr log = getLogger("ServerErrorHandler");
    LoggerPtr trace_log = getLogger("Poco");

    void logException()
    {
        DB::tryLogCurrentException(log);
    }
};
