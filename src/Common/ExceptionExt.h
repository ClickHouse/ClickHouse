#pragma once

#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

template <class Logger>
void tryLogCurrentExceptionImpl(Logger logger, const std::string & start_of_message, LogsLevel level)
{
    try
    {
        PreformattedMessage message = getCurrentExceptionMessageAndPattern(true);
        if (start_of_message.empty())
        {
            switch (level)
            {
                case LogsLevel::none: break;
                case LogsLevel::test: LOG_TEST(std::move(logger), message); break;
                case LogsLevel::trace: LOG_TRACE(std::move(logger), message); break;
                case LogsLevel::debug: LOG_DEBUG(std::move(logger), message); break;
                case LogsLevel::information: LOG_INFO(std::move(logger), message); break;
                case LogsLevel::warning: LOG_WARNING(std::move(logger), message); break;
                case LogsLevel::error: LOG_ERROR(std::move(logger), message); break;
                case LogsLevel::fatal: LOG_FATAL(std::move(logger), message); break;
            }
        }
        else
        {
            switch (level)
            {
                case LogsLevel::none: break;
                case LogsLevel::test: LOG_TEST(std::move(logger), "{}: {}", start_of_message, message.text); break;
                case LogsLevel::trace: LOG_TRACE(std::move(logger), "{}: {}", start_of_message, message.text); break;
                case LogsLevel::debug: LOG_DEBUG(std::move(logger), "{}: {}", start_of_message, message.text); break;
                case LogsLevel::information: LOG_INFO(std::move(logger), "{}: {}", start_of_message, message.text); break;
                case LogsLevel::warning: LOG_WARNING(std::move(logger), "{}: {}", start_of_message, message.text); break;
                case LogsLevel::error: LOG_ERROR(std::move(logger), "{}: {}", start_of_message, message.text); break;
                case LogsLevel::fatal: LOG_FATAL(std::move(logger), "{}: {}", start_of_message, message.text); break;
            }
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    /// Mark the exception as logged.
    try
    {
        throw;
    }
    catch (Exception & e)
    {
        e.markAsLogged();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }
}

}
