#include "config.h"

#if USE_SILK

#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/logger_useful.h>

#include <silk/util/logger.h>

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <string>
#include <string_view>

namespace silk
{

namespace
{

void writeLoggingFailureToStderr(const std::bad_alloc & logger_exception) noexcept
{
    (void)::write(STDERR_FILENO, static_cast<const void *>(MESSAGE_FOR_EXCEPTION_ON_LOGGING), sizeof(MESSAGE_FOR_EXCEPTION_ON_LOGGING));
    std::string_view logger_exception_message = logger_exception.what();
    (void)::write(STDERR_FILENO, static_cast<const void *>(logger_exception_message.data()), logger_exception_message.size());
}

void logImpl(LogLevel level, const char * file, int line, std::string_view format_string, std::string text)
{
    static Poco::Logger * const logger = getRawLogger("Silk");

    const auto [priority, logs_level] = [level]() -> std::pair<Poco::Message::Priority, DB::LogsLevel>
    {
        switch (level)
        {
            case LogLevel::DEBUG:
                return {Poco::Message::PRIO_DEBUG, DB::LogsLevel::debug};
            case LogLevel::INFO:
                return {Poco::Message::PRIO_INFORMATION, DB::LogsLevel::information};
            case LogLevel::WARN:
                return {Poco::Message::PRIO_WARNING, DB::LogsLevel::warning};
            case LogLevel::ERROR:
                return {Poco::Message::PRIO_ERROR, DB::LogsLevel::error};
        }
    }();

    PreformattedMessage message{std::move(text), format_string, {}};
    LOG_IMPL_AT(logger, logs_level, priority, file, "", line, message);
}

}

void Logger::log(LogLevel level, const char * file, int line, const char * message) noexcept
{
    MemoryTrackerBlockerInThread block_memory_tracker(VariableContext::Global);

    try
    {
        logImpl(level, file, line, message, message);
    }
    catch (const std::bad_alloc & logger_exception)
    {
        writeLoggingFailureToStderr(logger_exception);
    }
}

/// NOLINTNEXTLINE(cert-dcl50-cpp,modernize-avoid-variadic-functions)
void Logger::logf(LogLevel level, const char * file, int line, const char * fmt, ...) noexcept
{
    MemoryTrackerBlockerInThread block_memory_tracker(VariableContext::Global);

    va_list args;
    va_start(args, fmt);
    va_list args_copy;
    va_copy(args_copy, args);
    const int size = std::vsnprintf(nullptr, 0, fmt, args);
    va_end(args);

    try
    {
        std::string message(std::max(size, 0), '\0');
        std::vsnprintf(message.data(), message.size() + 1, fmt, args_copy); /// NOLINT(cert-err33-c)
        logImpl(level, file, line, fmt, std::move(message));
    }
    catch (const std::bad_alloc & logger_exception)
    {
        writeLoggingFailureToStderr(logger_exception);
    }
    va_end(args_copy);
}

}

#endif
