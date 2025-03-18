#pragma once

#include <utility>
#include <unistd.h>
#include <fmt/args.h>
#include <fmt/format.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Common/AtomicLogger.h>
#include <Common/Concepts.h>
#include <Common/CurrentThreadHelpers.h>
#include <Common/Logger.h>
#include <Common/QuillLogger.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/CurrentThread.h>
#include <Core/LogsLevel.h>
#include <Loggers/TextLogSink.h>
#include <Loggers/OwnPatternFormatter.h>

#define QUILL_DISABLE_NON_PREFIXED_MACROS 1

#include <quill/Logger.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>
#include <quill/LogMacros.h>

#define LogToStr(x, y) LogToStrImpl(x, y)
#define LogFrequencyLimiter(x, y) LogFrequencyLimiterImpl(x, y)

using LogSeriesLimiterPtr = std::shared_ptr<LogSeriesLimiter>;

namespace impl
{
    constexpr Poco::Message::Priority logLevelToPocoPriority(DB::LogsLevel log_level)
    {
        using enum DB::LogsLevel;
        switch (log_level)
        {
            case fatal: return Poco::Message::PRIO_FATAL;
            case error: return Poco::Message::PRIO_ERROR;
            case warning: return Poco::Message::PRIO_WARNING;
            case information: return Poco::Message::PRIO_INFORMATION;
            case debug: return Poco::Message::PRIO_DEBUG;
            case trace: return Poco::Message::PRIO_TRACE;
            case test: return Poco::Message::PRIO_TEST;
            case none: chassert(false, "Invalid log level 'none'");
        }

        std::unreachable();
    }

    constexpr quill::LogLevel logLevelToQuillLogLevel(DB::LogsLevel log_level)
    {
        using enum DB::LogsLevel;
        switch (log_level)
        {
            case fatal: return quill::LogLevel::Critical;
            case error: return quill::LogLevel::Error;
            case warning: return quill::LogLevel::Warning;
            case information: return quill::LogLevel::Info;
            case debug: return quill::LogLevel::Debug;
            case trace: return quill::LogLevel::TraceL1;
            case test: return quill::LogLevel::TraceL2;
            case none: return quill::LogLevel::None;
        }
    }

    /////////////////// getLoggerHelper ///////////////////
    [[maybe_unused]] inline LoggerPtr getLoggerHelper(const DB::AtomicLogger & logger) { return logger.load(); }
    [[maybe_unused]] inline LogSeriesLimiter * getLoggerHelper(LogSeriesLimiter & logger) { return &logger; }

    template <typename TLogger>
    requires (!DB::is_any_of<std::decay_t<TLogger>, DB::AtomicLogger>)
    inline TLogger getLoggerHelper(TLogger && logger)
    {
        return std::forward<TLogger>(logger);
    }
    /////////////////// getLoggerHelper ///////////////////

    /////////////////// getLoggerName ///////////////////
    template <typename TLogger>
    requires DB::is_any_of<TLogger, LoggerPtr, LoggerRawPtr, LogSeriesLimiter *, LogSeriesLimiterPtr>
    inline std::string_view getLoggerName(const TLogger & logger)
    {
        return logger->getName();
    }

    template <typename TLogger>
    requires DB::is_any_of<TLogger, LogToStrImpl, LogFrequencyLimiterImpl>
    inline std::string_view getLoggerName(const TLogger & logger)
    {
        return logger.getName();
    }

    [[maybe_unused]] inline std::string_view getLoggerName(DB::QuillLoggerPtr logger)
    {
        return logger->get_logger_name();
    }
    /////////////////// getLoggerName ///////////////////

    /////////////////// getQuillLogger ///////////////////
    template <typename TLogger>
    requires DB::is_any_of<TLogger, LoggerPtr, LoggerRawPtr>
    inline DB::QuillLoggerPtr getQuillLogger(const TLogger & logger)
    {
        return logger->getQuillLogger();
    }

    template <typename TLogger>
    requires DB::is_any_of<TLogger, LogToStrImpl, LogFrequencyLimiterImpl>
    inline DB::QuillLoggerPtr getQuillLogger(const TLogger & logger)
    {
        return getQuillLogger(logger.getLogger());
    }

    template <typename TLogger>
    requires DB::is_any_of<TLogger, LogSeriesLimiter *, LogSeriesLimiterPtr>
    inline DB::QuillLoggerPtr getQuillLogger(const TLogger & logger)
    {
        return getQuillLogger(logger->getLogger());
    }

    [[maybe_unused]] inline DB::QuillLoggerPtr getQuillLogger(DB::QuillLoggerPtr logger)
    {
        return logger;
    }
    /////////////////// getQuillLogger ///////////////////

    /////////////////// shouldLog ///////////////////
    [[maybe_unused]] inline bool shouldLog(DB::QuillLoggerPtr quill_logger, DB::LogsLevel level)
    {
        return quill_logger && quill_logger->should_log_statement(::impl::logLevelToQuillLogLevel(level));
    }

    template <typename TLogger>
    inline bool shouldLog(TLogger & logger, DB::LogsLevel level, Poco::Message * message)
    {
        if (!shouldLog(getQuillLogger(logger), level))
            return false;

        if constexpr (DB::is_any_of<TLogger, LogToStrImpl, LogFrequencyLimiterImpl>)
        {
            return !message || logger.shouldLogMessage(*message);
        }
        else if constexpr (DB::is_any_of<TLogger, LogSeriesLimiter *, LogSeriesLimiterPtr>)
        {
            return !message || logger->shouldLogMessage(*message);
        }
        else
        {
            return true;
        }
    }
    /////////////////// shouldLog ///////////////////
}

#define LOG_IMPL_FIRST_ARG(X, ...) X

/// Copy-paste from contrib/libpq/include/c.h
/// There's no easy way to count the number of arguments without evaluating these arguments...
#define CH_VA_ARGS_NARGS(...) \
    CH_VA_ARGS_NARGS_(__VA_ARGS__, \
                   63,62,61,60,                   \
                   59,58,57,56,55,54,53,52,51,50, \
                   49,48,47,46,45,44,43,42,41,40, \
                   39,38,37,36,35,34,33,32,31,30, \
                   29,28,27,26,25,24,23,22,21,20, \
                   19,18,17,16,15,14,13,12,11,10, \
                   9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define CH_VA_ARGS_NARGS_( \
    _01,_02,_03,_04,_05,_06,_07,_08,_09,_10, \
    _11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
    _21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
    _31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
    _41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
    _51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
    _61,_62,_63, N, ...) \
    (N)

#define LINE_NUM_AS_STRING_IMPL2(x) #x
#define LINE_NUM_AS_STRING_IMPL(x) LINE_NUM_AS_STRING_IMPL2(x)
#define LINE_NUM_AS_STRING LINE_NUM_AS_STRING_IMPL(__LINE__)
#define MESSAGE_FOR_EXCEPTION_ON_LOGGING "Failed to write a log message: " __FILE__ ":" LINE_NUM_AS_STRING "\n"

constexpr bool constexprContains(std::string_view haystack, std::string_view needle)
{
    return haystack.find(needle) != std::string_view::npos;
}

/// Logs a message to a specified logger with that level.
/// If more than one argument is provided,
///  the first argument is interpreted as a template with {}-substitutions
///  and the latter arguments are treated as values to substitute.
/// If only one argument is provided, it is treated as a message without substitutions.
#define LOG_IMPL(logger, level, ...)                                                                                                       \
    do                                                                                                                                     \
    {                                                                                                                                      \
        static_assert(!constexprContains(#__VA_ARGS__, "formatWithSecretsOneLine"), "Think twice!");                                       \
        static_assert(!constexprContains(#__VA_ARGS__, "formatWithSecretsMultiLine"), "Think twice!");                                     \
        if (!isLoggingEnabled())                                                                                                           \
            break;                                                                                                                         \
        auto _logger = ::impl::getLoggerHelper(logger);                                                                                    \
        const bool _is_clients_log = DB::currentThreadHasGroup() && DB::currentThreadLogsLevel() >= (level);                               \
        if (!_is_clients_log && !::impl::shouldLog(_logger, level, nullptr))                                                               \
            break;                                                                                                                         \
                                                                                                                                           \
        Stopwatch _logger_watch;                                                                                                           \
        try                                                                                                                                \
        {                                                                                                                                  \
            ProfileEvents::incrementForLogMessage(level);                                                                                  \
            constexpr size_t _nargs = CH_VA_ARGS_NARGS(__VA_ARGS__);                                                                       \
            using LogTypeInfo = FormatStringTypeInfo<std::decay_t<decltype(LOG_IMPL_FIRST_ARG(__VA_ARGS__))>>;                             \
                                                                                                                                           \
            std::string_view _format_string;                                                                                               \
            std::string _formatted_message;                                                                                                \
            std::vector<std::string> _format_string_args;                                                                                  \
                                                                                                                                           \
            if constexpr (LogTypeInfo::is_static)                                                                                          \
            {                                                                                                                              \
                formatStringCheckArgsNum(LOG_IMPL_FIRST_ARG(__VA_ARGS__), _nargs - 1);                                                     \
                _format_string = ConstexprIfsAreNotIfdefs<LogTypeInfo::is_static>::getStaticFormatString(LOG_IMPL_FIRST_ARG(__VA_ARGS__)); \
            }                                                                                                                              \
                                                                                                                                           \
            constexpr bool is_preformatted_message = !LogTypeInfo::is_static && LogTypeInfo::has_format;                                   \
            if constexpr (is_preformatted_message)                                                                                         \
            {                                                                                                                              \
                static_assert(_nargs == 1 || !is_preformatted_message);                                                                    \
                ConstexprIfsAreNotIfdefs<is_preformatted_message>::getPreformatted(LOG_IMPL_FIRST_ARG(__VA_ARGS__))                        \
                    .apply(_formatted_message, _format_string, _format_string_args);                                                       \
            }                                                                                                                              \
            else                                                                                                                           \
            {                                                                                                                              \
                _formatted_message = _nargs == 1                                                                                           \
                    ? firstArg(__VA_ARGS__)                                                                                                \
                    : ConstexprIfsAreNotIfdefs<!is_preformatted_message>::getArgsAndFormat(_format_string_args, __VA_ARGS__);              \
            }                                                                                                                              \
                                                                                                                                           \
            if (auto masker = DB::SensitiveDataMasker::getInstance())                                                                      \
                masker->wipeSensitiveData(_formatted_message);                                                                             \
                                                                                                                                           \
            std::string _file_function = __FILE__ "; ";                                                                                    \
            _file_function += __PRETTY_FUNCTION__;                                                                                         \
            auto _poco_log_level = ::impl::logLevelToPocoPriority(level);                                                                  \
            Poco::Message _poco_message(                                                                                                   \
                std::string{::impl::getLoggerName(_logger)},                                                                               \
                std::move(_formatted_message),                                                                                             \
                _poco_log_level,                                                                                                           \
                _file_function.c_str(),                                                                                                    \
                __LINE__,                                                                                                                  \
                _format_string,                                                                                                            \
                _format_string_args);                                                                                                      \
            DB::ExtendedLogMessage _msg_ext = DB::ExtendedLogMessage::getFrom(_poco_message);                                              \
            auto _should_log = ::impl::shouldLog(_logger, level, &_poco_message);                                                          \
            if (_should_log)                                                                                                               \
            {                                                                                                                              \
                std::string _text;                                                                                                         \
                if (auto * _formatter = Logger::getFormatter(); _formatter)                                                                \
                    _formatter->formatExtended(_msg_ext, _text);                                                                           \
                                                                                                                                           \
                QUILL_DEFINE_MACRO_METADATA(__PRETTY_FUNCTION__, "{}", nullptr, quill::LogLevel::Dynamic);                                 \
                ::impl::getQuillLogger(_logger)->template log_statement<QUILL_IMMEDIATE_FLUSH, true>(                                      \
                    ::impl::logLevelToQuillLogLevel(level), &macro_metadata, _text.empty() ? _msg_ext.base.getText() : _text);             \
            }                                                                                                                              \
            Logger::getTextLogSink().log(_msg_ext, _should_log);                                                                           \
        }                                                                                                                                  \
        catch (const Poco::Exception & logger_exception)                                                                                   \
        {                                                                                                                                  \
            ::write(STDERR_FILENO, static_cast<const void *>(MESSAGE_FOR_EXCEPTION_ON_LOGGING), sizeof(MESSAGE_FOR_EXCEPTION_ON_LOGGING)); \
            const std::string & logger_exception_message = logger_exception.message();                                                     \
            ::write(STDERR_FILENO, static_cast<const void *>(logger_exception_message.data()), logger_exception_message.size());           \
        }                                                                                                                                  \
        catch (const std::exception & logger_exception)                                                                                    \
        {                                                                                                                                  \
            ::write(STDERR_FILENO, static_cast<const void *>(MESSAGE_FOR_EXCEPTION_ON_LOGGING), sizeof(MESSAGE_FOR_EXCEPTION_ON_LOGGING)); \
            const char * logger_exception_message = logger_exception.what();                                                               \
            ::write(STDERR_FILENO, static_cast<const void *>(logger_exception_message), strlen(logger_exception_message));                 \
        }                                                                                                                                  \
        catch (...)                                                                                                                        \
        {                                                                                                                                  \
            ::write(STDERR_FILENO, static_cast<const void *>(MESSAGE_FOR_EXCEPTION_ON_LOGGING), sizeof(MESSAGE_FOR_EXCEPTION_ON_LOGGING)); \
        }                                                                                                                                  \
        ProfileEvents::incrementLoggerElapsedNanoseconds(_logger_watch.elapsedNanoseconds());                                              \
    } while (false)


#define LOG_TEST(logger, ...) LOG_IMPL(logger, DB::LogsLevel::test, __VA_ARGS__)
#define LOG_TRACE(logger, ...) LOG_IMPL(logger, DB::LogsLevel::trace, __VA_ARGS__)
#define LOG_DEBUG(logger, ...) LOG_IMPL(logger, DB::LogsLevel::debug, __VA_ARGS__)
#define LOG_INFO(logger, ...) LOG_IMPL(logger, DB::LogsLevel::information, __VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG_IMPL(logger, DB::LogsLevel::warning, __VA_ARGS__)
#define LOG_ERROR(logger, ...) LOG_IMPL(logger, DB::LogsLevel::error, __VA_ARGS__)
#define LOG_FATAL(logger, ...) LOG_IMPL(logger, DB::LogsLevel::fatal, __VA_ARGS__)
