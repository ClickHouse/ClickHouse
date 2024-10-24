#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <fmt/args.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <mutex>
#include <type_traits>
#include <unordered_map>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <base/EnumReflection.h>
#include <Common/Logger.h>

struct PreformattedMessage;
consteval void formatStringCheckArgsNumImpl(std::string_view str, size_t nargs);
template <typename T> constexpr std::string_view tryGetStaticFormatString(T && x);

[[maybe_unused]] inline void tryGetFormattedArgs(std::vector<std::string>&) {};
template <typename T, typename... Ts> [[maybe_unused]] inline void tryGetFormattedArgs(std::vector<std::string>&, T &&, Ts && ...);
template <typename... Args> inline std::string tryGetArgsAndFormat(std::vector<std::string>&, fmt::format_string<Args...>, Args && ...);

/// Extract format string from a string literal and constructs consteval fmt::format_string
template <typename... Args>
struct FormatStringHelperImpl
{
    std::string_view message_format_string;
    fmt::format_string<Args...> fmt_str;
    template<typename T>
    consteval FormatStringHelperImpl(T && str) : message_format_string(tryGetStaticFormatString(str)), fmt_str(std::forward<T>(str)) /// NOLINT
    {
        formatStringCheckArgsNumImpl(message_format_string, sizeof...(Args));
    }
    template<typename T>
    FormatStringHelperImpl(fmt::basic_runtime<T> && str) : fmt_str(std::forward<fmt::basic_runtime<T>>(str)) {} /// NOLINT

    PreformattedMessage format(Args && ...args) const;
};

template <typename... Args>
using FormatStringHelper = FormatStringHelperImpl<std::type_identity_t<Args>...>;

/// Saves a format string for already formatted message
struct PreformattedMessage
{
    std::string text;
    std::string_view format_string;
    std::vector<std::string> format_string_args;

    template <typename... Args>
    static PreformattedMessage create(FormatStringHelper<Args...> fmt, Args &&... args);

    operator const std::string & () const { return text; } /// NOLINT
    operator std::string () && { return std::move(text); } /// NOLINT
    operator fmt::format_string<> () const { UNREACHABLE(); } /// NOLINT

    void apply(std::string & out_text, std::string_view & out_format_string, std::vector<std::string> & out_format_string_args) const &
    {
        out_text = text;
        out_format_string = format_string;
        out_format_string_args = format_string_args;
    }
    void apply(std::string & out_text, std::string_view & out_format_string, std::vector<std::string> & out_format_string_args) &&
    {
        out_text = std::move(text);
        out_format_string = format_string;
        out_format_string_args = std::move(format_string_args);
    }
};

template <typename... Args>
PreformattedMessage FormatStringHelperImpl<Args...>::format(Args && ...args) const
{
    std::vector<std::string> out_format_string_args;
    std::string msg_text = tryGetArgsAndFormat(out_format_string_args, fmt_str, std::forward<Args>(args)...);
    return PreformattedMessage{.text = msg_text, .format_string = message_format_string, .format_string_args = out_format_string_args};
}

template <typename... Args>
PreformattedMessage PreformattedMessage::create(FormatStringHelper<Args...> fmt, Args && ...args)
{
    return fmt.format(std::forward<Args>(args)...);
}

template<typename T> struct is_fmt_runtime : std::false_type {};
template<typename T> struct is_fmt_runtime<fmt::basic_runtime<T>> : std::true_type {};

template <typename T> constexpr std::string_view tryGetStaticFormatString(T && x)
{
    /// Format string for an exception or log message must be a string literal (compile-time constant).
    /// Failure of this assertion may indicate one of the following issues:
    ///  - A message was already formatted into std::string before passing to Exception(...) or LOG_XXXXX(...).
    ///    Please use variadic constructor of Exception.
    ///    Consider using PreformattedMessage or LogToStr if you want to avoid double formatting and/or copy-paste.
    ///  - A string literal was converted to std::string (or const char *).
    ///  - Use Exception::createRuntime or fmt::runtime if there's no format string
    ///    and a message is generated in runtime by a third-party library
    ///    or deserialized from somewhere.
    static_assert(!std::is_same_v<std::string, std::decay_t<T>>);

    if constexpr (is_fmt_runtime<std::decay_t<T>>::value)
    {
        /// It definitely was fmt::runtime(something).
        /// We are not sure about a lifetime of the string, so return empty view.
        /// Also it can be arbitrary string, not a formatting pattern.
        /// So returning empty pattern will not pollute the set of patterns.
        return std::string_view();
    }
    else
    {
        if constexpr (std::is_same_v<PreformattedMessage, std::decay_t<T>>)
        {
            return x.format_string;
        }
        else
        {
            /// Most likely it was a string literal.
            /// Unfortunately, there's no good way to check if something is a string literal.
            /// But fmtlib requires a format string to be compile-time constant unless fmt::runtime is used.
            static_assert(std::is_nothrow_convertible_v<T, const char * const>);
            static_assert(!std::is_pointer_v<T>);
            return std::string_view(x);
        }
    }
}

template <typename T, typename... Ts> void tryGetFormattedArgs(std::vector<std::string>& out, T && x, Ts && ...rest)
{
    if constexpr (std::is_base_of_v<fmt::detail::view, std::decay_t<T>>)
        out.push_back(fmt::format("{}", std::remove_reference_t<T>(x)));
    else
        out.push_back(fmt::format("{}", std::forward<T>(x)));

    tryGetFormattedArgs(out, std::forward<Ts>(rest)...);
}

/// Constexpr ifs are not like ifdefs, and compiler still checks that unneeded code can be compiled
/// This template is useful to avoid compilation failures when condition of some "constexpr if" is false
template<bool enable> struct ConstexprIfsAreNotIfdefs
{
    template <typename T> constexpr static std::string_view getStaticFormatString(T &&) { return {}; }
    template <typename T> static PreformattedMessage getPreformatted(T &&) { return {}; }
    template <typename... Args> static std::string getArgsAndFormat(std::vector<std::string>&, fmt::format_string<Args...>, Args &&...) { return {}; }
};

template<> struct ConstexprIfsAreNotIfdefs<true>
{
    template <typename T> consteval static std::string_view getStaticFormatString(T && x)
    {
        /// See tryGetStaticFormatString(...)
        static_assert(!std::is_same_v<std::string, std::decay_t<T>>);
        static_assert(std::is_nothrow_convertible_v<T, const char * const>);
        static_assert(!std::is_pointer_v<T>);
        return std::string_view(x);
    }

    template <typename T> static T && getPreformatted(T && x) { return std::forward<T>(x); }

    template <typename... Args> static std::string getArgsAndFormat(std::vector<std::string>& out, fmt::format_string<Args...> fmt_str, Args && ...args)
    {
        return tryGetArgsAndFormat(out, std::move(fmt_str), std::forward<Args>(args)...);
    }
};

template <typename... Args> inline std::string tryGetArgsAndFormat(std::vector<std::string>& out, fmt::format_string<Args...> fmt_str, Args && ...args)
{
    tryGetFormattedArgs(out, args...);
    return fmt::format(fmt_str, std::forward<Args>(args)...);
}

template <typename... Ts> constexpr size_t numArgs(Ts &&...) { return sizeof...(Ts); }
template <typename T, typename... Ts> constexpr auto firstArg(T && x, Ts &&...) { return std::forward<T>(x); }
/// For implicit conversion of fmt::basic_runtime<> to char* for std::string ctor
template <typename T, typename... Ts> constexpr auto firstArg(fmt::basic_runtime<T> && data, Ts &&...) { return data.str.data(); }
template <typename T, typename... Ts> constexpr auto firstArg(const fmt::basic_runtime<T> & data, Ts &&...) { return data.str.data(); }

consteval ssize_t formatStringCountArgsNum(const char * const str, size_t len)
{
    /// It does not count named args, but we don't use them
    size_t cnt = 0;
    size_t i = 0;
    while (i + 1 < len)
    {
        if (str[i] == '{' && str[i + 1] == '}')
        {
            i += 2;
            cnt += 1;
        }
        else if (str[i] == '{')
        {
            /// Ignore checks for complex formatting like "{:.3f}"
            return -1;
        }
        else
        {
            i += 1;
        }
    }
    return cnt;
}

[[noreturn]] void functionThatFailsCompilationOfConstevalFunctions(const char * error);

/// fmt::format checks that there are enough arguments, but ignores extra arguments (e.g. fmt::format("{}", 1, 2) compiles)
/// This function will fail to compile if the number of "{}" substitutions does not exactly match
consteval void formatStringCheckArgsNumImpl(std::string_view str, size_t nargs)
{
    if (str.empty())
        return;
    ssize_t cnt = formatStringCountArgsNum(str.data(), str.size());
    if (0 <= cnt && cnt != nargs)
        functionThatFailsCompilationOfConstevalFunctions("unexpected number of arguments in a format string");
}

template<typename T>
consteval void formatStringCheckArgsNum(T && str, size_t nargs)
{
    formatStringCheckArgsNumImpl(tryGetStaticFormatString(str), nargs);
}
template<typename T> inline void formatStringCheckArgsNum(fmt::basic_runtime<T> &&, size_t) {}
template<> inline void formatStringCheckArgsNum(PreformattedMessage &, size_t) {}
template<> inline void formatStringCheckArgsNum(const PreformattedMessage &, size_t) {}
template<> inline void formatStringCheckArgsNum(PreformattedMessage &&, size_t) {}

template<typename T> struct FormatStringTypeInfo{ static constexpr bool is_static = true; static constexpr bool has_format = true; };
template<typename T> struct FormatStringTypeInfo<fmt::basic_runtime<T>> { static constexpr bool is_static = false; static constexpr bool has_format = false; };
template<> struct FormatStringTypeInfo<PreformattedMessage> { static constexpr bool is_static = false; static constexpr bool has_format = true; };

/// This wrapper helps to avoid too frequent and noisy log messages.
/// For each pair (logger_name, format_string) it remembers when such a message was logged the last time.
/// The message will not be logged again if less than min_interval_s seconds passed since the previously logged message.
class LogFrequencyLimiterImpl
{
    /// Hash(logger_name, format_string) -> (last_logged_time_s, skipped_messages_count)
    static std::unordered_map<UInt64, std::pair<time_t, size_t>> logged_messages;
    static time_t last_cleanup;
    static std::mutex mutex;

    LoggerPtr logger;
    time_t min_interval_s;
public:
    LogFrequencyLimiterImpl(LoggerPtr logger_, time_t min_interval_s_) : logger(std::move(logger_)), min_interval_s(min_interval_s_) {}

    LogFrequencyLimiterImpl & operator -> () { return *this; }
    bool is(Poco::Message::Priority priority) { return logger->is(priority); }
    LogFrequencyLimiterImpl * getChannel() {return this; }
    const String & name() const { return logger->name(); }

    void log(Poco::Message & message);

    /// Clears messages that were logged last time more than too_old_threshold_s seconds ago
    static void cleanup(time_t too_old_threshold_s = 600);

    LoggerPtr getLogger() { return logger; }
};

/// This wrapper helps to avoid too noisy log messages from similar objects.
/// Once an instance of LogSeriesLimiter type is created the decision is done
/// All followed messages which use this instance are either printed or muted altogether.
/// LogSeriesLimiter differs from LogFrequencyLimiterImpl in a way that
/// LogSeriesLimiter is useful for accept or mute series of logs when LogFrequencyLimiterImpl works for each line independently.
class LogSeriesLimiter
{
    static std::mutex mutex;
    static time_t last_cleanup;

    /// Hash(logger_name) -> (last_logged_time_s, accepted, muted)
    using SeriesRecords = std::unordered_map<UInt64, std::tuple<time_t, size_t, size_t>>;

    static SeriesRecords & getSeriesRecords() TSA_REQUIRES(mutex)
    {
        static SeriesRecords records;
        return records;
    }

    LoggerPtr logger = nullptr;
    bool accepted = false;
    String debug_message;
public:
    LogSeriesLimiter(LoggerPtr logger_, size_t allowed_count_, time_t interval_s_);

    LogSeriesLimiter & operator -> () { return *this; }
    bool is(Poco::Message::Priority priority) { return logger->is(priority); }
    LogSeriesLimiter * getChannel() {return this; }
    const String & name() const { return logger->name(); }

    void log(Poco::Message & message);

    LoggerPtr getLogger() { return logger; }
};

/// This wrapper is useful to save formatted message into a String before sending it to a logger
class LogToStrImpl
{
    String & out_str;
    LoggerPtr logger;
    std::unique_ptr<LogFrequencyLimiterImpl> maybe_nested;
    bool propagate_to_actual_log = true;
public:
    LogToStrImpl(String & out_str_, LoggerPtr logger_) : out_str(out_str_), logger(std::move(logger_)) {}
    LogToStrImpl(String & out_str_, std::unique_ptr<LogFrequencyLimiterImpl> && maybe_nested_)
        : out_str(out_str_), logger(maybe_nested_->getLogger()), maybe_nested(std::move(maybe_nested_)) {}
    LogToStrImpl & operator -> () { return *this; }
    bool is(Poco::Message::Priority priority) { propagate_to_actual_log &= logger->is(priority); return true; }
    LogToStrImpl * getChannel() {return this; }
    const String & name() const { return logger->name(); }
    void log(Poco::Message & message)
    {
        out_str = message.getText();
        if (!propagate_to_actual_log)
            return;
        if (maybe_nested)
            maybe_nested->log(message);
        else if (auto * channel = logger->getChannel())
            channel->log(message);
    }
};
