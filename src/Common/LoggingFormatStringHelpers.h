#pragma once
#include <base/defines.h>
#include <fmt/format.h>

/// Saves a format string for already formatted message
struct PreformattedMessage
{
    String message;
    std::string_view format_string;

    operator const String & () const { return message; }
    operator String () && { return std::move(message); }
    operator fmt::format_string<> () const { UNREACHABLE(); }
};

template<typename T> struct is_fmt_runtime : std::false_type {};
template<typename T> struct is_fmt_runtime<fmt::basic_runtime<T>> : std::true_type {};

template <typename T> constexpr std::string_view tryGetStaticFormatString(T && x)
{
    /// Failure of this asserting indicates that something went wrong during type deduction.
    /// For example, a string literal was implicitly converted to std::string. It should not happen.
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
            static_assert(std::is_nothrow_convertible<T, const char * const>::value);
            static_assert(!std::is_pointer<T>::value);
            return std::string_view(x);
        }
    }
}

template <typename... Ts> constexpr size_t numArgs(Ts &&...) { return sizeof...(Ts); }
template <typename T, typename... Ts> constexpr auto firstArg(T && x, Ts &&...) { return std::forward<T>(x); }
/// For implicit conversion of fmt::basic_runtime<> to char* for std::string ctor
template <typename T, typename... Ts> constexpr auto firstArg(fmt::basic_runtime<T> && data, Ts &&...) { return data.str.data(); }

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

[[noreturn]] void functionThatFailsCompilationOfConstevalFunctions(const char * error)
{
    throw std::runtime_error(error);
}

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

template <typename... Args>
struct CheckArgsNumHelperImpl
{
    //std::enable_if_t<std::is_same_v<std::decay_t<T>, PreformattedMessage>>
    template<typename T>
    consteval CheckArgsNumHelperImpl(T && str)
    {
        formatStringCheckArgsNumImpl(tryGetStaticFormatString(str), sizeof...(Args));
    }

    /// No checks for fmt::runtime and PreformattedMessage
    template<typename T> CheckArgsNumHelperImpl(fmt::basic_runtime<T> &&) {}
    template<> CheckArgsNumHelperImpl(PreformattedMessage &) {}
    template<> CheckArgsNumHelperImpl(const PreformattedMessage &) {}
    template<> CheckArgsNumHelperImpl(PreformattedMessage &&) {}

};

template <typename... Args> using CheckArgsNumHelper = CheckArgsNumHelperImpl<std::type_identity_t<Args>...>;
template <typename... Args> void formatStringCheckArgsNum(CheckArgsNumHelper<Args...>, Args &&...) {}
