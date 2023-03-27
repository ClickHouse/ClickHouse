#pragma once

#include <optional>
#include <string_view>
#include <unordered_set>
#include "State.h"

#include <iostream>

namespace DB
{

template <typename KeyStateHandler>
concept CInlineEscapingKeyStateHandler = requires(KeyStateHandler handler)
{
    { handler.wait(std::string_view {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readKeyValueDelimiter(std::string_view {}) } -> std::same_as<NextState>;
};

template <typename KeyStateHandler>
concept CNoEscapingKeyStateHandler = requires(KeyStateHandler handler)
{
    { handler.wait(std::string_view {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
    { handler.readKeyValueDelimiter(std::string_view {}) } -> std::same_as<NextState>;
};

template <typename T>
concept CKeyStateHandler = CInlineEscapingKeyStateHandler<T> || CNoEscapingKeyStateHandler<T>;

template <typename ValueStateHandler>
concept CInlineEscapingValueStateHandler = requires(ValueStateHandler handler)
{
    { handler.wait(std::string_view {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
};

template <typename ValueStateHandler>
concept CNoEscapingValueStateHandler = requires(ValueStateHandler handler)
{
    { handler.wait(std::string_view {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
};

template <typename T>
concept CValueStateHandler = CInlineEscapingValueStateHandler<T> || CNoEscapingValueStateHandler<T>;

struct StateHandler
{
    StateHandler() = default;
    StateHandler(const StateHandler &) = default;

    virtual ~StateHandler() = default;

protected:
    [[nodiscard]] static std::string_view createElement(std::string_view file, std::size_t begin, std::size_t end);
};

template <typename T>
struct CustomQuoted
{
    const char * start_quote = "\"";
    const char * end_quote = "\"";

    const T & value;
};

template <typename T>
CustomQuoted<T> customQuote(const char * start_quote, const T & value, const char * end_quote = nullptr)
{
    assert(start_quote != nullptr);

    return CustomQuoted<T>{
        .start_quote = start_quote,
        .end_quote = end_quote ? end_quote : start_quote,
        .value = value
    };
}

template <typename T>
CustomQuoted<T> fancyQuote(const T & value)
{
    return CustomQuoted<T>{
        .start_quote = "«",
        .end_quote = "»",
        .value = value
    };
}

template <typename T>
std::ostream & operator<<(std::ostream & ostr, const CustomQuoted<T> & val)
{
    return ostr << val.start_quote << val.value << val.end_quote;
}

}
