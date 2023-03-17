#pragma once

#include <optional>
#include <string_view>
#include <unordered_set>
#include "State.h"

namespace DB
{

template <typename KeyStateHandler>
concept CInlineEscapingKeyStateHandler = requires(KeyStateHandler handler)
{
    { handler.wait(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readKeyValueDelimiter(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
};

template <typename KeyStateHandler>
concept CNoEscapingKeyStateHandler = requires(KeyStateHandler handler)
{
    { handler.wait(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::size_t {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::size_t {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
    { handler.readKeyValueDelimiter(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
};

template <typename T>
concept CKeyStateHandler = CInlineEscapingKeyStateHandler<T> || CNoEscapingKeyStateHandler<T>;

template <typename ValueStateHandler>
concept CInlineEscapingValueStateHandler = requires(ValueStateHandler handler)
{
    { handler.wait(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
    { handler.readEmpty(std::string_view {}, std::size_t {}, std::declval<std::string &>()) } -> std::same_as<NextState>;
};

template <typename ValueStateHandler>
concept CNoEscapingValueStateHandler = requires(ValueStateHandler handler)
{
    { handler.wait(std::string_view {}, std::size_t {}) } -> std::same_as<NextState>;
    { handler.read(std::string_view {}, std::size_t {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
    { handler.readQuoted(std::string_view {}, std::size_t {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
    { handler.readEmpty(std::string_view {}, std::size_t {}, std::declval<std::string_view &>()) } -> std::same_as<NextState>;
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

}
