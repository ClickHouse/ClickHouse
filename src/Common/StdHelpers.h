#pragma once

/// Helper for std::visit with multiple lambda overloads
/// Usage:
///   std::variant<int, std::string> v = 42;
///   auto result = std::visit(overloaded {
///       [](int i) { return std::to_string(i); },
///       [](const std::string& s) { return s; },
///       [](const auto& other) { return "unknown"; }
///   }, v);
template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; }; // NOLINT
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;
