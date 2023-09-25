#pragma once

/// Construct an overloaded callable from multiple callables
/// See example here https://en.cppreference.com/w/cpp/utility/variant/visit
template <class... Ts>
struct Overloaded : Ts...
{
    using Ts::operator()...;
};

// explicit deduction guide
// https://en.cppreference.com/w/cpp/language/class_template_argument_deduction
template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;
