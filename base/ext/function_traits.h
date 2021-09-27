#pragma once

#include <tuple>
#include <type_traits>


template <typename T>
struct function_traits;

template <typename ReturnType, typename... Args>
struct function_traits<ReturnType(Args...)>
{
    using result = ReturnType;
    using arguments = std::tuple<Args...>;
    using arguments_decay = std::tuple<typename std::decay<Args>::type...>;
};
