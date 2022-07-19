#pragma once

#include <concepts>

namespace DB
{

template <typename... T>
concept OptionalArgument = requires(T &&...)
{
    requires(sizeof...(T) == 0 || sizeof...(T) == 1);
};

}
