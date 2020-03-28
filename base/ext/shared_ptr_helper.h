#pragma once

#include <memory>

namespace ext
{

template <typename T>
struct is_shared_ptr
{
    static constexpr bool value = false;
};


template <typename T>
struct is_shared_ptr<std::shared_ptr<T>>
{
    static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;
}
