#pragma once

#include <memory>

namespace ext
{

/** Allows to make std::shared_ptr<T> from T with private constructor.
  * Derive your T class from shared_ptr_helper<T> and define him as friend.
  */
template <typename T>
struct shared_ptr_helper
{
    template <typename... TArgs>
    static auto create(TArgs &&... args)
    {
        struct Local : T { using T::T; };
        return std::make_shared<Local>(std::forward<TArgs>(args)...);
    }
};

}

