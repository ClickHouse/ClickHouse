#pragma once

#include <memory>

namespace ext
{

/** Allows to make std::shared_ptr from T with protected constructor.
  *
  * Derive your T class from shared_ptr_helper<T> and add shared_ptr_helper<T> as a friend
  *  and you will have static 'create' method in your class.
  */
template <typename T>
struct shared_ptr_helper
{
    template <typename... TArgs>
    static std::shared_ptr<T> create(TArgs &&... args)
    {
        return std::shared_ptr<T>(new T(std::forward<TArgs>(args)...));
    }
};

}
