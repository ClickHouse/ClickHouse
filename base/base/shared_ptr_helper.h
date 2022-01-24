#pragma once

#include <memory>


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
