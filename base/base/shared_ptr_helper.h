#pragma once

#include <memory>
#include <type_traits>


/** Allows to make std::shared_ptr from T with protected constructor.
  *
  * Derive your T class from shared_ptr_helper<T> and add shared_ptr_helper<T> as a friend
  *  and you will have static 'create' method in your class.
  */
template <typename T>
struct shared_ptr_helper
{
private:
    // https://stackoverflow.com/a/25069711
    struct make_shared_enabler : T
    {
        template <typename... TArgs>
        make_shared_enabler(TArgs &&... args)
            : T{std::forward<TArgs>(args)...}
        {
        }
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<T> create(TArgs &&... args)
    {
        return std::make_shared<make_shared_enabler>(std::forward<TArgs>(args)...);
    }
};

template <typename T>
struct is_shared_ptr : std::false_type
{
};

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type
{
};

template <typename T>
inline constexpr bool is_shared_ptr_v = is_shared_ptr<T>::value;
