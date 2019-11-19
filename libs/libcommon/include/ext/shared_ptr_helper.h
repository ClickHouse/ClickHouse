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
class shared_ptr_helper
{
    struct CreateHelper : public T
    {
        template <typename ... Args>
        CreateHelper(Args && ... args) : T(std::forward<Args>(args) ...) {}
    };

public:
    template <typename ... Args>
    static std::shared_ptr<T> create(Args && ... args)
    {
        return std::make_shared<CreateHelper>(std::forward<Args>(args) ...);
    }
};

}
