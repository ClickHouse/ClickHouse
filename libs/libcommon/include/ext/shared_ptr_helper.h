#pragma once

#include <memory>

namespace ext
{

/** Allows to make std::shared_ptr from T with protected constructor.
  *
  * Derive your T class from shared_ptr_helper<T>
  *  and you will have static 'create' method in your class.
  *
  * Downsides:
  * - your class cannot be final;
  * - bad compilation error messages;
  * - bad code navigation.
  * - different dynamic type of created object, you cannot use typeid.
  */
template <typename T>
struct shared_ptr_helper
{
    template <typename... TArgs>
    static auto create(TArgs &&... args)
    {
        /** Local struct makes protected constructor to be accessible by std::make_shared function.
          * This trick is suggested by Yurii Diachenko,
          *  inspired by https://habrahabr.ru/company/mailru/blog/341584/
          *  that is translation of http://videocortex.io/2017/Bestiary/#-voldemort-types
          */
        struct Local : T
        {
            Local(TArgs &&... args) : T(std::forward<TArgs>(args)...) {}
        };

        return std::make_shared<Local>(std::forward<TArgs>(args)...);
    }
};

}
