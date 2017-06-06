#pragma once

#include <memory>

namespace ext
{

/** Class AllocateShared allow to make std::shared_ptr<T> from T with private constructor.
  * Derive your T class from shared_ptr_helper<T>, define him as friend and call allocate_shared()/make_shared() method.
  */
template <typename T>
class shared_ptr_helper
{
protected:
    typedef typename std::remove_const<T>::type TNoConst;

    template <typename TAlloc>
    struct Deleter
    {
        void operator()(typename TAlloc::value_type * ptr)
        {
            using AllocTraits = std::allocator_traits<TAlloc>;
            ptr->~TNoConst();
            AllocTraits::deallocate(alloc, ptr, 1);
        }

        TAlloc alloc;
    };

    /// see std::allocate_shared
    template <typename TAlloc, typename... TArgs>
    static std::shared_ptr<T> allocate_shared(const TAlloc & alloc, TArgs &&... args)
    {
        using AllocTraits = std::allocator_traits<TAlloc>;
        TAlloc alloc_copy(alloc);

        auto ptr = AllocTraits::allocate(alloc_copy, 1);

        try
        {
            new (ptr) TNoConst(std::forward<TArgs>(args)...);
        }
        catch (...)
        {
            AllocTraits::deallocate(alloc_copy, ptr, 1);
            throw;
        }

        return std::shared_ptr<TNoConst>(
            ptr,
            Deleter<TAlloc>(),
            alloc_copy);
    }

    template <typename... TArgs>
    static std::shared_ptr<T> make_shared(TArgs &&... args)
    {
        return allocate_shared(std::allocator<TNoConst>(), std::forward<TArgs>(args)...);
    }

public:

    /// Default implementation of 'create' method just use make_shared.
    template <typename... TArgs>
    static std::shared_ptr<T> create(TArgs &&... args)
    {
        return make_shared(std::forward<TArgs>(args)...);
    }
};

}

