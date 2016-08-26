#pragma once

#include <memory>

namespace ext
{

/**
 * Class AllocateShared allow to make std::shared_ptr<T> from T with private constructor.
 * Derive you T class from AllocateShared<T>, define him as friend and call allocate_shared()/make_shared() method.
**/
template <class T>
class share_ptr_helper
{
protected:
typedef typename std::remove_const<T>::type TNoConst;

	template <class TAlloc>
	struct Deleter
	{
	void operator()(typename TAlloc::value_type * ptr)
	{
		std::allocator_traits<TAlloc>::destroy(alloc, ptr);
	}
	TAlloc alloc;
	};

///see std::allocate_shared
template <class TAlloc, class ... TArgs>
static std::shared_ptr<T> allocate_shared(const TAlloc & alloc, TArgs && ... args)
{
	TAlloc alloc_copy(alloc);
	return std::shared_ptr<TNoConst>(new (std::allocator_traits<TAlloc>::allocate(alloc_copy, 1)) TNoConst(std::forward<TArgs>(args)...), Deleter<TAlloc>(), alloc_copy);
}

template <class ... TArgs>
static std::shared_ptr<T> make_shared(TArgs && ... args)
{
	return allocate_shared(std::allocator<TNoConst>(), std::forward<TArgs>(args)...);
}
};

}

