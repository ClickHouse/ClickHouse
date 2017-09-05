/*
Copyright 2012-2017 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under the Boost Software License, Version 1.0.
(http://www.boost.org/LICENSE_1_0.txt)
*/
#ifndef BOOST_SMART_PTR_ALLOCATE_SHARED_ARRAY_HPP
#define BOOST_SMART_PTR_ALLOCATE_SHARED_ARRAY_HPP

#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/type_traits/alignment_of.hpp>
#include <boost/type_traits/has_trivial_assign.hpp>
#include <boost/type_traits/has_trivial_constructor.hpp>
#include <boost/type_traits/has_trivial_destructor.hpp>
#include <boost/type_traits/type_with_alignment.hpp>

namespace boost {
namespace detail {

template<class>
struct sp_if_array { };

template<class T>
struct sp_if_array<T[]> {
    typedef boost::shared_ptr<T[]> type;
};

template<class>
struct sp_if_size_array { };

template<class T, std::size_t N>
struct sp_if_size_array<T[N]> {
    typedef boost::shared_ptr<T[N]> type;
};

template<class>
struct sp_array_element { };

template<class T>
struct sp_array_element<T[]> {
    typedef T type;
};

template<class T, std::size_t N>
struct sp_array_element<T[N]> {
    typedef T type;
};

template<class T>
struct sp_array_scalar {
    typedef T type;
};

template<class T, std::size_t N>
struct sp_array_scalar<T[N]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T, std::size_t N>
struct sp_array_scalar<const T[N]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T, std::size_t N>
struct sp_array_scalar<volatile T[N]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T, std::size_t N>
struct sp_array_scalar<const volatile T[N]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T>
struct sp_array_scalar<T[]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T>
struct sp_array_scalar<const T[]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T>
struct sp_array_scalar<volatile T[]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T>
struct sp_array_scalar<const volatile T[]> {
    typedef typename sp_array_scalar<T>::type type;
};

template<class T>
struct sp_array_count {
    enum {
        value = 1
    };
};

template<class T, std::size_t N>
struct sp_array_count<T[N]> {
    enum {
        value = N * sp_array_count<T>::value
    };
};

template<std::size_t N, std::size_t M>
struct sp_max_size {
    enum {
        value = N < M ? M : N
    };
};

template<std::size_t N, std::size_t M>
struct sp_align_up {
    enum {
        value = (N + M - 1) & ~(M - 1)
    };
};

#if !defined(BOOST_NO_CXX11_ALLOCATOR)
template<class A, class T>
struct sp_bind_allocator {
    typedef typename std::allocator_traits<A>::template rebind_alloc<T> type;
};
#else
template<class A, class T>
struct sp_bind_allocator {
    typedef typename A::template rebind<T>::other type;
};
#endif

template<class T>
BOOST_CONSTEXPR inline std::size_t
sp_objects(std::size_t size) BOOST_SP_NOEXCEPT
{
    return (size + sizeof(T) - 1) / sizeof(T);
}

template<bool, class = void>
struct sp_enable { };

template<class T>
struct sp_enable<true, T> {
    typedef T type;
};

template<bool E, class A, class T>
inline typename sp_enable<!E && boost::has_trivial_destructor<T>::value>::type
sp_array_destroy(A&, T*, std::size_t) BOOST_SP_NOEXCEPT { }

template<bool E, class A, class T>
inline typename sp_enable<!E &&
    !boost::has_trivial_destructor<T>::value>::type
sp_array_destroy(A&, T* start, std::size_t size)
{
    while (size > 0) {
        start[--size].~T();
    }
}

#if !defined(BOOST_NO_CXX11_ALLOCATOR)
template<bool E, class A, class T>
inline typename sp_enable<E>::type
sp_array_destroy(A& allocator, T* start, std::size_t size)
{
    while (size > 0) {
        std::allocator_traits<A>::destroy(allocator, start + --size);
    }
}
#endif

template<bool E, class A, class T>
inline typename sp_enable<!E &&
    boost::has_trivial_constructor<T>::value &&
    boost::has_trivial_assign<T>::value>::type
sp_array_construct(A&, T* start, std::size_t size)
{
    for (std::size_t i = 0; i < size; ++i) {
        start[i] = T();
    }
}

template<bool E, class A, class T>
inline typename sp_enable<!E &&
    boost::has_trivial_constructor<T>::value &&
    boost::has_trivial_assign<T>::value>::type
sp_array_construct(A&, T* start, std::size_t size, const T* list,
    std::size_t count)
{
    for (std::size_t i = 0; i < size; ++i) {
        start[i] = list[i % count];
    }
}

#if !defined(BOOST_NO_EXCEPTIONS)
template<bool E, class A, class T>
inline typename sp_enable<!E &&
    !(boost::has_trivial_constructor<T>::value &&
      boost::has_trivial_assign<T>::value)>::type
sp_array_construct(A& none, T* start, std::size_t size)
{
    std::size_t i = 0;
    try {
        for (; i < size; ++i) {
            ::new(static_cast<void*>(start + i)) T();
        }
    } catch (...) {
        sp_array_destroy<E>(none, start, i);
        throw;
    }
}

template<bool E, class A, class T>
inline typename sp_enable<!E &&
    !(boost::has_trivial_constructor<T>::value &&
      boost::has_trivial_assign<T>::value)>::type
sp_array_construct(A& none, T* start, std::size_t size, const T* list,
    std::size_t count)
{
    std::size_t i = 0;
    try {
        for (; i < size; ++i) {
            ::new(static_cast<void*>(start + i)) T(list[i % count]);
        }
    } catch (...) {
        sp_array_destroy<E>(none, start, i);
        throw;
    }
}
#else
template<bool E, class A, class T>
inline typename sp_enable<!E &&
    !(boost::has_trivial_constructor<T>::value &&
      boost::has_trivial_assign<T>::value)>::type
sp_array_construct(A&, T* start, std::size_t size)
{
    for (std::size_t i = 0; i < size; ++i) {
        ::new(static_cast<void*>(start + i)) T();
    }
}

template<bool E, class A, class T>
inline typename sp_enable<!E &&
    !(boost::has_trivial_constructor<T>::value &&
      boost::has_trivial_assign<T>::value)>::type
sp_array_construct(A&, T* start, std::size_t size, const T* list,
    std::size_t count)
{
    for (std::size_t i = 0; i < size; ++i) {
        ::new(static_cast<void*>(start + i)) T(list[i % count]);
    }
}
#endif

#if !defined(BOOST_NO_CXX11_ALLOCATOR)
#if !defined(BOOST_NO_EXCEPTIONS)
template<bool E, class A, class T>
inline typename sp_enable<E>::type
sp_array_construct(A& allocator, T* start, std::size_t size)
{
    std::size_t i = 0;
    try {
        for (i = 0; i < size; ++i) {
            std::allocator_traits<A>::construct(allocator, start + i);
        }
    } catch (...) {
        sp_array_destroy<E>(allocator, start, i);
        throw;
    }
}

template<bool E, class A, class T>
inline typename sp_enable<E>::type
sp_array_construct(A& allocator, T* start, std::size_t size, const T* list,
    std::size_t count)
{
    std::size_t i = 0;
    try {
        for (i = 0; i < size; ++i) {
            std::allocator_traits<A>::construct(allocator, start + i,
                list[i % count]);
        }
    } catch (...) {
        sp_array_destroy<E>(allocator, start, i);
        throw;
    }
}
#else
template<bool E, class A, class T>
inline typename sp_enable<E>::type
sp_array_construct(A& allocator, T* start, std::size_t size)
{
    for (std::size_t i = 0; i < size; ++i) {
        std::allocator_traits<A>::construct(allocator, start + i);
    }
}

template<bool E, class A, class T>
inline typename sp_enable<E>::type
sp_array_construct(A& allocator, T* start, std::size_t size, const T* list,
    std::size_t count)
{
    for (std::size_t i = 0; i < size; ++i) {
        std::allocator_traits<A>::construct(allocator, start + i,
            list[i % count]);
    }
}
#endif
#endif

template<class A, class T>
inline typename sp_enable<boost::has_trivial_constructor<T>::value>::type
sp_array_default(A&, T*, std::size_t) BOOST_SP_NOEXCEPT { }

#if !defined(BOOST_NO_EXCEPTIONS)
template<class A, class T>
inline typename sp_enable<!boost::has_trivial_constructor<T>::value>::type
sp_array_default(A& none, T* start, std::size_t size)
{
    std::size_t i = 0;
    try {
        for (; i < size; ++i) {
            ::new(static_cast<void*>(start + i)) T;
        }
    } catch (...) {
        sp_array_destroy<false>(none, start, i);
        throw;
    }
}
#else
template<bool E, class A, class T>
inline typename sp_enable<!boost::has_trivial_constructor<T>::value>::type
sp_array_default(A&, T* start, std::size_t size)
{
    for (std::size_t i = 0; i < size; ++i) {
        ::new(static_cast<void*>(start + i)) T;
    }
}
#endif

template<class A>
class sp_array_state {
public:
    typedef A type;

    template<class U>
    sp_array_state(const U& allocator, std::size_t size) BOOST_SP_NOEXCEPT
        : allocator_(allocator),
          size_(size) { }

    A& allocator() BOOST_SP_NOEXCEPT {
        return allocator_;
    }

    std::size_t size() const BOOST_SP_NOEXCEPT {
        return size_;
    }

private:
    A allocator_;
    std::size_t size_;
};

template<class A, std::size_t N>
class sp_size_array_state {
public:
    typedef A type;

    template<class U>
    sp_size_array_state(const U& allocator, std::size_t) BOOST_SP_NOEXCEPT
        : allocator_(allocator) { }

    A& allocator() BOOST_SP_NOEXCEPT {
        return allocator_;
    }

    BOOST_CONSTEXPR std::size_t size() const BOOST_SP_NOEXCEPT {
        return N;
    }

private:
    A allocator_;
};

#if !defined(BOOST_NO_CXX11_ALLOCATOR)
template<class A>
struct sp_use_construct {
    enum {
        value = true
    };
};

template<class T>
struct sp_use_construct<std::allocator<T> > {
    enum {
        value = false
    };
};
#else
template<class>
struct sp_use_construct {
    enum {
        value = false
    };
};
#endif

template<class T, class U>
struct sp_array_alignment {
    enum {
        value = sp_max_size<boost::alignment_of<T>::value,
            boost::alignment_of<U>::value>::value
    };
};

template<class T, class U>
struct sp_array_offset {
    enum {
        value = sp_align_up<sizeof(T), sp_array_alignment<T, U>::value>::value
    };
};

template<class T, class U>
struct sp_array_storage {
    enum {
        value = sp_array_alignment<T, U>::value
    };
    typedef typename boost::type_with_alignment<value>::type type;
};

template<class T, class U>
inline U*
sp_array_start(void* base) BOOST_SP_NOEXCEPT
{
    enum {
        size = sp_array_offset<T, U>::value
    };
    return reinterpret_cast<U*>(static_cast<char*>(base) + size);
}

template<class A, class T>
class sp_array_creator {
    typedef typename A::value_type scalar;

    enum {
        offset = sp_array_offset<T, scalar>::value
    };

    typedef typename sp_array_storage<T, scalar>::type type;

public:
    template<class U>
    sp_array_creator(const U& other, std::size_t size) BOOST_SP_NOEXCEPT
        : other_(other),
          size_(sp_objects<type>(offset + sizeof(scalar) * size)) { }

    T* create() {
        return reinterpret_cast<T*>(other_.allocate(size_));
    }

    void destroy(T* base) {
        other_.deallocate(reinterpret_cast<type*>(base), size_);
    }

private:
    typename sp_bind_allocator<A, type>::type other_;
    std::size_t size_;
};

struct sp_default { };

template<class T, bool E = sp_use_construct<T>::value>
class sp_array_base
    : public sp_counted_base {
    typedef typename T::type allocator;

public:
    typedef typename allocator::value_type type;

    template<class A>
    sp_array_base(const A& other, std::size_t size, type* start)
        : state_(other, size) {
        sp_array_construct<E>(state_.allocator(), start, state_.size());
    }

    template<class A>
    sp_array_base(const A& other, std::size_t size, const type* list,
        std::size_t count, type* start)
        : state_(other, size) {
        sp_array_construct<E>(state_.allocator(), start, state_.size(), list,
            count);
    }

    template<class A>
    sp_array_base(sp_default, const A& other, std::size_t size, type* start)
        : state_(other, size) {
        sp_array_default(state_.allocator(), start, state_.size());
    }

    T& state() BOOST_SP_NOEXCEPT {
        return state_;
    }

    virtual void dispose() {
        sp_array_destroy<E>(state_.allocator(),
            sp_array_start<sp_array_base, type>(this), state_.size());
    }

    virtual void destroy() {
        sp_array_creator<allocator, sp_array_base> other(state_.allocator(),
            state_.size());
        this->~sp_array_base();
        other.destroy(this);
    }

    virtual void* get_deleter(const sp_typeinfo&) {
        return 0;
    }

    virtual void* get_local_deleter(const sp_typeinfo&) {
        return 0;
    }

    virtual void* get_untyped_deleter() {
        return 0;
    }

private:
    T state_;
};

template<class A, class T>
struct sp_array_result {
public:
    template<class U>
    sp_array_result(const U& other, std::size_t size)
        : creator_(other, size),
          result_(creator_.create()) { }

    ~sp_array_result() {
        if (result_) {
            creator_.destroy(result_);
        }
    }

    T* get() const {
        return result_;
    }

    void release() {
        result_ = 0;
    }

private:
    sp_array_result(const sp_array_result&);
    sp_array_result& operator=(const sp_array_result&);

    sp_array_creator<A, T> creator_;
    T* result_;
};

} /* detail */

template<class T, class A>
inline typename detail::sp_if_array<T>::type
allocate_shared(const A& allocator, std::size_t count)
{
    typedef typename detail::sp_array_element<T>::type type;
    typedef typename detail::sp_array_scalar<T>::type scalar;
    typedef typename detail::sp_bind_allocator<A, scalar>::type other;
    typedef detail::sp_array_state<other> state;
    typedef detail::sp_array_base<state> base;
    std::size_t size = count * detail::sp_array_count<type>::value;
    detail::sp_array_result<other, base> result(allocator, size);
    detail::sp_counted_base* node = result.get();
    scalar* start = detail::sp_array_start<base, scalar>(node);
    ::new(static_cast<void*>(node)) base(allocator, size, start);
    result.release();
    return shared_ptr<T>(detail::sp_internal_constructor_tag(),
        reinterpret_cast<type*>(start), detail::shared_count(node));
}

template<class T, class A>
inline typename detail::sp_if_size_array<T>::type
allocate_shared(const A& allocator)
{
    enum {
        size = detail::sp_array_count<T>::value
    };
    typedef typename detail::sp_array_element<T>::type type;
    typedef typename detail::sp_array_scalar<T>::type scalar;
    typedef typename detail::sp_bind_allocator<A, scalar>::type other;
    typedef detail::sp_size_array_state<other, size> state;
    typedef detail::sp_array_base<state> base;
    detail::sp_array_result<other, base> result(allocator, size);
    detail::sp_counted_base* node = result.get();
    scalar* start = detail::sp_array_start<base, scalar>(node);
    ::new(static_cast<void*>(node)) base(allocator, size, start);
    result.release();
    return shared_ptr<T>(detail::sp_internal_constructor_tag(),
        reinterpret_cast<type*>(start), detail::shared_count(node));
}

template<class T, class A>
inline typename detail::sp_if_array<T>::type
allocate_shared(const A& allocator, std::size_t count,
    const typename detail::sp_array_element<T>::type& value)
{
    typedef typename detail::sp_array_element<T>::type type;
    typedef typename detail::sp_array_scalar<T>::type scalar;
    typedef typename detail::sp_bind_allocator<A, scalar>::type other;
    typedef detail::sp_array_state<other> state;
    typedef detail::sp_array_base<state> base;
    std::size_t size = count * detail::sp_array_count<type>::value;
    detail::sp_array_result<other, base> result(allocator, size);
    detail::sp_counted_base* node = result.get();
    scalar* start = detail::sp_array_start<base, scalar>(node);
    ::new(static_cast<void*>(node)) base(allocator, size,
        reinterpret_cast<const scalar*>(&value),
        detail::sp_array_count<type>::value, start);
    result.release();
    return shared_ptr<T>(detail::sp_internal_constructor_tag(),
        reinterpret_cast<type*>(start), detail::shared_count(node));
}

template<class T, class A>
inline typename detail::sp_if_size_array<T>::type
allocate_shared(const A& allocator,
    const typename detail::sp_array_element<T>::type& value)
{
    enum {
        size = detail::sp_array_count<T>::value
    };
    typedef typename detail::sp_array_element<T>::type type;
    typedef typename detail::sp_array_scalar<T>::type scalar;
    typedef typename detail::sp_bind_allocator<A, scalar>::type other;
    typedef detail::sp_size_array_state<other, size> state;
    typedef detail::sp_array_base<state> base;
    detail::sp_array_result<other, base> result(allocator, size);
    detail::sp_counted_base* node = result.get();
    scalar* start = detail::sp_array_start<base, scalar>(node);
    ::new(static_cast<void*>(node)) base(allocator, size,
        reinterpret_cast<const scalar*>(&value),
        detail::sp_array_count<type>::value, start);
    result.release();
    return shared_ptr<T>(detail::sp_internal_constructor_tag(),
        reinterpret_cast<type*>(start), detail::shared_count(node));
}

template<class T, class A>
inline typename detail::sp_if_array<T>::type
allocate_shared_noinit(const A& allocator, std::size_t count)
{
    typedef typename detail::sp_array_element<T>::type type;
    typedef typename detail::sp_array_scalar<T>::type scalar;
    typedef typename detail::sp_bind_allocator<A, scalar>::type other;
    typedef detail::sp_array_state<other> state;
    typedef detail::sp_array_base<state, false> base;
    std::size_t size = count * detail::sp_array_count<type>::value;
    detail::sp_array_result<other, base> result(allocator, size);
    detail::sp_counted_base* node = result.get();
    scalar* start = detail::sp_array_start<base, scalar>(node);
    ::new(static_cast<void*>(node)) base(detail::sp_default(), allocator,
        size, start);
    result.release();
    return shared_ptr<T>(detail::sp_internal_constructor_tag(),
        reinterpret_cast<type*>(start), detail::shared_count(node));
}

template<class T, class A>
inline typename detail::sp_if_size_array<T>::type
allocate_shared_noinit(const A& allocator)
{
    enum {
        size = detail::sp_array_count<T>::value
    };
    typedef typename detail::sp_array_element<T>::type type;
    typedef typename detail::sp_array_scalar<T>::type scalar;
    typedef typename detail::sp_bind_allocator<A, scalar>::type other;
    typedef detail::sp_size_array_state<other, size> state;
    typedef detail::sp_array_base<state, false> base;
    detail::sp_array_result<other, base> result(allocator, size);
    detail::sp_counted_base* node = result.get();
    scalar* start = detail::sp_array_start<base, scalar>(node);
    ::new(static_cast<void*>(node)) base(detail::sp_default(), allocator,
        size, start);
    result.release();
    return shared_ptr<T>(detail::sp_internal_constructor_tag(),
        reinterpret_cast<type*>(start), detail::shared_count(node));
}

} /* boost */

#endif
