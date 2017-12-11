#pragma once

#include <boost/noncopyable.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <iostream>


/** Copy-on-write shared ptr.
  * Allows to work with shared immutable objects and sometimes unshare and mutate you own unique copy.
  *
  * Usage:

    class Column : public COWPtr<Column>
    {
    private:
        friend class COWPtr<Column>;

        Column()
        Column(const Column &)
    public:
        ...
    }

  * It will provide 'create', 'clone' and 'mutate' methods.
  *
  * 'create' and 'clone' methods creates mutable noncopyable object: you cannot share mutable objects.
  * 'mutate' method allows to create mutable noncopyable object from immutable object:
  *   either by cloning or by using directly, if it is not shared.
  * These methods are thread-safe.
  *
  * Example:
  *
    Column::Ptr x = Column::create(1);
    Column::Ptr y = x;

    /// Now x and y are shared.

    /// Change value of x.
    {
        Column::MutablePtr mutate_x = x->mutate();
        mutate_x->set(2);
        x = std::move(mutate_x);
    }

    /// Now x and y are unshared and have different values.
  */
template <typename Derived>
class COWPtr : public boost::intrusive_ref_counter<Derived>
{
private:
    Derived * derived() { return static_cast<Derived *>(this); }
    const Derived * derived() const { return static_cast<const Derived *>(this); }

public:
    template <typename T> class noncopyable : public T, private boost::noncopyable { using T::T; };

    template <typename T> class immutable_ptr : public boost::intrusive_ptr<const T>
    {
    public:
        template <typename U>
        immutable_ptr(const boost::intrusive_ptr<const U> & other) : boost::intrusive_ptr<const U>(other) {}

        template <typename U>
        immutable_ptr(const boost::intrusive_ptr<U> && other) : boost::intrusive_ptr<const U>(std::move(other)) {}

        template <typename U>
        immutable_ptr(const boost::intrusive_ptr<U> &) = delete;
    };

    using Ptr = immutable_ptr<Derived>;
    using MutablePtr = noncopyable<boost::intrusive_ptr<Derived>>;

    template <typename... Args>
    static MutablePtr create(Args &&... args) { return new Derived(std::forward<Args>(args)...); }

    Ptr getPtr() const { return static_cast<Ptr>(derived()); }
    MutablePtr getPtr() { return static_cast<MutablePtr>(derived()); }

    MutablePtr clone() const { return new Derived(*derived()); }

    MutablePtr mutate() const
    {
        if (this->use_count() > 1)
            return clone();
        else
            return const_cast<COWPtr*>(this)->getPtr();
    }
};
