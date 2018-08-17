#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <initializer_list>


/** Copy-on-write shared ptr.
  * Allows to work with shared immutable objects and sometimes unshare and mutate you own unique copy.
  *
  * Usage:

    class Column : public COWPtr<Column>
    {
    private:
        friend class COWPtr<Column>;

        /// Leave all constructors in private section. They will be avaliable through 'create' method.
        Column();

        /// Provide 'clone' method. It can be virtual if you want polymorphic behaviour.
        virtual Column * clone() const;
    public:
        /// Correctly use const qualifiers in your interface.

        virtual ~IColumn() {}
    };

  * It will provide 'create' and 'mutate' methods.
  * And 'Ptr' and 'MutablePtr' types.
  * Ptr is refcounted pointer to immutable object.
  * MutablePtr is refcounted noncopyable pointer to mutable object.
  * MutablePtr can be assigned to Ptr through move assignment.
  *
  * 'create' method creates MutablePtr: you cannot share mutable objects.
  * To share, move-assign to immutable pointer.
  * 'mutate' method allows to create mutable noncopyable object from immutable object:
  *   either by cloning or by using directly, if it is not shared.
  * These methods are thread-safe.
  *
  * Example:
  *
    /// Creating and assigning to immutable ptr.
    Column::Ptr x = Column::create(1);
    /// Sharing single immutable object in two ptrs.
    Column::Ptr y = x;

    /// Now x and y are shared.

    /// Change value of x.
    {
        /// Creating mutable ptr. It can clone an object under the hood if it was shared.
        Column::MutablePtr mutate_x = x->mutate();
        /// Using non-const methods of an object.
        mutate_x->set(2);
        /// Assigning pointer 'x' to mutated object.
        x = std::move(mutate_x);
    }

    /// Now x and y are unshared and have different values.

  * Note. You may have heard that COW is bad practice.
  * Actually it is, if your values are small or if copying is done implicitly.
  * This is the case for string implementations.
  *
  * In contrast, COWPtr is intended for the cases when you need to share states of large objects,
  * (when you usually will use std::shared_ptr) but you also want precise control over modification
  * of this shared state.
  *
  * Caveats:
  * - after a call to 'mutate' method, you can still have a reference to immutable ptr somewhere.
  * - as 'mutable_ptr' should be unique, it's refcount is redundant - probably it would be better
  *   to use std::unique_ptr for it somehow.
  */
template <typename Derived>
class COWPtr : public boost::intrusive_ref_counter<Derived>
{
private:
    Derived * derived() { return static_cast<Derived *>(this); }
    const Derived * derived() const { return static_cast<const Derived *>(this); }

    template <typename T>
    class IntrusivePtr : public boost::intrusive_ptr<T>
    {
    public:
        using boost::intrusive_ptr<T>::intrusive_ptr;

        T & operator*() const & { return boost::intrusive_ptr<T>::operator*(); }
        T && operator*() const && { return const_cast<typename std::remove_const<T>::type &&>(*boost::intrusive_ptr<T>::get()); }
    };

protected:
    template <typename T>
    class mutable_ptr : public IntrusivePtr<T>
    {
    private:
        using Base = IntrusivePtr<T>;

        template <typename> friend class COWPtr;
        template <typename, typename> friend class COWPtrHelper;

        explicit mutable_ptr(T * ptr) : Base(ptr) {}

    public:
        /// Copy: not possible.
        mutable_ptr(const mutable_ptr &) = delete;

        /// Move: ok.
        mutable_ptr(mutable_ptr &&) = default;
        mutable_ptr & operator=(mutable_ptr &&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        mutable_ptr(mutable_ptr<U> && other) : Base(std::move(other)) {}

        mutable_ptr() = default;

        mutable_ptr(const std::nullptr_t *) {}
    };

public:
    using MutablePtr = mutable_ptr<Derived>;

protected:
    template <typename T>
    class immutable_ptr : public IntrusivePtr<const T>
    {
    private:
        using Base = IntrusivePtr<const T>;

        template <typename> friend class COWPtr;
        template <typename, typename> friend class COWPtrHelper;

        explicit immutable_ptr(const T * ptr) : Base(ptr) {}

    public:
        /// Copy from immutable ptr: ok.
        immutable_ptr(const immutable_ptr &) = default;
        immutable_ptr & operator=(const immutable_ptr &) = default;

        template <typename U>
        immutable_ptr(const immutable_ptr<U> & other) : Base(other) {}

        /// Move: ok.
        immutable_ptr(immutable_ptr &&) = default;
        immutable_ptr & operator=(immutable_ptr &&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        immutable_ptr(immutable_ptr<U> && other) : Base(std::move(other)) {}

        /// Move from mutable ptr: ok.
        template <typename U>
        immutable_ptr(mutable_ptr<U> && other) : Base(std::move(other)) {}

        /// Copy from mutable ptr: not possible.
        template <typename U>
        immutable_ptr(const mutable_ptr<U> &) = delete;

        immutable_ptr() = default;

        immutable_ptr(const std::nullptr_t *) {}
    };

public:
    using Ptr = immutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args &&... args) { return MutablePtr(new Derived(std::forward<Args>(args)...)); }

    template <typename T>
    static MutablePtr create(std::initializer_list<T> && arg) { return create(std::forward<std::initializer_list<T>>(arg)); }

public:
    Ptr getPtr() const { return static_cast<Ptr>(derived()); }
    MutablePtr getPtr() { return static_cast<MutablePtr>(derived()); }

    MutablePtr mutate() const
    {
        if (this->use_count() > 1)
            return derived()->clone();
        else
            return assumeMutable();
    }

    MutablePtr assumeMutable() const
    {
        return const_cast<COWPtr*>(this)->getPtr();
    }

    Derived & assumeMutableRef() const
    {
        return const_cast<Derived &>(*derived());
    }
};


/** Helper class to support inheritance.
  * Example:
  *
  * class IColumn : public COWPtr<IColumn>
  * {
  *     friend class COWPtr<IColumn>;
  *     virtual MutablePtr clone() const = 0;
  *     virtual ~IColumn() {}
  * };
  *
  * class ConcreteColumn : public COWPtrHelper<IColumn, ConcreteColumn>
  * {
  *     friend class COWPtrHelper<IColumn, ConcreteColumn>;
  * };
  *
  * Here is complete inheritance diagram:
  *
  * ConcreteColumn
  *  COWPtrHelper<IColumn, ConcreteColumn>
  *   IColumn
  *    CowPtr<IColumn>
  *     boost::intrusive_ref_counter<IColumn>
  */
template <typename Base, typename Derived>
class COWPtrHelper : public Base
{
private:
    Derived * derived() { return static_cast<Derived *>(this); }
    const Derived * derived() const { return static_cast<const Derived *>(this); }

public:
    using Ptr = typename Base::template immutable_ptr<Derived>;
    using MutablePtr = typename Base::template mutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args &&... args) { return MutablePtr(new Derived(std::forward<Args>(args)...)); }

    template <typename T>
    static MutablePtr create(std::initializer_list<T> && arg) { return create(std::forward<std::initializer_list<T>>(arg)); }

    typename Base::MutablePtr clone() const override { return typename Base::MutablePtr(new Derived(*derived())); }
};


/** Compositions.
  *
  * Sometimes your objects contain another objects, and you have tree-like structure.
  * And you want non-const methods of your object to also modify your subobjects.
  *
  * There are the following possible solutions:
  *
  * 1. Store subobjects as immutable ptrs. Call mutate method of subobjects inside non-const methods of your objects; modify them and assign back.
  * Drawback: additional checks inside methods: CPU overhead on atomic ops.
  *
  * 2. Store subobjects as mutable ptrs. Subobjects cannot be shared in another objects.
  * Drawback: it's not possible to share subobjects.
  *
  * 3. Store subobjects as immutable ptrs. Implement copy-constructor to do shallow copy.
  * But reimplement 'mutate' method, so it will call 'mutate' of all subobjects (do deep mutate).
  * It will guarantee, that mutable object have all subobjects unshared.
  * From non-const method, you can modify subobjects with 'assumeMutableRef' method.
  * Drawback: it's more complex than other solutions.
  */
