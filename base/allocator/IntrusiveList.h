#pragma once

/// Intrusive circular doubly-linked rings and lists (jemalloc: `qr.h`, `ql.h`, `typed_list.h`).
///
/// The semantics (including the iteration order after every operation) are exactly those of jemalloc, because the
/// order of elements in these lists is observable (e.g. which extent or tcache is visited first).
///
///     struct Node
///     {
///         int data;
///         RingLink<Node> link;
///     };
///     using NodeList = IntrusiveList<Node, &Node::link>;

#include <allocator/Common.h>

namespace jemalloc
{

/// jemalloc: qr(a_type) / ql_elm(a_type)
template <typename T>
struct RingLink
{
    T * next;
    T * prev;
};

/// Operations on rings: every element is in exactly one ring (a single element is a ring of itself).
template <typename T, RingLink<T> T::*link>
struct Ring
{
    /// Initialize a link. Every link must be initialized before being used, even if it is about to be overwritten.
    /// jemalloc: qr_new
    JE_ALWAYS_INLINE static void init(T * elm)
    {
        (elm->*link).next = elm;
        (elm->*link).prev = elm;
    }

    /// jemalloc: qr_next
    JE_ALWAYS_INLINE static T * next(const T * elm) { return (elm->*link).next; }

    /// jemalloc: qr_prev
    JE_ALWAYS_INLINE static T * prev(const T * elm) { return (elm->*link).prev; }

    /// Given rings `a -> a_1 -> ... -> a_n` and `b -> b_1 -> ... -> b_n`, results in the ring
    /// `a -> a_1 -> ... -> a_n -> b -> b_1 -> ... -> b_n`.
    /// jemalloc: qr_meld
    JE_ALWAYS_INLINE static void meld(T * a, T * b)
    {
        ((b->*link).prev->*link).next = (a->*link).prev;
        (a->*link).prev = (b->*link).prev;
        (b->*link).prev = ((b->*link).prev->*link).next;
        ((a->*link).prev->*link).next = a;
        ((b->*link).prev->*link).next = b;
    }

    /// Logically a meld; `elm` is intended to be a single-element ring that gets inserted before `ringelm`.
    /// jemalloc: qr_before_insert
    JE_ALWAYS_INLINE static void beforeInsert(T * ringelm, T * elm) { meld(ringelm, elm); }

    /// jemalloc: qr_after_insert
    JE_ALWAYS_INLINE static void afterInsert(T * ringelm, T * elm) { beforeInsert(next(ringelm), elm); }

    /// Inverts meld: given the ring `a -> ... -> a_n -> b -> ... -> b_n`, results in the rings `a -> ... -> a_n` and
    /// `b -> ... -> b_n`.
    /// jemalloc: qr_split
    JE_ALWAYS_INLINE static void split(T * a, T * b) { meld(a, b); }

    /// Splits `elm` off the rest of its ring, so that it becomes a single-element ring.
    /// jemalloc: qr_remove
    JE_ALWAYS_INLINE static void remove(T * elm) { split(next(elm), elm); }

    /// Calls `f(elm)` for every element of the ring exactly once, starting with `start`. `start` may be null.
    /// jemalloc: qr_foreach
    template <typename F>
    JE_ALWAYS_INLINE static void forEach(T * start, F && f)
    {
        for (T * var = start; var != nullptr; var = (next(var) != start) ? next(var) : nullptr)
            f(var);
    }

    /// The same in the opposite order, ending with `start`.
    /// jemalloc: qr_reverse_foreach
    template <typename F>
    JE_ALWAYS_INLINE static void reverseForEach(T * start, F && f)
    {
        for (T * var = (start != nullptr) ? prev(start) : nullptr; var != nullptr; var = (var != start) ? prev(var) : nullptr)
            f(var);
    }
};

/// A list built on top of a ring: the head points to the first element (or is null), advancing past the tail does
/// not wrap around.
/// jemalloc: ql_head(a_type)
template <typename T, RingLink<T> T::*link>
class IntrusiveList
{
public:
    using RingOps = Ring<T, link>;

    /// jemalloc: ql_head_initializer
    constexpr IntrusiveList() = default;

    IntrusiveList(const IntrusiveList &) = delete;
    IntrusiveList & operator=(const IntrusiveList &) = delete;

    /// Dynamically initializes a list.
    /// jemalloc: ql_new
    JE_ALWAYS_INLINE void init() { head = nullptr; }

    /// jemalloc: ql_first
    JE_ALWAYS_INLINE T * first() const { return head; }

    /// jemalloc: ql_empty
    JE_ALWAYS_INLINE bool empty() const { return head == nullptr; }

    /// Sets this list to the contents of `src` (overwriting any elements here), leaving `src` empty.
    /// jemalloc: ql_move
    JE_ALWAYS_INLINE void moveFrom(IntrusiveList & src)
    {
        head = src.head;
        src.init();
    }

    /// Initializes an element link. Must be called even if the link is about to be overwritten.
    /// jemalloc: ql_elm_new
    JE_ALWAYS_INLINE static void elementInit(T * elm) { RingOps::init(elm); }

    /// jemalloc: ql_last
    JE_ALWAYS_INLINE T * last() const { return empty() ? nullptr : RingOps::prev(head); }

    /// jemalloc: ql_next
    JE_ALWAYS_INLINE T * next(const T * elm) const { return (last() != elm) ? RingOps::next(elm) : nullptr; }

    /// jemalloc: ql_prev
    JE_ALWAYS_INLINE T * prev(const T * elm) const { return (head != elm) ? RingOps::prev(elm) : nullptr; }

    /// Inserts `elm` before `listelm`.
    /// jemalloc: ql_before_insert
    JE_ALWAYS_INLINE void beforeInsert(T * listelm, T * elm)
    {
        RingOps::beforeInsert(listelm, elm);
        if (head == listelm)
            head = elm;
    }

    /// Inserts `elm` after `listelm`.
    /// jemalloc: ql_after_insert
    JE_ALWAYS_INLINE static void afterInsert(T * listelm, T * elm) { RingOps::afterInsert(listelm, elm); }

    /// Inserts `elm` as the first item.
    /// jemalloc: ql_head_insert
    JE_ALWAYS_INLINE void headInsert(T * elm)
    {
        if (!empty())
            RingOps::beforeInsert(head, elm);
        head = elm;
    }

    /// Inserts `elm` as the last item.
    /// jemalloc: ql_tail_insert
    JE_ALWAYS_INLINE void tailInsert(T * elm)
    {
        if (!empty())
            RingOps::beforeInsert(head, elm);
        head = RingOps::next(elm);
    }

    /// Given lists a = [a_1, ..., a_n] (this) and b = [b_1, ..., b_n], results in a = [a_1, ..., a_n, b_1, ..., b_n]
    /// and b = [].
    /// jemalloc: ql_concat
    JE_ALWAYS_INLINE void concat(IntrusiveList & b)
    {
        if (empty())
        {
            moveFrom(b);
        }
        else if (!b.empty())
        {
            RingOps::meld(head, b.head);
            b.init();
        }
    }

    /// jemalloc: ql_remove
    JE_ALWAYS_INLINE void remove(T * elm)
    {
        if (head == elm)
            head = RingOps::next(head);
        if (head != elm)
            RingOps::remove(elm);
        else
            init();
    }

    /// jemalloc: ql_head_remove
    JE_ALWAYS_INLINE void headRemove()
    {
        T * t = first();
        remove(t);
    }

    /// jemalloc: ql_tail_remove
    JE_ALWAYS_INLINE void tailRemove()
    {
        T * t = last();
        remove(t);
    }

    /// Given a = [a_1, ..., a_n-1, a_n, a_n+1, ...] (this), results in a = [a_1, ..., a_n-1] and replaces the
    /// contents of b with [a_n, a_n+1, ...].
    /// jemalloc: ql_split
    JE_ALWAYS_INLINE void split(T * elm, IntrusiveList & b)
    {
        if (head == elm)
        {
            b.moveFrom(*this);
        }
        else
        {
            RingOps::split(head, elm);
            b.head = elm;
        }
    }

    /// An optimized version of: remove the first element and insert it at the tail.
    /// jemalloc: ql_rotate
    JE_ALWAYS_INLINE void rotate() { head = RingOps::next(head); }

    /// Iterates from the head. The callback must not modify the list.
    /// jemalloc: ql_foreach
    template <typename F>
    JE_ALWAYS_INLINE void forEach(F && f) const
    {
        RingOps::forEach(head, static_cast<F &&>(f));
    }

    /// Iterates from the tail. The callback must not modify the list.
    /// jemalloc: ql_reverse_foreach
    template <typename F>
    JE_ALWAYS_INLINE void reverseForEach(F && f) const
    {
        RingOps::reverseForEach(head, static_cast<F &&>(f));
    }

    /// Range-for support with exactly the `ql_foreach` order.
    class Iterator
    {
    public:
        Iterator(T * start_, T * current_) : start(start_), current(current_) { }
        T * operator*() const { return current; }
        Iterator & operator++()
        {
            current = (RingOps::next(current) != start) ? RingOps::next(current) : nullptr;
            return *this;
        }
        bool operator==(const Iterator & other) const { return current == other.current; }
        bool operator!=(const Iterator & other) const { return current != other.current; }

    private:
        T * start;
        T * current;
    };

    Iterator begin() const { return Iterator(head, head); }
    Iterator end() const { return Iterator(head, nullptr); }

private:
    T * head = nullptr;
};

/// A list class that handles `ql_elm_new` calls itself (jemalloc: `TYPED_LIST(list_type, el_type, linkage)`, e.g.
/// `edata_list_active_t`, `edata_list_inactive_t`).
template <typename T, RingLink<T> T::*link>
class TypedList
{
public:
    using List = IntrusiveList<T, link>;

    constexpr TypedList() = default;

    TypedList(const TypedList &) = delete;
    TypedList & operator=(const TypedList &) = delete;

    /// jemalloc: <list_type>_init
    JE_ALWAYS_INLINE void init() { head.init(); }

    /// jemalloc: <list_type>_first
    JE_ALWAYS_INLINE T * first() const { return head.first(); }

    /// jemalloc: <list_type>_last
    JE_ALWAYS_INLINE T * last() const { return head.last(); }

    /// jemalloc: <list_type>_next
    JE_ALWAYS_INLINE T * next(T * item) const { return head.next(item); }

    /// jemalloc: <list_type>_append
    JE_ALWAYS_INLINE void append(T * item)
    {
        List::elementInit(item);
        head.tailInsert(item);
    }

    /// jemalloc: <list_type>_prepend
    JE_ALWAYS_INLINE void prepend(T * item)
    {
        List::elementInit(item);
        head.headInsert(item);
    }

    /// jemalloc: <list_type>_replace
    JE_ALWAYS_INLINE void replace(T * to_remove, T * to_insert)
    {
        List::elementInit(to_insert);
        List::afterInsert(to_remove, to_insert);
        head.remove(to_remove);
    }

    /// jemalloc: <list_type>_remove
    JE_ALWAYS_INLINE void remove(T * item) { head.remove(item); }

    /// jemalloc: <list_type>_empty
    JE_ALWAYS_INLINE bool empty() const { return head.empty(); }

    /// jemalloc: <list_type>_concat
    JE_ALWAYS_INLINE void concat(TypedList & other) { head.concat(other.head); }

    template <typename F>
    JE_ALWAYS_INLINE void forEach(F && f) const
    {
        head.forEach(static_cast<F &&>(f));
    }

    template <typename F>
    JE_ALWAYS_INLINE void reverseForEach(F && f) const
    {
        head.reverseForEach(static_cast<F &&>(f));
    }

    typename List::Iterator begin() const { return head.begin(); }
    typename List::Iterator end() const { return head.end(); }

    /// The underlying `ql` list (`list->head` in jemalloc), for `ql_*` operations that `TYPED_LIST` does not wrap.
    List & raw() { return head; }
    const List & raw() const { return head; }

private:
    List head;
};

}
