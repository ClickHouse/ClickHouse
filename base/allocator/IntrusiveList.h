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
template <typename T, RingLink<T> T::* link>
struct Ring
{
    /// Initialize a link. Every link must be initialized before being used, even if it is about to be overwritten.
    /// jemalloc: qr_new
    ALLOCATOR_ALWAYS_INLINE static void init(T * element)
    {
        (element->*link).next = element;
        (element->*link).prev = element;
    }

    /// jemalloc: qr_next
    ALLOCATOR_ALWAYS_INLINE static T * next(const T * element) { return (element->*link).next; }

    /// jemalloc: qr_prev
    ALLOCATOR_ALWAYS_INLINE static T * prev(const T * element) { return (element->*link).prev; }

    /// Given rings `a -> a_1 -> ... -> a_n` and `b -> b_1 -> ... -> b_n`, results in the ring
    /// `a -> a_1 -> ... -> a_n -> b -> b_1 -> ... -> b_n`.
    /// jemalloc: qr_meld
    ALLOCATOR_ALWAYS_INLINE static void meld(T * a, T * b)
    {
        ((b->*link).prev->*link).next = (a->*link).prev;
        (a->*link).prev = (b->*link).prev;
        (b->*link).prev = ((b->*link).prev->*link).next;
        ((a->*link).prev->*link).next = a;
        ((b->*link).prev->*link).next = b;
    }

    /// Logically a meld; `element` is intended to be a single-element ring that gets inserted before `ring_element`.
    /// jemalloc: qr_before_insert
    ALLOCATOR_ALWAYS_INLINE static void beforeInsert(T * ring_element, T * element) { meld(ring_element, element); }

    /// jemalloc: qr_after_insert
    ALLOCATOR_ALWAYS_INLINE static void afterInsert(T * ring_element, T * element) { beforeInsert(next(ring_element), element); }

    /// Inverts meld: given the ring `a -> ... -> a_n -> b -> ... -> b_n`, results in the rings `a -> ... -> a_n` and
    /// `b -> ... -> b_n`.
    /// jemalloc: qr_split
    ALLOCATOR_ALWAYS_INLINE static void split(T * a, T * b) { meld(a, b); }

    /// Splits `element` off the rest of its ring, so that it becomes a single-element ring.
    /// jemalloc: qr_remove
    ALLOCATOR_ALWAYS_INLINE static void remove(T * element) { split(next(element), element); }

    /// Calls `f(element)` for every element of the ring exactly once, starting with `start`. `start` may be null.
    /// jemalloc: qr_foreach
    template <typename F>
    ALLOCATOR_ALWAYS_INLINE static void forEach(T * start, F && f)
    {
        for (T * variable = start; variable != nullptr; variable = (next(variable) != start) ? next(variable) : nullptr)
            f(variable);
    }

    /// The same in the opposite order, ending with `start`.
    /// jemalloc: qr_reverse_foreach
    template <typename F>
    ALLOCATOR_ALWAYS_INLINE static void reverseForEach(T * start, F && f)
    {
        for (T * variable = (start != nullptr) ? prev(start) : nullptr; variable != nullptr;
             variable = (variable != start) ? prev(variable) : nullptr)
            f(variable);
    }
};

/// A list built on top of a ring: the head points to the first element (or is null), advancing past the tail does
/// not wrap around.
/// jemalloc: ql_head(a_type)
template <typename T, RingLink<T> T::* link>
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
    ALLOCATOR_ALWAYS_INLINE void init() { head = nullptr; }

    /// jemalloc: ql_first
    ALLOCATOR_ALWAYS_INLINE T * first() const { return head; }

    /// jemalloc: ql_empty
    ALLOCATOR_ALWAYS_INLINE bool empty() const { return head == nullptr; }

    /// Sets this list to the contents of `src` (overwriting any elements here), leaving `src` empty.
    /// jemalloc: ql_move
    ALLOCATOR_ALWAYS_INLINE void moveFrom(IntrusiveList & src)
    {
        head = src.head;
        src.init();
    }

    /// Initializes an element link. Must be called even if the link is about to be overwritten.
    /// jemalloc: ql_elm_new
    ALLOCATOR_ALWAYS_INLINE static void elementInit(T * element) { RingOps::init(element); }

    /// jemalloc: ql_last
    ALLOCATOR_ALWAYS_INLINE T * last() const { return empty() ? nullptr : RingOps::prev(head); }

    /// jemalloc: ql_next
    ALLOCATOR_ALWAYS_INLINE T * next(const T * element) const { return (last() != element) ? RingOps::next(element) : nullptr; }

    /// jemalloc: ql_prev
    ALLOCATOR_ALWAYS_INLINE T * prev(const T * element) const { return (head != element) ? RingOps::prev(element) : nullptr; }

    /// Inserts `element` before `list_element`.
    /// jemalloc: ql_before_insert
    ALLOCATOR_ALWAYS_INLINE void beforeInsert(T * list_element, T * element)
    {
        RingOps::beforeInsert(list_element, element);
        if (head == list_element)
            head = element;
    }

    /// Inserts `element` after `list_element`.
    /// jemalloc: ql_after_insert
    ALLOCATOR_ALWAYS_INLINE static void afterInsert(T * list_element, T * element) { RingOps::afterInsert(list_element, element); }

    /// Inserts `element` as the first item.
    /// jemalloc: ql_head_insert
    ALLOCATOR_ALWAYS_INLINE void headInsert(T * element)
    {
        if (!empty())
            RingOps::beforeInsert(head, element);
        head = element;
    }

    /// Inserts `element` as the last item.
    /// jemalloc: ql_tail_insert
    ALLOCATOR_ALWAYS_INLINE void tailInsert(T * element)
    {
        if (!empty())
            RingOps::beforeInsert(head, element);
        head = RingOps::next(element);
    }

    /// Given lists a = [a_1, ..., a_n] (this) and b = [b_1, ..., b_n], results in a = [a_1, ..., a_n, b_1, ..., b_n]
    /// and b = [].
    /// jemalloc: ql_concat
    ALLOCATOR_ALWAYS_INLINE void concat(IntrusiveList & b)
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
    ALLOCATOR_ALWAYS_INLINE void remove(T * element)
    {
        if (head == element)
            head = RingOps::next(head);
        if (head != element)
            RingOps::remove(element);
        else
            init();
    }

    /// jemalloc: ql_head_remove
    ALLOCATOR_ALWAYS_INLINE void headRemove()
    {
        T * t = first();
        remove(t);
    }

    /// jemalloc: ql_tail_remove
    ALLOCATOR_ALWAYS_INLINE void tailRemove()
    {
        T * t = last();
        remove(t);
    }

    /// Given a = [a_1, ..., a_n-1, a_n, a_n+1, ...] (this), results in a = [a_1, ..., a_n-1] and replaces the
    /// contents of b with [a_n, a_n+1, ...].
    /// jemalloc: ql_split
    ALLOCATOR_ALWAYS_INLINE void split(T * element, IntrusiveList & b)
    {
        if (head == element)
        {
            b.moveFrom(*this);
        }
        else
        {
            RingOps::split(head, element);
            b.head = element;
        }
    }

    /// An optimized version of: remove the first element and insert it at the tail.
    /// jemalloc: ql_rotate
    ALLOCATOR_ALWAYS_INLINE void rotate() { head = RingOps::next(head); }

    /// Iterates from the head. The callback must not modify the list.
    /// jemalloc: ql_foreach
    template <typename F>
    ALLOCATOR_ALWAYS_INLINE void forEach(F && f) const
    {
        RingOps::forEach(head, static_cast<F &&>(f));
    }

    /// Iterates from the tail. The callback must not modify the list.
    /// jemalloc: ql_reverse_foreach
    template <typename F>
    ALLOCATOR_ALWAYS_INLINE void reverseForEach(F && f) const
    {
        RingOps::reverseForEach(head, static_cast<F &&>(f));
    }

    /// Range-for support with exactly the `ql_foreach` order.
    class Iterator
    {
    public:
        Iterator(T * start_, T * current_)
            : start(start_)
            , current(current_)
        {
        }
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
template <typename T, RingLink<T> T::* link>
class TypedList
{
public:
    using List = IntrusiveList<T, link>;

    constexpr TypedList() = default;

    TypedList(const TypedList &) = delete;
    TypedList & operator=(const TypedList &) = delete;

    /// jemalloc: <list_type>_init
    ALLOCATOR_ALWAYS_INLINE void init() { head.init(); }

    /// jemalloc: <list_type>_first
    ALLOCATOR_ALWAYS_INLINE T * first() const { return head.first(); }

    /// jemalloc: <list_type>_last
    ALLOCATOR_ALWAYS_INLINE T * last() const { return head.last(); }

    /// jemalloc: <list_type>_next
    ALLOCATOR_ALWAYS_INLINE T * next(T * item) const { return head.next(item); }

    /// jemalloc: <list_type>_append
    ALLOCATOR_ALWAYS_INLINE void append(T * item)
    {
        List::elementInit(item);
        head.tailInsert(item);
    }

    /// jemalloc: <list_type>_prepend
    ALLOCATOR_ALWAYS_INLINE void prepend(T * item)
    {
        List::elementInit(item);
        head.headInsert(item);
    }

    /// jemalloc: <list_type>_replace
    ALLOCATOR_ALWAYS_INLINE void replace(T * to_remove, T * to_insert)
    {
        List::elementInit(to_insert);
        List::afterInsert(to_remove, to_insert);
        head.remove(to_remove);
    }

    /// jemalloc: <list_type>_remove
    ALLOCATOR_ALWAYS_INLINE void remove(T * item) { head.remove(item); }

    /// jemalloc: <list_type>_empty
    ALLOCATOR_ALWAYS_INLINE bool empty() const { return head.empty(); }

    /// jemalloc: <list_type>_concat
    ALLOCATOR_ALWAYS_INLINE void concat(TypedList & other) { head.concat(other.head); }

    template <typename F>
    ALLOCATOR_ALWAYS_INLINE void forEach(F && f) const
    {
        head.forEach(static_cast<F &&>(f));
    }

    template <typename F>
    ALLOCATOR_ALWAYS_INLINE void reverseForEach(F && f) const
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
