#pragma once

#include <Common/HashTable/HashSet.h>

#include <array>
#include <memory>

namespace DB
{

template <bool flag_per_row>
class KnownRowsHolder;

/// Keep already joined rows to prevent duplication if many disjuncts
///   if for a particular pair of rows condition looks like TRUE or TRUE or TRUE
///   we want to have it once in resultset
template<>
class KnownRowsHolder<true>
{
public:
    /// The encoded RowRef word (the INLINE_FLAG bit is always set, so equality of
    /// words is equality of (block_no, row_no) pairs).
    using Type = UInt64;

private:
    static const size_t MAX_LINEAR = 16; // threshold to switch from Array to Set
    using ArrayHolder = std::array<Type, MAX_LINEAR>;
    using SetHolder = HashSet<Type, DefaultHash<Type>, HashTableGrower<5>>;
    using SetHolderPtr = std::unique_ptr<SetHolder>;

    ArrayHolder array_holder;
    SetHolderPtr set_holder_ptr;

    size_t items;

public:
    /// `array_holder` is left uninitialized: only its first `items` entries are read.
    KnownRowsHolder() /// NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
        : items(0)
    {
    }

    /// Forgets the rows. It keeps the hash set that it made when the rows outgrew the array, but never reads it again: when the
    /// rows outgrow the array again, a new set replaces it.
    void clear() { items = 0; }

    template<class InputIt>
    void add(InputIt from, InputIt to)
    {
        const size_t new_items = std::distance(from, to);
        if (items + new_items <= MAX_LINEAR)
        {
            std::copy(from, to, std::begin(array_holder) + items);
        }
        else
        {
            if (items <= MAX_LINEAR)
            {
                set_holder_ptr = std::make_unique<SetHolder>(items + new_items);
                for (size_t i = 0; i < items; ++i)
                    set_holder_ptr->insert(array_holder[i]);
            }
            for (auto it = from; it != to; ++it)
                set_holder_ptr->insert(*it);
        }
        items += new_items;
    }

    template<class Needle>
    bool isKnown(const Needle & needle)
    {
        return items <= MAX_LINEAR
            ? std::find(std::cbegin(array_holder), std::cbegin(array_holder) + items, needle) != std::cbegin(array_holder) + items
            : set_holder_ptr->has(needle);
    }
};

template<>
class KnownRowsHolder<false>
{
public:
    template<class InputIt>
    void add(InputIt, InputIt)
    {
    }

    template<class Needle>
    static bool isKnown(const Needle &)
    {
        return false;
    }
};

}
