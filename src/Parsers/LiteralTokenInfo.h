#pragma once

#include <cstddef>
#include <cstdint>

namespace DB
{

class ASTLiteral;

/// Token position info for literals - stores raw character pointers into the query string.
/// Used for ConstantExpressionTemplate construction and LIKE/REGEXP syntax highlighting.
/// Stored externally to reduce ASTLiteral size by ~48 bytes per literal.
///
/// IMPORTANT: These are raw pointers into the original query string. They are only valid
/// during parsing while the query buffer exists. Do not store or access after parsing.
struct LiteralTokenInfo
{
    const char * begin = nullptr; /// Start of literal in query string
    const char * end = nullptr;   /// End of literal in query string

    LiteralTokenInfo() = default;

    LiteralTokenInfo(const char * begin_, const char * end_)
        : begin(begin_)
        , end(end_)
    {
    }
};

/** Map from an `ASTLiteral` to the position of its token in the query string.
  *
  * An open-addressing table serving only what the parser asks of it: insert-or-overwrite, look up,
  * and throw the whole thing away. Nothing erases, so there are no tombstones and probing stops at
  * the first empty slot; nothing iterates; and a key is never null, so a null key is what marks a
  * slot empty. That is the entire design.
  *
  * It is worth having its own type because of how it is used. The map is built from scratch for
  * every column expression that the `Values` format has to parse and for every query the client
  * highlights, and most of them hold a handful of literals - so the cost that matters is
  * construction and destruction, not lookup. Those first few entries live inside the object and
  * cost no allocation at all. This started out as `absl::flat_hash_map`, which was measurably
  * faster than `std::unordered_map` here (roughly 13 ns against 29 ns for a single entry), but
  * abseil is a large dependency to carry for one map.
  *
  * NOTE: keys are `ASTLiteral` addresses. While parsing nested literals such as the tuple `(1, 2)`
  * the allocator may hand out an address that a discarded intermediate node used before, so an
  * insert overwrites any earlier entry - the surviving node is the one written last.
  */
struct LiteralTokenMap
{
public:
    LiteralTokenMap() = default;

    ~LiteralTokenMap()
    {
        if (slots != inline_slots)
            delete[] slots;
    }

    LiteralTokenMap(const LiteralTokenMap &) = delete;
    LiteralTokenMap & operator=(const LiteralTokenMap &) = delete;

    void insert_or_assign(const ASTLiteral * key, LiteralTokenInfo value) /// NOLINT(readability-identifier-naming)
    {
        /// Keep the table at most half full, so probing always terminates on an empty slot.
        if ((size + 1) * 2 > capacity)
            grow();

        Slot & slot = slotFor(slots, capacity, key);
        if (!slot.key)
        {
            slot.key = key;
            ++size;
        }
        slot.value = value;
    }

    /// Make `find` stop returning anything for `key`.
    ///
    /// A parser that throws a subtree away has to do this for the literals in it: the allocator can
    /// hand the same address to the next `ASTLiteral`, and one created without token info of its own
    /// would otherwise inherit the dead entry and be taken for a literal it has nothing to do with.
    /// Nothing is erased - an empty range is recorded instead - so probing still stops at the first
    /// empty slot and there are still no tombstones.
    void forget(const ASTLiteral * key) { insert_or_assign(key, LiteralTokenInfo{}); }

    /// Returns nullptr when the literal was not recorded, or was forgotten.
    const LiteralTokenInfo * find(const ASTLiteral * key) const
    {
        const Slot & slot = slotFor(slots, capacity, key);
        return slot.key && slot.value.begin ? &slot.value : nullptr;
    }

private:
    struct Slot
    {
        const ASTLiteral * key = nullptr;
        LiteralTokenInfo value;
    };

    /// Enough for the handful of literals a single expression usually has, while staying small
    /// enough that zeroing it is cheaper than one allocation.
    static constexpr size_t INLINE_CAPACITY = 8;

    Slot inline_slots[INLINE_CAPACITY];
    Slot * slots = inline_slots;
    size_t capacity = INLINE_CAPACITY;
    size_t size = 0;

    static size_t hash(const ASTLiteral * key)
    {
        /// Addresses are aligned, so the low bits carry no information - mix the high ones down.
        auto x = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(key));
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 29;
        return static_cast<size_t>(x);
    }

    /// The slot holding `key`, or the empty slot where it belongs. `capacity` is a power of two.
    template <typename SlotType>
    static SlotType & slotFor(SlotType * slots_, size_t capacity_, const ASTLiteral * key)
    {
        size_t mask = capacity_ - 1;
        size_t position = hash(key) & mask;
        while (slots_[position].key && slots_[position].key != key)
            position = (position + 1) & mask;
        return slots_[position];
    }

    void grow()
    {
        size_t new_capacity = capacity * 2;
        Slot * new_slots = new Slot[new_capacity];

        for (size_t i = 0; i < capacity; ++i)
            if (slots[i].key)
                slotFor(new_slots, new_capacity, slots[i].key) = slots[i];

        if (slots != inline_slots)
            delete[] slots;
        slots = new_slots;
        capacity = new_capacity;
    }
};

}
