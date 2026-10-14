#pragma once

#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/RowRefs.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>

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
    /// A holder is constructed for every probe row on the multi-disjunct path, so `array_holder`
    /// is deliberately left uninitialized (only the first `items` entries are ever read); value-
    /// initializing 128 bytes per row would be a pure waste.
    KnownRowsHolder() /// NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
        : items(0)
    {
    }

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

/// With `claim_flags` set, a row is appended only if this call wins its used flag, so it is emitted
/// for exactly one left row; pass nullptr when a row may be emitted more than once (ALL joins).
/// Returns whether anything was appended.
template <typename Map, bool add_missing, bool flag_per_row, typename AddedColumns>
bool addFoundRowAll(
    const typename Map::mapped_type & mapped,
    AddedColumns & added,
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows [[maybe_unused]],
    JoinStuff::JoinUsedFlags * claim_flags [[maybe_unused]],
    bool is_last_disjunct [[maybe_unused]])
{
    if constexpr (add_missing)
        added.applyLazyDefaults();

    if constexpr (flag_per_row)
    {
        std::vector<UInt64> new_known_rows;
        bool any_row_added = false;

        for (const UInt64 ref_word : refsOf(mapped.word))
        {
            if (known_rows.isKnown(ref_word))
                continue;

            if (claim_flags
                && !claim_flags->JoinStuff::JoinUsedFlags::setUsedOnce<true, flag_per_row>(
                    refWordBlockNo(ref_word), refWordRowNo(ref_word), 0))
                continue;

            added.appendFromBlock(ref_word, false);
            ++current_offset;
            any_row_added = true;
            if (!is_last_disjunct)
                new_known_rows.push_back(ref_word);
        }

        if (!is_last_disjunct)
            known_rows.add(std::cbegin(new_known_rows), std::cend(new_known_rows));

        return any_row_added;
    }
    else if constexpr (AddedColumns::isLazy())
    {
        /// Load-free fast path: the cell word carries the saturating row count, so unique keys
        /// (inline refs) and duplicate keys are both appended without dereferencing the node.
        added.appendFromBlock(mapped.word, false);
        current_offset += mapped.rows();
        return true;
    }
    else
    {
        /// No single-row fast path needed here (unlike the pre-RowRef code): a single ref lives
        /// inline in the cell word and the iterator decodes it without touching the arena node.
        for (const UInt64 ref_word : refsOf(mapped.word))
        {
            added.appendFromBlock(ref_word, false);
            ++current_offset;
        }
        return true;
    }
}

template <bool add_missing, bool need_offset, typename AddedColumns>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (add_missing)
    {
        added.appendDefaultRow();
        if constexpr (need_offset)
            ++current_offset;
    }
}

}
