#pragma once

#include <ranges>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/RowRefs.h>
#include <Common/HashTable/HashMap.h>

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
    using SetHolder = std::set<Type>;
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
                set_holder_ptr = std::make_unique<SetHolder>();
                set_holder_ptr->insert(std::cbegin(array_holder), std::cbegin(array_holder) + items);
            }
            set_holder_ptr->insert(from, to);
        }
        items += new_items;
    }

    template<class Needle>
    bool isKnown(const Needle & needle)
    {
        return items <= MAX_LINEAR
            ? std::find(std::cbegin(array_holder), std::cbegin(array_holder) + items, needle) != std::cbegin(array_holder) + items
            : set_holder_ptr->find(needle) != set_holder_ptr->end();
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

template <typename Map, bool add_missing, bool flag_per_row, typename AddedColumns>
void addFoundRowAll(
    const typename Map::mapped_type & mapped,
    AddedColumns & added,
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows [[maybe_unused]],
    JoinStuff::JoinUsedFlags * used_flags [[maybe_unused]])
{
    if constexpr (add_missing)
        added.applyLazyDefaults();

    if constexpr (flag_per_row)
    {
        std::vector<UInt64> new_known_rows;

        for (const UInt64 ref_word : refsOf(mapped.word))
        {
            if (!known_rows.isKnown(ref_word))
            {
                added.appendFromBlock(ref_word, false);
                ++current_offset;
                new_known_rows.push_back(ref_word);

                if (used_flags)
                {
                    used_flags->JoinStuff::JoinUsedFlags::setUsedOnce<true, flag_per_row>(
                        refWordBlockNo(ref_word), refWordRowNo(ref_word), 0);
                }
            }
        }

        known_rows.add(std::cbegin(new_known_rows), std::cend(new_known_rows));
    }
    else if constexpr (AddedColumns::isLazy())
    {
        /// Load-free fast path: the cell word carries the saturating row count, so unique keys
        /// (inline refs) and duplicate keys are both appended without dereferencing the node.
        added.appendFromBlock(mapped.word, false);
        current_offset += mapped.rows();
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
