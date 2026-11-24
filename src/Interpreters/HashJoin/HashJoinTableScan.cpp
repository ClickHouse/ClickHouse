#include <Interpreters/HashJoin/HashJoinTable.h>

#include <Columns/ColumnsCommon.h>
#include <Common/ProfileEvents.h>

#include <algorithm>

namespace ProfileEvents
{
extern const Event JoinNonJoinedHashTableScans;
extern const Event JoinNonJoinedHashTableScansSkipped;
}

namespace DB
{

namespace detail
{

/// Whether every key of `map` has its used flag set. The flags of the cells of `map` start at `first_flag`, and the zero key's
/// flag is 0.
template <typename Map>
bool allKeysUsed(const Map & map, const JoinStuff::JoinUsedFlags & used_flags, size_t first_flag)
{
    const size_t cells = map.getBufferSizeInCells();
    if (used_flags.per_offset_flags.size() < first_flag + cells)
        return false;
    size_t zero = 0;
    if constexpr (requires { map.hasZero(); })
    {
        zero = map.hasZero();
        if (zero && !used_flags.getUsedSafe(0))
            return false;
    }
    return countBytesInFilter(used_flags.per_offset_flags.data(), first_flag, first_flag + cells) == map.size() - zero;
}

/// Whether every key of `map` has its used flag set, when the flags number its cells from 1 and give the zero key 0.
template <typename Map>
bool allKeysUsedInTable(const Map & map, const JoinStuff::JoinUsedFlags & used_flags)
{
    if (used_flags.per_offset_flags.size() < map.getBufferSizeInCells() + 1)
        return false;
    return countBytesInFilter(used_flags.per_offset_flags.data(), 0, map.getBufferSizeInCells() + 1) == map.size();
}

/// Where a scan by cell range resumes: the next cell over all partitions, and whether the zero key was looked at.
struct CellRangeCursor
{
    size_t next_cell = 0;
    size_t end_cell = 0;
    bool zero_key_done = false;
};

/// Appends one hash map cell's not-joined rows as a flat run of encoded ref words. Returns the rows appended.
template <typename Mapped>
size_t collectNonJoinedRefWords([[maybe_unused]] const Mapped & mapped, [[maybe_unused]] PaddedPODArray<UInt64> & words)
{
    if constexpr (std::is_same_v<Mapped, AsofRowRefs>)
    {
        return 0;
    }
    else if constexpr (std::is_same_v<Mapped, RowRef>)
    {
        words.push_back(mapped.encode());
        return 1;
    }
    else
    {
        size_t rows = 0;
        for (auto it = mapped.begin(); it.ok(); ++it, ++rows)
            words.push_back(*it);
        return rows;
    }
}


/// The scan of a partitioned view with fewer partitions than shares: share `share` walks a contiguous range of the cells over
/// all partitions, in the order of their used flags. Every share skips when every key of the table is used, and share 0 emits
/// the zero key.
template <typename View>
size_t scanCellRange(
    const View & view,
    size_t share,
    size_t num_shares,
    const JoinStuff::JoinUsedFlags & used_flags,
    size_t max_rows,
    PaddedPODArray<UInt64> & words,
    std::any & cursor)
{
    using Mapped = typename View::mapped_type;
    constexpr HashTableNoState no_state{};

    if (!cursor.has_value())
    {
        const size_t cells = view.getBufferSizeInCells();
        const bool all_keys_used = allKeysUsedInTable(view, used_flags);
        ProfileEvents::increment(
            all_keys_used ? ProfileEvents::JoinNonJoinedHashTableScansSkipped : ProfileEvents::JoinNonJoinedHashTableScans);
        CellRangeCursor range;
        if (all_keys_used)
            range.zero_key_done = true;
        else
        {
            range.next_cell = share * cells / num_shares;
            range.end_cell = (share + 1) * cells / num_shares;
            range.zero_key_done = share != 0;
        }
        cursor = range;
    }
    auto & range = std::any_cast<CellRangeCursor &>(cursor);

    size_t rows_added = 0;
    if (!range.zero_key_done)
    {
        range.zero_key_done = true;
        if (!used_flags.getUsedSafe(0))
            for (size_t p = 0; p < view.numPartitions(); ++p)
                if (view.partition(p).hasZero())
                    rows_added += collectNonJoinedRefWords<Mapped>(view.partition(p).zeroValue()->getMapped(), words);
    }

    while (range.next_cell < range.end_cell && rows_added < max_rows)
    {
        const size_t p = range.next_cell >> view.cellBits();
        const auto desc = view.desc(p);
        const auto & cell = desc.cells[range.next_cell & desc.mask];
        const size_t offset = range.next_cell + 1;
        ++range.next_cell;
        if (cell.isZero(no_state) || used_flags.getUsedSafe(offset))
            continue;
        rows_added += collectNonJoinedRefWords<Mapped>(cell.getMapped(), words);
    }
    return rows_added;
}

}

template <typename Maps>
size_t HashJoinTable::scanNotJoined(
    Cursor & cursor,
    size_t share,
    size_t num_shares,
    const JoinStuff::JoinUsedFlags & used_flags,
    size_t max_rows,
    PaddedPODArray<UInt64> & words) const
{
    return dispatchType(
        [&]<Type TYPE>() { return scanNotJoinedTyped<TYPE, Maps>(cursor, share, num_shares, used_flags, max_rows, words); });
}

template <HashJoin::Type TYPE, typename Maps>
size_t HashJoinTable::scanNotJoinedTyped(
    Cursor & cursor,
    size_t share,
    size_t num_shares,
    const JoinStuff::JoinUsedFlags & used_flags,
    size_t max_rows,
    PaddedPODArray<UInt64> & words) const
{
    return visitForm<TYPE, Maps>([&]<typename Form>(const Form & form) -> size_t
    {
        using Mapped = typename Form::mapped_type;
        size_t rows_added = 0;

        if constexpr (is_join_partitioned_view<Form>)
        {
            using Iterator = typename Form::map_type::const_iterator;
            const size_t parts = form.numPartitions();
            /// With fewer partitions than shares, the shares split the cells instead.
            if (num_shares > 1 && parts < num_shares)
                return detail::scanCellRange(form, share, num_shares, used_flags, max_rows, words, cursor);

            /// Share `share` walks the partitions [`first`, `last`).
            const size_t first = share * parts / num_shares;
            const size_t last = (share + 1) * parts / num_shares;

            if (!cursor.has_value())
                cursor = std::make_any<PartitionCursor<Iterator>>(PartitionCursor<Iterator>{.partition = first, .it = Iterator()});
            auto & pc = std::any_cast<PartitionCursor<Iterator> &>(cursor);

            for (; pc.partition < last; ++pc.partition)
            {
                const auto & map = form.partition(pc.partition);
                if (pc.it == Iterator())
                {
                    const bool all_keys_used = detail::allKeysUsed(map, used_flags, form.prefixOf(pc.partition) + 1);
                    ProfileEvents::increment(
                        all_keys_used ? ProfileEvents::JoinNonJoinedHashTableScansSkipped : ProfileEvents::JoinNonJoinedHashTableScans);
                    if (all_keys_used)
                        continue;
                    pc.it = map.begin();
                }
                for (auto end = map.end(); pc.it != end;)
                {
                    /// The zero key's cell gets offset 0, so the share of its partition emits it.
                    const size_t offset = pc.it.getPtr()->isZero(HashTableNoState{}) ? 0 : form.offsetOf(pc.it.getPtr(), pc.partition);
                    if (!used_flags.getUsedSafe(offset))
                        rows_added += detail::collectNonJoinedRefWords<Mapped>(pc.it->getMapped(), words);
                    ++pc.it;
                    if (rows_added >= max_rows)
                        return rows_added;
                }
                pc.it = Iterator();
            }
            return rows_added;
        }
        else
        {
            using Iterator = typename Form::const_iterator;
            /// A table without partitions is scanned by share 0.
            if (!cursor.has_value())
            {
                /// Every share counts its verdict on the whole table, as each share of the one-bucket map did.
                const bool all_keys_used = detail::allKeysUsedInTable(form, used_flags);
                ProfileEvents::increment(
                    all_keys_used ? ProfileEvents::JoinNonJoinedHashTableScansSkipped : ProfileEvents::JoinNonJoinedHashTableScans);
                const bool in_share = num_shares <= 1 || share == 0;
                cursor = std::make_any<Iterator>(all_keys_used || !in_share ? form.end() : form.begin());
            }

            Iterator & it = std::any_cast<Iterator &>(cursor);
            for (auto end = form.end(); it != end; ++it)
            {
                const size_t offset = form.offsetInternal(it.getPtr());
                if (used_flags.getUsedSafe(offset))
                    continue;

                rows_added += detail::collectNonJoinedRefWords<Mapped>(it->getMapped(), words);

                if (rows_added >= max_rows)
                {
                    ++it;
                    break;
                }
            }
            return rows_added;
        }
    });
}

template size_t HashJoinTable::scanNotJoined<HashJoin::MapsOne>(
    Cursor &, size_t, size_t, const JoinStuff::JoinUsedFlags &, size_t, PaddedPODArray<UInt64> &) const;
template size_t HashJoinTable::scanNotJoined<HashJoin::MapsAll>(
    Cursor &, size_t, size_t, const JoinStuff::JoinUsedFlags &, size_t, PaddedPODArray<UInt64> &) const;
template size_t HashJoinTable::scanNotJoined<HashJoin::MapsAsof>(
    Cursor &, size_t, size_t, const JoinStuff::JoinUsedFlags &, size_t, PaddedPODArray<UInt64> &) const;

}
