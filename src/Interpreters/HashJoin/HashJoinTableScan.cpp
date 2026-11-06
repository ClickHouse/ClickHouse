#include <Interpreters/HashJoin/HashJoinTable.h>

#include <Columns/ColumnsCommon.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event JoinNonJoinedHashTableScans;
extern const Event JoinNonJoinedHashTableScansSkipped;
}

namespace DB
{

namespace detail
{

/// Whether every key in the buckets `b` with `b % num_shares == share` has its used flag set.
/// A two-level map numbers the cells of bucket `b` from `bucket_cells_prefix[b] + 1`, and its zero key has offset 0.
/// A one-bucket map is checked as a whole, and so is a `PartitionedFixedHashTable`, which keeps no count per bucket.
template <typename Map>
bool allKeysUsed(const Map & map, const JoinStuff::JoinUsedFlags & used_flags, size_t share, size_t num_shares)
{
    if (used_flags.per_offset_flags.size() < map.getBufferSizeInCells() + 1)
        return false;

    if constexpr (requires { map.bucket_cells_prefix; })
    {
        if constexpr (Map::NUM_BUCKETS > 1)
        {
            if (map.bucket_cells_prefix.size() != Map::NUM_BUCKETS)
                return false;
            for (size_t bucket = share; bucket < Map::NUM_BUCKETS; bucket += num_shares)
            {
                const auto & impl = map.impls[bucket];
                const size_t begin = map.bucket_cells_prefix[bucket] + 1;
                if (countBytesInFilter(used_flags.per_offset_flags.data(), begin, begin + impl.getBufferSizeInCells())
                    != impl.size() - impl.hasZero())
                    return false;
                if (impl.hasZero() && !used_flags.getUsedSafe(0))
                    return false;
            }
            return true;
        }
    }
    return countBytesInFilter(used_flags.per_offset_flags.data(), 0, map.getBufferSizeInCells() + 1) == map.size();
}

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
    using Map = JoinMapOf<TYPE, Maps>;
    using Mapped = typename Map::mapped_type;
    using Iterator = typename Map::const_iterator;

    const Map & map = typedMap<TYPE, Maps>(*this);
    auto is_bucket_in_share = [&](size_t bucket) { return num_shares <= 1 || (bucket % num_shares) == share; };

    if (!cursor.has_value())
    {
        const bool all_keys_used = detail::allKeysUsed(map, used_flags, share, num_shares);
        ProfileEvents::increment(
            all_keys_used ? ProfileEvents::JoinNonJoinedHashTableScansSkipped : ProfileEvents::JoinNonJoinedHashTableScans);
        cursor = std::make_any<Iterator>(all_keys_used ? map.end() : map.begin());
    }

    Iterator & it = std::any_cast<Iterator &>(cursor);
    auto end = map.end();
    size_t rows_added = 0;

    /// case: two-level hash tables with parallel iteration
    if constexpr (requires { map.iteratorAt(0); })
    {
        auto skip_to_next_owned_bucket = [&]() -> bool
        {
            while (it != end && !is_bucket_in_share(it.getBucket()))
            {
                /// smallest bucket > current that satisfies: bucket ≡ share (mod num_shares)
                size_t cur = it.getBucket();
                size_t next = cur - (cur % num_shares) + share;
                if (next <= cur)
                    next += num_shares;
                it = map.iteratorAt(next);
            }
            return it != end;
        };

        /// position at the first bucket owned by this stream
        if (!skip_to_next_owned_bucket())
            return 0;

        while (it != end && rows_added < max_rows)
        {
            size_t offset = map.offsetInternal(it.getPtr());
            if (!used_flags.getUsedSafe(offset))
                rows_added += detail::collectNonJoinedRefWords<Mapped>(it->getMapped(), words);

            ++it;

            /// if we crossed into a bucket not owned by this stream, skip ahead
            if (it != end && !is_bucket_in_share(it.getBucket()) && !skip_to_next_owned_bucket())
                break;
        }
    }
    else
    {
        /// A `PartitionedFixedHashTable` has no `iteratorAt`, so the walk tests the bucket of every cell.
        for (; it != end; ++it)
        {
            if (!is_bucket_in_share(it.getBucket()))
                continue;

            size_t offset = map.offsetInternal(it.getPtr());
            if (used_flags.getUsedSafe(offset))
                continue;

            rows_added += detail::collectNonJoinedRefWords<Mapped>(it->getMapped(), words);

            if (rows_added >= max_rows)
            {
                ++it;
                break;
            }
        }
    }

    return rows_added;
}

template size_t HashJoinTable::scanNotJoined<HashJoin::MapsOne>(
    Cursor &, size_t, size_t, const JoinStuff::JoinUsedFlags &, size_t, PaddedPODArray<UInt64> &) const;
template size_t HashJoinTable::scanNotJoined<HashJoin::MapsAll>(
    Cursor &, size_t, size_t, const JoinStuff::JoinUsedFlags &, size_t, PaddedPODArray<UInt64> &) const;
template size_t HashJoinTable::scanNotJoined<HashJoin::MapsAsof>(
    Cursor &, size_t, size_t, const JoinStuff::JoinUsedFlags &, size_t, PaddedPODArray<UInt64> &) const;

}
