#pragma once

#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/Prefetching.h>
#include <base/defines.h>

#include <algorithm>
#include <bit>
#include <type_traits>

namespace DB
{

class Arena;

namespace ColumnsHashing
{

/** How a prefetching loop over the rows of a block gets their keys. It builds the key of a row ahead of the
  * current one to prefetch the cell of that key, and inserts the key of the current row. `LookAheadKeysFor`
  * picks one of two ways to get them:
  *
  * - `RebuiltLookAheadKeys` builds the key of the current row again for the insert.
  * - `KeptLookAheadKeys` keeps the keys that it builds ahead and inserts the kept key, so that a key that
  *   stores its hash (`key_holder_key_stores_hash`) is built and hashed once.
  *
  * Usage, with `look_ahead` from `PrefetchingHelper`:
  *
  *     LookAheadKeysFor<prefetch, KeyHolder> look_ahead_keys(row_begin);
  *     for (size_t i = row_begin; i < row_end; ++i)
  *     {
  *         if constexpr (prefetch)
  *             look_ahead_keys.prefetch(state, data, i, look_ahead, row_end, pool);
  *         auto emplace_result = look_ahead_keys.emplaceKey(state, data, i, pool);
  *         ...
  *     }
  */
class RebuiltLookAheadKeys
{
public:
    explicit RebuiltLookAheadKeys(size_t /*first_row*/) {}

    /// Prefetches the cell of the key of row `row + look_ahead`, if that row is below `end`.
    template <typename State, typename Data>
    ALWAYS_INLINE void prefetch(State & state, Data & data, size_t row, size_t look_ahead, size_t end, Arena & pool)
    {
        if (row + look_ahead < end)
            data.prefetch(state.getKeyHolder(row + look_ahead, pool));
    }

    /// Builds the key of `row` and inserts it.
    template <typename State, typename Data>
    ALWAYS_INLINE auto emplaceKey(State & state, Data & data, size_t row, Arena & pool)
    {
        return state.emplaceKey(data, row, pool);
    }
};

template <typename KeyHolder>
class KeptLookAheadKeys
{
public:
    explicit KeptLookAheadKeys(size_t first_row) : built_end(first_row) {}

    /// Builds and keeps the keys of the rows from the first one without a key to `row + look_ahead`,
    /// below `end`, and prefetches their cells. The caller passes the rows in increasing order, from
    /// `first_row` on, and a look-ahead of `PrefetchingHelper`, which is at most its maximum.
    template <typename State, typename Data>
    ALWAYS_INLINE void prefetch(State & state, Data & data, size_t row, size_t look_ahead, size_t end, Arena & pool)
    {
        const size_t want_end = std::min(end, row + look_ahead + 1);
        for (; built_end < want_end; ++built_end)
        {
            const Key key = state.getKeyHolder(built_end, pool).key;
            ring[built_end % ring_size] = key;
            data.prefetch(key);
        }
    }

    /// Inserts the key of `row` that `prefetch` kept. The caller called `prefetch` at `row` or at an earlier row,
    /// and the ring still holds the key, because `prefetch` builds fewer than `ring_size` rows ahead of the
    /// current one.
    template <typename State, typename Data>
    ALWAYS_INLINE auto emplaceKey(State & state, Data & data, size_t row, Arena & pool)
    {
        chassert(row < built_end && built_end <= row + ring_size);
        KeyHolder key_holder{ring[row % ring_size], pool};
        return state.emplaceKeyHolder(data, key_holder);
    }

private:
    using Key = decltype(KeyHolder::key);

    /// A power of two above the largest look-ahead, so that the keys from the current row to the last one
    /// built fit in the ring: 64 for the largest look-ahead of 32.
    static constexpr size_t ring_size = std::bit_ceil(PrefetchingHelper::getMaxLookAheadValue() + 1);

    Key ring[ring_size]{};
    /// The rows below it have their keys built.
    size_t built_end;
};

/// The way a prefetching loop gets the keys of `KeyHolder` (see `RebuiltLookAheadKeys`): it keeps the keys
/// that store their hash, and builds the others again.
template <bool prefetch, typename KeyHolder>
using LookAheadKeysFor
    = std::conditional_t<prefetch && key_holder_key_stores_hash<KeyHolder>, KeptLookAheadKeys<KeyHolder>, RebuiltLookAheadKeys>;

}

}
