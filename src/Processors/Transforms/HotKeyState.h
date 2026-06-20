#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Set.h>
#include <Common/MultiVersion.h>

namespace DB
{

/// A set of "hot" keys that only ever grows, with an exact and thread-safe membership test.
///
/// Keys are added one at a time with `promote` and tested in bulk with `buildHotMask`. Membership is
/// backed by a `Set`, the same structure that implements the SQL `IN` operator, so the test is exact and
/// handles multi-column keys, every data type, and NULLs. Reads are lock-free: every promotion builds a
/// fresh immutable snapshot and publishes it atomically through `MultiVersion`, so many threads can test
/// membership at the same time while the rare promotions are serialized by a mutex.
///
/// Skew-robust sharded aggregation uses this to hold the set of keys it has detected as hot.
class HotKeyState
{
public:
    explicit HotKeyState(ColumnsWithTypeAndName key_header_);

    /// Returns a `UInt8` mask that holds 1 for each row whose key is hot, given the key columns in header
    /// order, or null when no key has been promoted yet. This is thread-safe, as it reads an immutable
    /// snapshot.
    ColumnPtr buildHotMask(const Columns & key_columns) const;

    /// Promotes the key found at the given row of the key columns, which are in header order. The hash is
    /// used to ignore repeated attempts to promote a key that is already hot. Each call rebuilds the whole
    /// membership snapshot from every key promoted so far and publishes it atomically. The rebuild is cheap
    /// because the hot set stays small: in sharded aggregation the detector promotes only keys above its
    /// imbalance threshold, and at most about 2 * `num_cold_shards` keys can clear it, where
    /// `num_cold_shards` is `max_threads`.
    void promote(const Columns & key_columns, size_t row, UInt32 hash);

private:
    const ColumnsWithTypeAndName key_header;

    /// Immutable membership snapshot, swapped on every promotion. It is null until the first promotion.
    MultiVersion<Set> snapshot;

    /// Guards the promotion path, which is rare.
    std::mutex mutex;
    std::unordered_set<UInt32> promoted_hashes;
    MutableColumns accumulated_keys;
};

using HotKeyStatePtr = std::shared_ptr<HotKeyState>;

}
