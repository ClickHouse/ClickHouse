#pragma once

#include <Columns/IColumn.h>

namespace DB
{

/// The comparison keys that the in-memory state of a group limit (`DISTINCT` or `LIMIT BY`) retains and that
/// its spill runs compare: the typed key columns or their 128-bit fingerprints.
enum class SpillKeyRepresentation
{
    Columns,
    Hash128,
};

/// A batch of groups retained by the in-memory state of a group limit: their keys, and the number of input
/// rows counted for each group. `seen_rows` has one row per group: a constant 1 for `DISTINCT`, the group
/// counts for `LIMIT BY`. A count is exact below the window end of the group and at least the window end
/// otherwise, which is all that ranking the later rows needs.
struct SeenGroups
{
    MutableColumns keys;
    ColumnPtr seen_rows;
};

/// Reads frozen grouping state as owning key and count columns. The caller sorts the returned groups, so
/// their extraction order has no meaning.
class SeenGroupExtractor
{
public:
    virtual ~SeenGroupExtractor() = default;

    /// Returns at most `max_rows` groups, stopping after a complete key reaches `max_bytes` of allocated
    /// column memory. The byte target is soft because a key or an allocation can exceed it; zero disables it.
    /// `max_rows` must be positive. Empty `keys` mark exhaustion.
    virtual SeenGroups next(size_t max_rows, size_t max_bytes) = 0;

    /// The memory of the grouping state that the extractor retains.
    virtual size_t getTotalByteCount() const = 0;
};

}
