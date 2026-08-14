#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <memory>

namespace DB
{

/// Reads the incremental refreshable-MV cursor for a refresh. Two backings exist:
///  - the Keeper coordination znode (default): the cursor is persisted separately from the appended
///    data, so a crash between the append and the cursor write replays the round -> at-least-once;
///  - a transactional target table (e.g. Iceberg on a REST/filesystem catalog): the write path commits
///    the cursor atomically with the data files in one catalog compare-and-swap -> exactly-once, and
///    the refresh keeps the cursor out of Keeper entirely and reads it back from the target here.
class RefreshCursorStore
{
public:
    virtual ~RefreshCursorStore() = default;

    /// The cursor is committed atomically with the appended data, so the refresh must not also
    /// persist it in the Keeper coordination znode.
    virtual bool isTransactional() const = 0;

    /// The serialized cursor persisted by the previous refresh (empty if none yet). Same opaque
    /// encoding as `serializeStreamingCursor`, so the caller can `deserializeStreamingCursor` it.
    virtual String load(ContextPtr context) = 0;
};

using RefreshCursorStorePtr = std::shared_ptr<RefreshCursorStore>;

}
