#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;

/// Reads the incremental refreshable-MV cursor for a refresh. A transactional target (e.g. Iceberg) commits
/// the cursor atomically with the data in one all-or-nothing commit (exactly-once, so the refresh uses a single
/// writer); otherwise the cursor is kept separately in the Keeper coordination znode (at-least-once).
class RefreshCursorStore
{
public:
    virtual ~RefreshCursorStore() = default;

    /// The cursor is committed atomically with the appended data, so the refresh must not also
    /// persist it in the Keeper coordination znode.
    virtual bool isTransactional() const = 0;

    /// The cursor persisted by the previous refresh (null if none yet).
    virtual CursorTreeNodePtr load(ContextPtr context) = 0;
};

using RefreshCursorStorePtr = std::shared_ptr<RefreshCursorStore>;

}
