#pragma once

#include <Storages/MaterializedView/RefreshCursorStore.h>

#include <memory>

namespace DB
{

class StorageObjectStorage;

/// Transactional refresh-cursor store backed by a data-lake target table. The write path embeds the
/// cursor into the same commit as the appended data (Iceberg snapshot summary), so it is exactly-once
/// and never touches Keeper; `load` reads it back from the current table snapshot.
class DataLakeRefreshCursorStore : public RefreshCursorStore
{
public:
    explicit DataLakeRefreshCursorStore(std::shared_ptr<StorageObjectStorage> storage_);

    bool isTransactional() const override { return true; }
    CursorTreeNodePtr load(ContextPtr context) override;

private:
    std::shared_ptr<StorageObjectStorage> storage;
};

/// Serialize/deserialize the cursor tree to the opaque binary form used by the Keeper coordination znode.
String serializeCursorTree(const CursorTreeNodePtr & cursor);
CursorTreeNodePtr deserializeCursorTree(const String & serialized);

/// Encode/decode that binary form to a text form safe to store in a JSON string field (an Iceberg
/// snapshot summary value); `to` for the write path, `from` for `load`. Hex keeps the round-trip byte-exact.
String refreshCursorToStorage(const String & serialized_cursor);
String refreshCursorFromStorage(const String & stored);

}
