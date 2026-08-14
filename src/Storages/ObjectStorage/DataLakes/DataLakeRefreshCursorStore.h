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
    String load(ContextPtr context) override;

private:
    std::shared_ptr<StorageObjectStorage> storage;
};

/// Encode/decode the opaque (binary) cursor from `serializeStreamingCursor` to a text form safe to
/// store in a JSON string field (an Iceberg snapshot summary value); `to` for the write path, `from`
/// for `load`. Hex keeps the round-trip byte-exact.
String refreshCursorToStorage(const String & serialized_cursor);
String refreshCursorFromStorage(const String & stored);

}
