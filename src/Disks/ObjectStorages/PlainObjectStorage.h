#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/ObjectStorageKeyGenerator.h>

namespace DB
{

/// Do not encode keys, store as-is, and do not require separate disk for metadata.
/// But because of this does not support renames/hardlinks/attrs/...
///
/// NOTE: This disk has excessive API calls.
template <typename BaseObjectStorage>
class PlainObjectStorage : public BaseObjectStorage
{
public:
    template <class ...Args>
    explicit PlainObjectStorage(Args && ...args)
        : BaseObjectStorage(std::forward<Args>(args)...) {}

    std::string getName() const override { return "" + BaseObjectStorage::getName(); }

    /// Notes:
    /// - supports BACKUP to this disk
    /// - does not support INSERT into MergeTree table on this disk
    bool isWriteOnce() const override { return true; }

    bool isPlain() const override { return true; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & /* key_prefix */) const override
    {
        return ObjectStorageKey::createAsRelative(BaseObjectStorage::getCommonKeyPrefix(), path);
    }
};

}
