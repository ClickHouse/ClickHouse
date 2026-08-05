#pragma once

#include <Disks/IStoragePolicy.h>
#include <Storages/MergeTree/ColumnIdMapping.h>
#include <Common/Logger.h>

#include <memory>
#include <optional>

namespace DB
{

class MergeTreeData;

/// A table's durable home for its column-ID mapping.  One implementation per engine: a file on the
/// storage policy for `MergeTree`, Keeper for a future `ReplicatedMergeTree`.
class ColumnIdMappingStore
{
public:
    virtual ~ColumnIdMappingStore() = default;

    /// The stored mapping, `nullopt` when the table has none; throws when the settings say it must
    /// exist.  Returns it rather than publishing it.
    virtual std::optional<ColumnIdMapping> load(bool attach) = 0;

    /// Store @mapping durably.  @target_policy is the policy the table uses once the ALTER commits.
    virtual void store(const ColumnIdMapping & mapping, const StoragePolicyPtr & target_policy) = 0;

    /// Undo an activation that never committed.  Best-effort by contract.
    virtual void remove() noexcept = 0;
};

using ColumnIdMappingStorePtr = std::unique_ptr<ColumnIdMappingStore>;

/// `column_ids.json` on the table's storage policy.
class DiskColumnIdMappingStore : public ColumnIdMappingStore
{
public:
    DiskColumnIdMappingStore(MergeTreeData & data_, LoggerPtr log_);

    std::optional<ColumnIdMapping> load(bool attach) override;
    void store(const ColumnIdMapping & mapping, const StoragePolicyPtr & target_policy) override;
    void remove() noexcept override;

private:
    /// The one disk that holds `column_ids.json`: the policy's first.
    DiskPtr getAuthoritativeDisk(const StoragePolicyPtr & policy) const;

    MergeTreeData & data;
    LoggerPtr log;
};

}
