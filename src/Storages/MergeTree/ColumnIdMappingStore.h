#pragma once

#include <Disks/IStoragePolicy.h>
#include <Storages/MergeTree/ColumnIdMapping.h>
#include <Common/Logger.h>

#include <memory>
#include <optional>

namespace DB
{

class MergeTreeData;

/// A table's durable home for its column-ID mapping: persistence only, and the sole owner of WHERE
/// the mapping lives.  One implementation per engine: a file on the storage policy for `MergeTree`,
/// Keeper for a future `ReplicatedMergeTree`.  What to write is `ColumnIdMappingUpdate`'s business.
class ColumnIdMappingStore
{
public:
    virtual ~ColumnIdMappingStore() = default;

    /// The stored mapping, `nullopt` when the table has none -- the answer for every table that never
    /// opted into column IDs.  Returns it rather than publishing it.
    virtual std::optional<ColumnIdMapping> load() = 0;

    /// Store @mapping durably, wherever this table's mapping belongs.
    virtual void store(const ColumnIdMapping & mapping) = 0;

    /// Undo an activation that never committed.  Best-effort by contract.
    virtual void remove() noexcept = 0;
};

using ColumnIdMappingStorePtr = std::unique_ptr<ColumnIdMappingStore>;

/// `column_ids.json` on the table's storage policy.
class DiskColumnIdMappingStore : public ColumnIdMappingStore
{
public:
    DiskColumnIdMappingStore(MergeTreeData & data_, LoggerPtr log_);

    std::optional<ColumnIdMapping> load() override;
    void store(const ColumnIdMapping & mapping) override;
    void remove() noexcept override;

private:
    /// The one disk that holds `column_ids.json`: the storage policy's first.
    DiskPtr getAuthoritativeDisk() const;

    MergeTreeData & data;
    LoggerPtr log;
};

}
