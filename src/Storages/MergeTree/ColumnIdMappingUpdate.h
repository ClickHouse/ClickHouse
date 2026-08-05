#pragma once

#include <Core/Names.h>
#include <Disks/IStoragePolicy.h>
#include <Storages/MergeTree/ColumnIdMapping.h>
#include <Common/Logger.h>

#include <optional>

namespace DB
{

class MergeTreeData;
struct ColumnIdAlterPlan;
struct StorageInMemoryMetadata;

/// Publishes @data's stored mapping into its metadata. `attach` distinguishes a pre-existing table
/// (which must have a stored mapping once it opted into column IDs) from CREATE, which persists the
/// mapping only after the storage is constructed.
void loadColumnIdMapping(MergeTreeData & data, bool attach);

/// Republishes the mapping trimmed to what @data's metadata still names. Throws `CORRUPTED_DATA`
/// when metadata names a column the mapping does not cover, or when two live columns share one ID.
void reconcileColumnIdMappingWithMetadata(MergeTreeData & data);

/// The state machine owns one change to a table's column-ID mapping, from the plan to the publish.
class ColumnIdMappingUpdate
{
public:
    explicit ColumnIdMappingUpdate(MergeTreeData & data_, LoggerPtr log_);

    /// Restores `column_ids.json` to its pre-update state unless `commit()` ran.
    ~ColumnIdMappingUpdate();

    ColumnIdMappingUpdate(const ColumnIdMappingUpdate &) = delete;
    ColumnIdMappingUpdate & operator=(const ColumnIdMappingUpdate &) = delete;

    void persistBeforeSchemaCommit(ColumnIdAlterPlan & plan, StorageInMemoryMetadata & metadata_to_publish, const StoragePolicyPtr & target_policy_);

    void persistAfterSchemaCommit(const ColumnIdAlterPlan & plan, StorageInMemoryMetadata & metadata_to_publish);

    void commit() { state = State::Committed; }

    /// Old-to-new names whose table-level size aggregates the caller moves after its publish.
    const NameToNameVector & columnSizeRenames() const { return column_size_renames; }

private:
    enum class State
    {
        Empty,
        Copied,
        Written,
        Pruned,
        Committed,
    };

    /// True when writing `planned` would only replace the published pointer with an equal mapping.
    bool isAlreadyPublished(const ColumnIdMapping & planned, const ColumnIdAlterPlan & plan) const;

    /// Places the unchanged mapping on the disk the table uses after a policy switch.
    void copyToTargetPolicy();

    void writeToDisk(const ColumnIdMapping & mapping_to_write) const;

    void stampInto(StorageInMemoryMetadata & metadata_to_publish) const;

    void restoreFile() noexcept;

    MergeTreeData & data;
    LoggerPtr log;

    /// Rollback target. Not the live policy: a failed ALTER may not have got its settings back --
    /// see `MergeTreeData::tryRevertSettings`.
    StoragePolicyPtr published_policy;
    /// Set only when the ALTER switches policy; null means "the disk it is already on".
    StoragePolicyPtr target_policy;
    ColumnIdMappingPtr published_before;
    std::optional<ColumnIdMapping> mapping;
    NameToNameVector column_size_renames;
    State state = State::Empty;
};

}
