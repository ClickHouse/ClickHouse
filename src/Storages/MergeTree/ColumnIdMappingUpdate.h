#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/ColumnIdMapping.h>
#include <Common/Logger.h>

#include <optional>

namespace DB
{

class MergeTreeData;
struct ColumnIdAlterPlan;
struct StorageInMemoryMetadata;

/// The state machine owns one change to a table's column-ID mapping, from the plan to the publish:
/// WHAT to store and when, while `ColumnIdMappingStore` owns where it lands and how.
class ColumnIdMappingUpdate
{
public:
    explicit ColumnIdMappingUpdate(MergeTreeData & data_, LoggerPtr log_);

    /// Restores `column_ids.json` to its pre-update state unless `commit()` ran.
    ~ColumnIdMappingUpdate();

    ColumnIdMappingUpdate(const ColumnIdMappingUpdate &) = delete;
    ColumnIdMappingUpdate & operator=(const ColumnIdMappingUpdate &) = delete;

    void persistBeforeSchemaCommit(ColumnIdAlterPlan & plan, StorageInMemoryMetadata & metadata_to_publish);

    void persistAfterSchemaCommit(const ColumnIdAlterPlan & plan, StorageInMemoryMetadata & metadata_to_publish);

    void commit() { state = State::Committed; }

    /// Old-to-new names whose table-level size aggregates the caller moves after its publish.
    const NameToNameVector & columnSizeRenames() const { return column_size_renames; }

private:
    enum class State
    {
        Empty,
        Written,
        Pruned,
        Committed,
    };

    /// True when writing `planned` would only replace the published pointer with an equal mapping.
    bool isAlreadyPublished(const ColumnIdMapping & planned, const ColumnIdAlterPlan & plan) const;

    void stampInto(StorageInMemoryMetadata & metadata_to_publish) const;

    void restoreFile() noexcept;

    MergeTreeData & data;
    LoggerPtr log;

    ColumnIdMappingPtr published_before;
    std::optional<ColumnIdMapping> mapping;
    NameToNameVector column_size_renames;
    State state = State::Empty;
};

}
