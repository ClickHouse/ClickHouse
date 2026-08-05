#include <Storages/MergeTree/ColumnIdMappingUpdate.h>

#include <Storages/MergeTree/ColumnIdAlterPlanner.h>
#include <Storages/MergeTree/ColumnIdMappingStore.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/FailPoint.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char column_ids_throw_before_mapping_persist[];
    extern const char column_ids_throw_after_mapping_persist[];
}

namespace
{

/// The published pointer is the version token FREEZE and BACKUP compare, so an unchanged mapping has
/// to keep it -- and the planner returns a copy of the live mapping for every ALTER on an id-active
/// table, most of which change nothing about it.
bool sameMapping(const ColumnIdMapping & lhs, const ColumnIdMapping & rhs)
{
    return lhs.isActive() == rhs.isActive() && lhs.getNextColumnIdCounter() == rhs.getNextColumnIdCounter()
        && lhs.getLogicalToId() == rhs.getLogicalToId();
}

void failpointBeforeMappingPersist()
{
    fiu_do_on(FailPoints::column_ids_throw_before_mapping_persist,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure before column ID mapping persist");
    });
}

void failpointAfterMappingPersist()
{
    fiu_do_on(FailPoints::column_ids_throw_after_mapping_persist,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after column ID mapping persist");
    });
}


/// Runs on the load path BEFORE the mapping is published, so a torn or hand-edited `column_ids.json`
/// is refused with a file-naming error rather than tripping the schema-stamp desync assertion (a
/// debug abort).
void checkColumnIdMappingCoversMetadata(const MergeTreeData & data, const ColumnIdMapping & mapping)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr(nullptr, false);

    Names missing_from_mapping;
    for (const auto & col : metadata_snapshot->getColumns().getAllPhysical())
    {
        if (!mapping.hasLogicalName(col.name))
            missing_from_mapping.push_back(col.name);
    }
    if (!missing_from_mapping.empty())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Column ID mapping for table {} is missing entries for column(s): {}. "
            "This indicates a torn write of `column_ids.json` (mapping not "
            "updated while `metadata.sql` already committed the schema change). "
            "The mapping cannot be rebuilt safely because DROP + re-ADD of the "
            "same column name makes on-disk files indistinguishable from their "
            "column ID alone. Restore the table from backup or fix "
            "`column_ids.json` manually.",
            data.getStorageID().getNameForLogs(),
            fmt::join(missing_from_mapping, ", "));
}

/// A two-phase rename transiently maps two logical names to one column ID, so this runs only after
/// the reconciliation trim has dropped the old name: a duplicate surviving that is real corruption
/// that would silently let two SQL columns share one on-disk stream.
void checkNoLogicalNamesShareColumnId(const MergeTreeData & data, const ColumnIdMapping & mapping)
{
    std::unordered_map<String, Names> logicals_by_id;
    for (const auto & [logical, column_id] : mapping.getLogicalToId())
        logicals_by_id[column_id].push_back(logical);

    for (const auto & [column_id, logicals] : logicals_by_id)
    {
        if (logicals.size() > 1)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Column ID mapping for table {} has multiple logical columns "
                "mapped to the same column ID '{}': {}.  All of them are "
                "still present in the schema, which indicates a corrupted "
                "`column_ids.json` (not a transient two-phase-rename state). "
                "Restore from backup or repair the file manually.",
                data.getStorageID().getNameForLogs(),
                column_id,
                fmt::join(logicals, ", "));
    }
}

NameToNameVector pruneRetainedNames(ColumnIdMapping & mapping, const ColumnIdAlterPlan & plan)
{
    NameToNameVector column_size_renames;

    // rename
    for (const auto & old_name : plan.rename_old_names)
    {
        auto column_id = mapping.tryGetColumnId(old_name);
        mapping.finishRename(old_name);

        if (!column_id)
            continue;

        if (auto new_name = mapping.tryGetLogicalName(*column_id); new_name && *new_name != old_name)
            column_size_renames.emplace_back(old_name, *new_name);
    }

    // drop
    for (const auto & name : plan.drop_names)
    {
        if (mapping.hasLogicalName(name))
            mapping.removeColumn(name);
    }

    return column_size_renames;
}

}

void loadColumnIdMapping(MergeTreeData & data, bool attach)
{
    auto loaded_mapping = data.getColumnIdMappingStore().load(attach);
    if (!loaded_mapping)
        return;

    if (loaded_mapping->isActive())
        checkColumnIdMappingCoversMetadata(data, *loaded_mapping);
    data.setColumnIdMapping(std::move(*loaded_mapping));
}

void reconcileColumnIdMappingWithMetadata(MergeTreeData & data)
{
    if (!data.hasActiveColumnIdMapping())
        return;

    auto mapping = data.getColumnIdMapping();
    auto metadata_snapshot = data.getInMemoryMetadataPtr(nullptr, false);
    auto metadata_columns = metadata_snapshot->getColumns().getAllPhysical();

    checkColumnIdMappingCoversMetadata(data, *mapping);

    /// Trim mapping entries whose logical name is no longer in metadata (mapping-ahead-of-schema
    /// window from a crash between the mapping write and the `metadata.sql` truncation). Safe to
    /// recover from: the dropped or renamed column lost its on-disk authority at the schema commit,
    /// so the entry is dead weight.
    ColumnIdMapping reconciled = *mapping;
    bool changed = false;
    for (const auto & col_name : mapping->logicalNames())
    {
        if (!metadata_columns.tryGetByName(col_name))
        {
            reconciled.removeColumn(col_name);
            changed = true;
        }
    }

    if (changed)
        data.persistMapping(std::move(reconciled));

    if (auto final_mapping = data.getColumnIdMapping())
        checkNoLogicalNamesShareColumnId(data, *final_mapping);
}

ColumnIdMappingUpdate::ColumnIdMappingUpdate(MergeTreeData & data_, LoggerPtr log_)
    : data(data_)
    , log(std::move(log_))
    , published_policy(data_.getStoragePolicy())
    , published_before(data_.getColumnIdMapping())
{
}

ColumnIdMappingUpdate::~ColumnIdMappingUpdate()
{
    if (state != State::Empty && state != State::Committed)
        restoreFile();
}

void ColumnIdMappingUpdate::writeToDisk(const ColumnIdMapping & mapping_to_write) const
{
    data.getColumnIdMappingStore().store(mapping_to_write, target_policy ? target_policy : data.getStoragePolicy());
}

void ColumnIdMappingUpdate::stampInto(StorageInMemoryMetadata & metadata_to_publish) const
{
    metadata_to_publish.column_id_mapping = std::make_shared<const ColumnIdMapping>(*mapping);
}

bool ColumnIdMappingUpdate::isAlreadyPublished(const ColumnIdMapping & planned, const ColumnIdAlterPlan & plan) const
{
    if (!published_before)
        return false;

    /// Never for a plan with names to prune: a two-phase drop's phase-1 mapping is equal to the
    /// published one on purpose, and the change lands in phase 2.
    if (!plan.rename_old_names.empty() || !plan.drop_names.empty())
        return false;

    return sameMapping(planned, *published_before);
}

void ColumnIdMappingUpdate::copyToTargetPolicy()
{
    if (!target_policy || !published_before || !published_before->isActive())
        return;

    writeToDisk(*published_before);
    state = State::Copied;

    failpointAfterMappingPersist();
}

void ColumnIdMappingUpdate::persistBeforeSchemaCommit(
    ColumnIdAlterPlan & plan,
    StorageInMemoryMetadata & metadata_to_publish,
    const StoragePolicyPtr & target_policy_)
{
    if (state != State::Empty)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column ID mapping update: persistBeforeSchemaCommit called twice");

    target_policy = target_policy_;

    /// The planner folds activation into `new_mapping` only when the ALTER also touches columns, so a
    /// settings-only activation builds the identity mapping here: the load path fails closed when the
    /// setting says `with_column_ids` and `column_ids.json` is absent.
    auto planned = std::move(plan.new_mapping);
    if (!planned && data.columnIdActivationPending())
        planned = ColumnIdMapping::createIdentity(metadata_to_publish.getColumns().getAllPhysical());

    if (!planned || isAlreadyPublished(*planned, plan))
    {
        copyToTargetPolicy();
        return;
    }

    failpointBeforeMappingPersist();

    writeToDisk(*planned);
    mapping = std::move(planned);
    state = State::Written;

    failpointAfterMappingPersist();

    stampInto(metadata_to_publish);
}

void ColumnIdMappingUpdate::persistAfterSchemaCommit(const ColumnIdAlterPlan & plan, StorageInMemoryMetadata & metadata_to_publish)
{
    if (state != State::Written)
        return;

    if (plan.rename_old_names.empty() && plan.drop_names.empty())
        return;

    try
    {
        ColumnIdMapping pruned = *mapping;
        auto size_renames = pruneRetainedNames(pruned, plan);

        writeToDisk(pruned);

        mapping = std::move(pruned);
        column_size_renames = std::move(size_renames);
        state = State::Pruned;
        stampInto(metadata_to_publish);
    }
    catch (...)
    {
        tryLogCurrentException(log,
            "Failed to persist the finalized column ID mapping; the ALTER stands and "
            "reconciliation at next startup prunes the retained names");
    }
}

void ColumnIdMappingUpdate::restoreFile() noexcept
{
    try
    {
        if (published_before)
            data.getColumnIdMappingStore().store(*published_before, published_policy);
        else
            data.getColumnIdMappingStore().remove();
    }
    catch (...)
    {
        tryLogCurrentException(log,
            "Failed to restore `column_ids.json`; reconciliation at the next startup will fix it");
    }
}

}
