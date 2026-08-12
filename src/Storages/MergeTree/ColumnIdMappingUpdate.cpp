#include <Storages/MergeTree/ColumnIdMappingUpdate.h>

#include <Storages/MergeTree/ColumnIdAlterPlanner.h>
#include <Storages/MergeTree/ColumnIdMappingStore.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/FailPoint.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char column_ids_throw_before_mapping_persist[];
    extern const char column_ids_throw_after_mapping_persist[];
    extern const char column_ids_throw_before_mapping_prune[];
}

namespace
{

/// The published pointer is the version token FREEZE and BACKUP compare, so an unchanged mapping has
/// to keep it -- and the planner returns a copy of the live mapping for every ALTER on an id-active
/// table, most of which change nothing about it.
bool sameMapping(const ColumnIdMapping & lhs, const ColumnIdMapping & rhs)
{
    return lhs.isActive() == rhs.isActive() && lhs.getNextColumnIdCounter() == rhs.getNextColumnIdCounter()
        && lhs.getNameToId() == rhs.getNameToId();
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

void failpointBeforeMappingPrune()
{
    fiu_do_on(FailPoints::column_ids_throw_before_mapping_prune,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure before column ID mapping prune");
    });
}

NameToNameVector pruneRetainedNames(ColumnIdMapping & mapping, const ColumnIdAlterPlan & plan)
{
    NameToNameVector column_size_renames;

    for (const auto & old_name : plan.rename_old_names)
    {
        auto column_id = mapping.tryGetColumnId(old_name);
        mapping.finishRename(old_name);

        if (!column_id)
            continue;

        if (auto new_name = mapping.tryGetColumnName(*column_id); new_name && *new_name != old_name)
            column_size_renames.emplace_back(old_name, *new_name);
    }

    for (const auto & name : plan.drop_names)
    {
        if (mapping.hasColumnName(name))
            mapping.removeColumn(name);
    }

    return column_size_renames;
}

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
        failpointBeforeMappingPrune();

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
