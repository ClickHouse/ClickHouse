#include <Databases/DatabaseSchemaMutationTransaction.h>

#include <Common/FailPoint.h>

#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace DB::FailPoints
{
extern const char database_schema_mutation_pause_after_prepare[];
extern const char database_schema_mutation_pause_after_first_artifact_action[];
extern const char database_schema_mutation_pause_after_installation_barrier[];
extern const char database_schema_mutation_pause_after_commit[];
}

namespace DB::UDT
{
namespace
{

struct ArtifactImages
{
    DatabaseSchemaWALStagedArtifactKind kind{};
    SchemaObjectID object;
    std::optional<size_t> before_index;
    std::optional<size_t> after_index;
};

bool sameArtifactIdentity(const ArtifactImages & images, const DatabaseSchemaWALStagedArtifactRef & artifact) noexcept
{
    return images.kind == artifact.kind && images.object == artifact.object;
}

std::vector<ArtifactImages> collectArtifactImages(const DatabaseSchemaWALValidatedTransition & transition)
{
    const auto & artifacts = transition.getPrepare().staged_artifacts;
    std::vector<ArtifactImages> result;
    result.reserve(artifacts.size());

    for (size_t index = 0; index < artifacts.size(); ++index)
    {
        const auto & artifact = artifacts[index];
        if (result.empty() || !sameArtifactIdentity(result.back(), artifact))
        {
            result.push_back(
                ArtifactImages{
                    .kind = artifact.kind,
                    .object = artifact.object,
                    .before_index = std::nullopt,
                    .after_index = std::nullopt,
                });
        }

        auto & image_index
            = artifact.image == DatabaseSchemaWALStagedArtifactImage::Before ? result.back().before_index : result.back().after_index;
        if (image_index)
            throw std::logic_error("validated schema-WAL transition contains duplicate artifact images");
        image_index = index;
    }

    return result;
}

enum class DependencyGraphImage : UInt8
{
    Before = 1,
    After = 2,
};

bool containsNode(const DatabaseSchemaWALValidatedTransition & transition, const SchemaObjectID & object, DependencyGraphImage image)
{
    const auto & after_graph = transition.getAfterGraph();
    if (image == DependencyGraphImage::After)
        return after_graph.containsNode(object);

    const auto & delta = transition.getPrepare().graph_delta;
    if (std::binary_search(delta.node_removals.begin(), delta.node_removals.end(), object))
        return true;
    if (std::binary_search(delta.node_additions.begin(), delta.node_additions.end(), object))
        return false;
    return after_graph.containsNode(object);
}

std::vector<SchemaObjectID>
collectDependencies(const DatabaseSchemaWALValidatedTransition & transition, const SchemaObjectID & object, DependencyGraphImage image)
{
    const auto & after_graph = transition.getAfterGraph();
    const auto & delta = transition.getPrepare().graph_delta;
    std::vector<SchemaObjectID> result;
    result.reserve(after_graph.getDependencies(object).size());
    for (const auto & dependency : after_graph.getDependencies(object))
    {
        const SchemaObjectDependencyEdge edge{
            .dependent = object,
            .dependency = dependency.object,
            .kind = dependency.kind,
        };
        if (image == DependencyGraphImage::Before && std::binary_search(delta.edge_additions.begin(), delta.edge_additions.end(), edge))
            continue;
        result.push_back(dependency.object);
    }

    if (image == DependencyGraphImage::Before)
    {
        auto it = std::lower_bound(
            delta.edge_removals.begin(),
            delta.edge_removals.end(),
            object,
            [](const auto & edge, const auto & dependent) { return edge.dependent < dependent; });
        while (it != delta.edge_removals.end() && it->dependent == object)
        {
            result.push_back(it->dependency);
            ++it;
        }
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

std::vector<SchemaObjectID> collectActionObjects(std::span<const DatabaseSchemaMutationArtifactAction> actions)
{
    std::vector<SchemaObjectID> result;
    result.reserve(actions.size());
    for (const auto & action : actions)
        result.push_back(action.object);
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

bool isGraphlessPhysicalObjectMetadataInstallation(
    const DatabaseSchemaWALValidatedTransition & transition,
    const SchemaObjectID & object,
    DependencyGraphImage graph_image,
    std::span<const DatabaseSchemaMutationArtifactAction> actions)
{
    /// `Table` supports both mapped admission and physicalization. `View` and
    /// `Dictionary` cannot enter authority through `ALTER`, but may leave it by
    /// publishing their physical metadata after the mapped graph node is
    /// removed.
    const bool supported_object = object.kind == SchemaObjectKind::Table
        || (graph_image == DependencyGraphImage::After
            && (object.kind == SchemaObjectKind::View || object.kind == SchemaObjectKind::Dictionary));
    if (!supported_object)
        return false;

    const auto & deltas = transition.getPrepare().dependent_object_deltas;
    const auto delta_it = std::lower_bound(
        deltas.begin(), deltas.end(), object, [](const auto & delta, const auto & value) { return delta.object < value; });
    if (delta_it == deltas.end() || delta_it->object != object)
        return false;

    const auto & selected_state = graph_image == DependencyGraphImage::Before ? delta_it->before : delta_it->after;
    const auto & opposite_state = graph_image == DependencyGraphImage::Before ? delta_it->after : delta_it->before;
    if (!selected_state || selected_state->sidecar_record_hash || selected_state->expectation_record_hash || !opposite_state
        || !opposite_state->sidecar_record_hash || !opposite_state->expectation_record_hash)
    {
        return false;
    }

    bool found = false;
    for (const auto & action : actions)
    {
        if (action.object != object)
            continue;
        if (found || action.action != DatabaseSchemaMutationArtifactActionKind::Install
            || action.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata || !action.staged_artifact_ordinal)
        {
            return false;
        }
        found = true;
    }
    return found;
}

std::vector<SchemaObjectID> dependencyFirstOrder(
    const DatabaseSchemaWALValidatedTransition & transition,
    DependencyGraphImage graph_image,
    std::span<const DatabaseSchemaMutationArtifactAction> actions)
{
    const auto objects = collectActionObjects(actions);
    std::vector<size_t> remaining_dependencies(objects.size(), 0);
    std::vector<std::vector<size_t>> local_dependents(objects.size());
    std::set<SchemaObjectID> ready;

    for (size_t index = 0; index < objects.size(); ++index)
    {
        if (!containsNode(transition, objects[index], graph_image))
        {
            /// Physical-only tables are intentionally absent from the sparse
            /// UDT graph. A validated physical<->mapped transition can still
            /// install that table's ordinary metadata through the mapped
            /// opposite image, where storage obtains its canonical path.
            if (!isGraphlessPhysicalObjectMetadataInstallation(transition, objects[index], graph_image, actions))
                throw std::logic_error("schema-mutation artifact object is absent from its selected dependency graph");
            ready.insert(objects[index]);
            continue;
        }
        for (const auto & dependency : collectDependencies(transition, objects[index], graph_image))
        {
            if (dependency == objects[index])
                continue;
            const auto it = std::lower_bound(objects.begin(), objects.end(), dependency);
            if (it == objects.end() || *it != dependency)
                continue;
            ++remaining_dependencies[index];
            local_dependents[static_cast<size_t>(it - objects.begin())].push_back(index);
        }
        if (remaining_dependencies[index] == 0)
            ready.insert(objects[index]);
    }

    std::vector<SchemaObjectID> result;
    result.reserve(objects.size());
    while (!ready.empty())
    {
        const SchemaObjectID object = *ready.begin();
        ready.erase(ready.begin());
        result.push_back(object);

        const auto object_it = std::lower_bound(objects.begin(), objects.end(), object);
        if (object_it == objects.end() || *object_it != object)
            throw std::logic_error("schema-mutation dependency order lost an action object");
        for (const size_t dependent_index : local_dependents[static_cast<size_t>(object_it - objects.begin())])
        {
            if (remaining_dependencies[dependent_index] == 0)
                throw std::logic_error("schema-mutation dependency order accounting underflow");
            --remaining_dependencies[dependent_index];
            if (remaining_dependencies[dependent_index] == 0)
                ready.insert(objects[dependent_index]);
        }
    }

    if (result.size() != objects.size())
        throw std::logic_error("schema-mutation artifact objects contain a dependency cycle");
    return result;
}

using RankedObject = std::pair<SchemaObjectID, size_t>;

std::vector<RankedObject> buildObjectRanks(std::span<const SchemaObjectID> order)
{
    std::vector<RankedObject> result;
    result.reserve(order.size());
    for (size_t index = 0; index < order.size(); ++index)
        result.emplace_back(order[index], index);
    std::sort(result.begin(), result.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    return result;
}

size_t objectRank(std::span<const RankedObject> ranks, const SchemaObjectID & object)
{
    const auto it = std::lower_bound(
        ranks.begin(), ranks.end(), object, [](const auto & ranked, const auto & value) { return ranked.first < value; });
    if (it == ranks.end() || it->first != object)
        throw std::logic_error("schema-mutation artifact object is missing from its installation order");
    return it->second;
}

void orderActions(
    std::vector<DatabaseSchemaMutationArtifactAction> & actions,
    const DatabaseSchemaWALValidatedTransition & transition,
    DependencyGraphImage graph_image,
    bool dependents_first)
{
    auto object_order = dependencyFirstOrder(transition, graph_image, actions);
    if (dependents_first)
        std::reverse(object_order.begin(), object_order.end());
    const auto ranks = buildObjectRanks(object_order);

    std::sort(
        actions.begin(),
        actions.end(),
        [&ranks](const auto & lhs, const auto & rhs)
        {
            const size_t lhs_rank = objectRank(ranks, lhs.object);
            const size_t rhs_rank = objectRank(ranks, rhs.object);
            if (lhs_rank != rhs_rank)
                return lhs_rank < rhs_rank;
            return static_cast<UInt8>(lhs.kind) < static_cast<UInt8>(rhs.kind);
        });
}

void applyArtifactPlan(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaWALValidatedTransition & transition,
    std::span<const DatabaseSchemaMutationArtifactAction> actions)
{
    const auto & artifacts = transition.getPrepare().staged_artifacts;
    const auto bytes = transition.getStagedArtifactBytes();

    for (const auto & action : actions)
    {
        switch (action.action)
        {
            case DatabaseSchemaMutationArtifactActionKind::Install:
                if (!action.staged_artifact_ordinal || *action.staged_artifact_ordinal >= artifacts.size())
                    throw std::logic_error("validated schema-mutation install action has no artifact ordinal");
                storage.installArtifact(artifacts[*action.staged_artifact_ordinal], bytes[*action.staged_artifact_ordinal]);
                break;
            case DatabaseSchemaMutationArtifactActionKind::Remove:
                if (action.staged_artifact_ordinal)
                    throw std::logic_error("validated schema-mutation remove action has an artifact ordinal");
                storage.removeArtifact(action.kind, action.object);
                break;
        }
    }
}

void applyPreparedArtifactPlan(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaWALPrepare & prepare,
    std::span<const String> staged_artifact_bytes,
    std::span<const DatabaseSchemaMutationArtifactAction> actions)
{
    bool first_action = true;
    for (const auto & action : actions)
    {
        if (action.action == DatabaseSchemaMutationArtifactActionKind::Install)
        {
            const size_t ordinal = *action.staged_artifact_ordinal;
            storage.installArtifact(prepare.staged_artifacts[ordinal], staged_artifact_bytes[ordinal]);
        }
        else
        {
            storage.removeArtifact(action.kind, action.object);
        }
        if (first_action)
        {
            first_action = false;
            FailPointInjection::pauseFailPoint(FailPoints::database_schema_mutation_pause_after_first_artifact_action);
        }
    }
}

void requireGuardDatabaseAndState(
    const DatabaseSchemaMutationGuard & guard, UUID database_uuid, DatabaseSchemaMutationGuard::State expected_state)
{
    if (guard.getDatabaseUUID() != database_uuid)
        throw std::logic_error("schema-mutation guard belongs to another database");
    if (guard.getState() != expected_state)
        throw std::logic_error("schema-mutation guard is not in the required state");
}

}

DatabaseSchemaMutationIndeterminateDurabilityError::DatabaseSchemaMutationIndeterminateDurabilityError(
    UInt64 transaction_id_, DatabaseSchemaMutationDurabilityPhase phase_)
    : std::runtime_error(
          "schema mutation transaction " + std::to_string(transaction_id_) + " has indeterminate durability at phase "
          + std::to_string(static_cast<UInt8>(phase_)))
    , transaction_id(transaction_id_)
    , phase(phase_)
{
}

DatabaseSchemaMutationReplayConflictError::DatabaseSchemaMutationReplayConflictError(std::string_view message)
    : std::runtime_error(String(message))
{
}

PreparedDatabaseSchemaMutationExecution::PreparedDatabaseSchemaMutationExecution(
    DatabaseSchemaWALPrepare prepare_,
    std::vector<String> staged_artifact_bytes_,
    std::vector<DatabaseSchemaMutationArtifactAction> actions_,
    String prepare_bytes_,
    DatabaseSchemaWALCommit commit_,
    String commit_bytes_,
    std::vector<DatabaseSchemaWALStagedArtifactLocator> locators_)
    : prepare(std::move(prepare_))
    , staged_artifact_bytes(std::move(staged_artifact_bytes_))
    , actions(std::move(actions_))
    , prepare_bytes(std::move(prepare_bytes_))
    , commit(std::move(commit_))
    , commit_bytes(std::move(commit_bytes_))
    , locators(std::move(locators_))
{
}

PreparedDatabaseSchemaMutationExecution::PreparedDatabaseSchemaMutationExecution(
    PreparedDatabaseSchemaMutationExecution && other) noexcept
    : prepare(std::move(other.prepare))
    , staged_artifact_bytes(std::move(other.staged_artifact_bytes))
    , actions(std::move(other.actions))
    , prepare_bytes(std::move(other.prepare_bytes))
    , commit(std::move(other.commit))
    , commit_bytes(std::move(other.commit_bytes))
    , locators(std::move(other.locators))
    , validated_guard_database_uuid(other.validated_guard_database_uuid)
    , validated_guard_opaque_identity(other.validated_guard_opaque_identity)
    , validated_guard_predecessor(other.validated_guard_predecessor)
    , validated_storage(other.validated_storage)
    , durable_preflight_complete(other.durable_preflight_complete)
    , usable(other.usable)
{
    other.validated_guard_database_uuid = UUIDHelpers::Nil;
    other.validated_guard_opaque_identity = 0;
    other.validated_guard_predecessor = 0;
    other.validated_storage = nullptr;
    other.durable_preflight_complete = false;
    other.usable = false;
}

DatabaseSchemaMutationGuard::DatabaseSchemaMutationGuard(
    UUID database_uuid_, UInt64 opaque_identity_, UInt64 durable_predecessor_transaction_id_)
    : database_uuid(database_uuid_)
    , opaque_identity(opaque_identity_)
    , durable_predecessor_transaction_id(durable_predecessor_transaction_id_)
{
    if (database_uuid == UUIDHelpers::Nil || opaque_identity == 0)
        throw std::invalid_argument("schema-mutation guard identity is invalid");
}

DatabaseSchemaMutationGuard::DatabaseSchemaMutationGuard(DatabaseSchemaMutationGuard && other) noexcept
    : database_uuid(other.database_uuid)
    , opaque_identity(other.opaque_identity)
    , durable_predecessor_transaction_id(other.durable_predecessor_transaction_id)
    , state(other.state)
{
    other.invalidateAfterMove();
}

void DatabaseSchemaMutationGuard::invalidateAfterMove() noexcept
{
    database_uuid = UUIDHelpers::Nil;
    opaque_identity = 0;
    durable_predecessor_transaction_id = 0;
    state = State::Finished;
}

DatabaseSchemaMutationGuard
DatabaseSchemaMutationGuard::issue(UUID database_uuid, UInt64 opaque_identity, UInt64 durable_predecessor_transaction_id)
{
    return DatabaseSchemaMutationGuard(database_uuid, opaque_identity, durable_predecessor_transaction_id);
}

std::vector<DatabaseSchemaMutationArtifactAction> planValidatedDatabaseSchemaArtifactInstallation(
    const DatabaseSchemaWALValidatedTransition & transition, DatabaseSchemaWALStagedArtifactImage selected_image)
{
    if (selected_image != DatabaseSchemaWALStagedArtifactImage::Before && selected_image != DatabaseSchemaWALStagedArtifactImage::After)
    {
        throw std::invalid_argument("schema-mutation selected artifact image is invalid");
    }

    /// An exact repair has no trustworthy physical Before image: its whole
    /// purpose is to replace a missing or damaged canonical artifact with the
    /// immutable image already addressed by the authority root. Once Prepare
    /// is durable, both committed completion and nominal rollback therefore
    /// reinstall the authenticated After image. Logical authority publication
    /// still follows the Commit decision; quarantine remains until a complete
    /// re-verification explicitly releases it.
    if (isDatabaseSchemaWALExactRepair(transition.getPrepare()))
        selected_image = DatabaseSchemaWALStagedArtifactImage::After;

    const auto images = collectArtifactImages(transition);
    const auto install_graph_image
        = selected_image == DatabaseSchemaWALStagedArtifactImage::After ? DependencyGraphImage::After : DependencyGraphImage::Before;
    const auto remove_graph_image
        = selected_image == DatabaseSchemaWALStagedArtifactImage::After ? DependencyGraphImage::Before : DependencyGraphImage::After;

    std::vector<DatabaseSchemaMutationArtifactAction> removals;
    std::vector<DatabaseSchemaMutationArtifactAction> installations;
    removals.reserve(images.size());
    installations.reserve(images.size());
    for (const auto & artifact_images : images)
    {
        const auto index
            = selected_image == DatabaseSchemaWALStagedArtifactImage::Before ? artifact_images.before_index : artifact_images.after_index;
        auto action = DatabaseSchemaMutationArtifactAction{
            .action = index ? DatabaseSchemaMutationArtifactActionKind::Install : DatabaseSchemaMutationArtifactActionKind::Remove,
            .kind = artifact_images.kind,
            .object = artifact_images.object,
            .staged_artifact_ordinal = index,
        };
        if (index)
            installations.push_back(std::move(action));
        else
            removals.push_back(std::move(action));
    }

    orderActions(removals, transition, remove_graph_image, true);
    orderActions(installations, transition, install_graph_image, false);
    removals.insert(removals.end(), std::make_move_iterator(installations.begin()), std::make_move_iterator(installations.end()));
    return removals;
}

DatabaseSchemaWALCommit executeDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedTransition & transition,
    const DatabaseSchemaWALLimits & limits)
{
    auto prepared = prepareDatabaseSchemaMutationExecution(transition, limits);
    validatePreparedDatabaseSchemaMutationExecution(storage, guard, prepared);
    return executePreparedDatabaseSchemaMutation(storage, guard, std::move(prepared));
}

PreparedDatabaseSchemaMutationExecution
prepareDatabaseSchemaMutationExecution(const DatabaseSchemaWALValidatedTransition & transition, const DatabaseSchemaWALLimits & limits)
{
    const auto & prepare = transition.getPrepare();
    auto actions = planValidatedDatabaseSchemaArtifactInstallation(transition, DatabaseSchemaWALStagedArtifactImage::After);
    DatabaseSchemaWALPrepare owned_prepare = prepare;
    String prepare_bytes = encodeDatabaseSchemaWALPrepare(prepare, limits);
    DatabaseSchemaWALCommit commit = makeDatabaseSchemaWALCommit(transition, limits);
    String commit_bytes = encodeDatabaseSchemaWALCommit(commit, limits);
    const auto staged_artifact_bytes = transition.getStagedArtifactBytes();
    if (staged_artifact_bytes.size() != prepare.staged_artifacts.size())
        throw std::logic_error("validated schema-mutation artifact bytes do not match the manifest");
    std::vector<String> owned_staged_artifact_bytes(staged_artifact_bytes.begin(), staged_artifact_bytes.end());

    for (const auto & action : actions)
    {
        if (action.action == DatabaseSchemaMutationArtifactActionKind::Install)
        {
            if (!action.staged_artifact_ordinal || *action.staged_artifact_ordinal >= prepare.staged_artifacts.size())
                throw std::logic_error("validated schema-mutation install action has no artifact ordinal");
        }
        else if (action.staged_artifact_ordinal)
        {
            throw std::logic_error("validated schema-mutation remove action has an artifact ordinal");
        }
    }

    std::vector<DatabaseSchemaWALStagedArtifactLocator> locators;
    locators.reserve(prepare.staged_artifacts.size());
    for (size_t index = 0; index < prepare.staged_artifacts.size(); ++index)
    {
        locators.push_back(
            makeDatabaseSchemaWALStagedArtifactLocator(prepare.after_authority_state.database_uuid, prepare.transaction_id, index));
    }

    return PreparedDatabaseSchemaMutationExecution(
        std::move(owned_prepare),
        std::move(owned_staged_artifact_bytes),
        std::move(actions),
        std::move(prepare_bytes),
        std::move(commit),
        std::move(commit_bytes),
        std::move(locators));
}

void validatePreparedDatabaseSchemaMutationExecution(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    PreparedDatabaseSchemaMutationExecution & prepared)
{
    if (!prepared.usable)
        throw std::logic_error("schema-mutation execution image was moved from");
    if (prepared.durable_preflight_complete)
        throw std::logic_error("schema-mutation execution image was already validated");

    const auto & prepare = prepared.prepare;
    requireGuardDatabaseAndState(guard, prepare.after_authority_state.database_uuid, DatabaseSchemaMutationGuard::State::Ready);
    storage.validateMutationGuardAndDurablePredecessor(guard, prepare.before_authority_state, prepare.transaction_id);
    prepared.validated_guard_database_uuid = guard.getDatabaseUUID();
    prepared.validated_guard_opaque_identity = guard.getOpaqueIdentity();
    prepared.validated_guard_predecessor = guard.getDurablePredecessorTransactionID();
    prepared.validated_storage = &storage;
    prepared.durable_preflight_complete = true;
}

DatabaseSchemaWALCommit executePreparedDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & guard,
    PreparedDatabaseSchemaMutationExecution prepared)
{
    const auto & prepare = prepared.prepare;
    if (!prepared.usable || !prepared.durable_preflight_complete || prepared.validated_storage != &storage
        || guard.getState() != DatabaseSchemaMutationGuard::State::Ready
        || guard.getDatabaseUUID() != prepared.validated_guard_database_uuid
        || guard.getOpaqueIdentity() != prepared.validated_guard_opaque_identity
        || guard.getDurablePredecessorTransactionID() != prepared.validated_guard_predecessor)
        std::terminate();

    for (size_t index = 0; index < prepare.staged_artifacts.size(); ++index)
        storage.stageArtifact(prepared.locators[index], prepare.staged_artifacts[index], prepared.staged_artifact_bytes[index]);
    storage.finishStaging(prepare.after_authority_state.database_uuid, prepare.transaction_id);

    auto phase = DatabaseSchemaMutationDurabilityPhase::PrepareMarker;
    try
    {
        storage.persistPrepare(prepare.transaction_id, prepared.prepare_bytes);
        FailPointInjection::pauseFailPoint(FailPoints::database_schema_mutation_pause_after_prepare);
        phase = DatabaseSchemaMutationDurabilityPhase::AfterImage;
        applyPreparedArtifactPlan(storage, prepare, prepared.staged_artifact_bytes, prepared.actions);
        phase = DatabaseSchemaMutationDurabilityPhase::InstallationBarrier;
        storage.finishInstallation(prepare.after_authority_state.database_uuid, prepare.transaction_id);
        phase = DatabaseSchemaMutationDurabilityPhase::CommitMarker;
        FailPointInjection::pauseFailPoint(FailPoints::database_schema_mutation_pause_after_installation_barrier);
        storage.persistCommit(prepare.transaction_id, prepared.commit_bytes);
        FailPointInjection::pauseFailPoint(FailPoints::database_schema_mutation_pause_after_commit);
    }
    catch (...)
    {
        guard.state = DatabaseSchemaMutationGuard::State::RecoveryRequired;
        storage.markMutationRecoveryRequired(guard, prepare.transaction_id, phase);
        std::throw_with_nested(DatabaseSchemaMutationIndeterminateDurabilityError(prepare.transaction_id, phase));
    }

    guard.state = DatabaseSchemaMutationGuard::State::Finished;
    return std::move(prepared.commit);
}

DatabaseSchemaWALRecoveryDecision recoverDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedTransition & transition,
    const std::optional<DatabaseSchemaWALCommit> & commit,
    const DatabaseSchemaWALLimits & limits)
{
    const auto decision = decideDatabaseSchemaWALRecovery(transition, commit, limits);
    const auto selected_image = decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted
        ? DatabaseSchemaWALStagedArtifactImage::After
        : DatabaseSchemaWALStagedArtifactImage::Before;
    const auto actions = planValidatedDatabaseSchemaArtifactInstallation(transition, selected_image);
    const auto & prepare = transition.getPrepare();

    if (guard.getDatabaseUUID() != prepare.after_authority_state.database_uuid
        || guard.getState() == DatabaseSchemaMutationGuard::State::Finished)
    {
        throw std::logic_error("schema-mutation recovery guard is invalid");
    }
    storage.validateRecoveryGuard(guard, prepare.transaction_id);

    auto phase = DatabaseSchemaMutationDurabilityPhase::RecoveryImage;
    try
    {
        applyArtifactPlan(storage, transition, actions);
        phase = DatabaseSchemaMutationDurabilityPhase::RecoveryBarrier;
        storage.finishInstallation(prepare.after_authority_state.database_uuid, prepare.transaction_id);
        phase = DatabaseSchemaMutationDurabilityPhase::RecoveryMarker;
        storage.finishRecovery(prepare.after_authority_state.database_uuid, prepare.transaction_id, decision);
    }
    catch (...)
    {
        guard.state = DatabaseSchemaMutationGuard::State::RecoveryRequired;
        storage.markMutationRecoveryRequired(guard, prepare.transaction_id, phase);
        std::throw_with_nested(DatabaseSchemaMutationIndeterminateDurabilityError(prepare.transaction_id, phase));
    }

    guard.state = DatabaseSchemaMutationGuard::State::Finished;
    return decision;
}

void discardUnpreparedDatabaseSchemaMutationStaging(
    IDatabaseSchemaMutationDurableStorage & storage, DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    if (guard.getState() == DatabaseSchemaMutationGuard::State::Finished)
        throw std::logic_error("schema-mutation guard has already finished");
    storage.discardUnpreparedStaging(guard, transaction_id);
    guard.state = DatabaseSchemaMutationGuard::State::Finished;
}

void retireRolledBackDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage, const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    if (guard.getState() != DatabaseSchemaMutationGuard::State::Finished)
        throw std::logic_error("schema-mutation rollback cannot be retired before recovery finishes");
    storage.retireRolledBackTransaction(guard, transaction_id);
}

void persistValidatedDatabaseSchemaCheckpoint(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedCheckpoint & checkpoint,
    const DatabaseSchemaWALLimits & limits)
{
    const auto & record = checkpoint.getCheckpoint();
    requireGuardDatabaseAndState(guard, record.authority_state.database_uuid, DatabaseSchemaMutationGuard::State::Ready);
    const String checkpoint_bytes = encodeDatabaseSchemaWALCheckpoint(record, limits);
    storage.persistValidatedCheckpoint(
        guard, record, checkpoint_bytes, checkpoint.getInventorySnapshotBytes(), checkpoint.getSchemaGraphSnapshotBytes());
}

void compactDatabaseSchemaWALThroughValidatedCheckpoint(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedCheckpoint & checkpoint)
{
    const auto & record = checkpoint.getCheckpoint();
    requireGuardDatabaseAndState(guard, record.authority_state.database_uuid, DatabaseSchemaMutationGuard::State::Ready);
    storage.compactThroughValidatedCheckpoint(guard, record);
}

}
