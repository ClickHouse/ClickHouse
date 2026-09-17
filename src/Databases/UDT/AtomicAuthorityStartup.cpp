#include <Databases/UDT/AtomicAuthorityStartup.h>

#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/DependentObjectActivationPlanner.h>
#include <Databases/UDT/SyntheticObjectMetadata.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/UDT/AuthorityInventorySnapshot.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/Record.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

namespace DB::UDT
{

namespace
{

using StartupError = AtomicAuthorityStartupError;

struct DegradedStartupCause
{
    std::optional<AuthorityInventoryKey> invalid_definition;
    String invalid_definition_error;
    String incomplete_definition_error;
};

[[noreturn]] void startupFail(StartupError::Code code, std::string_view message)
{
    throw StartupError(code, message);
}

DatabaseSchemaWALTransitionBase makeEmptyBase(UUID database_uuid, const DatabaseSchemaWALLimits & limits)
{
    std::vector<AuthorityInventoryLeaf> no_leaves;
    const auto summary = buildAuthorityInventorySummary(no_leaves, limits.inventory_snapshot.inventory);
    return {
        .authority_state = std::nullopt,
        .authority_inventory = AuthorityInventory::create(summary, std::move(no_leaves), limits.inventory_snapshot.inventory),
        .schema_graph = SchemaObjectDependencyGraph::createEmpty(database_uuid, limits.schema_graph),
    };
}

bool isStartupRepairArtifactKind(DatabaseSchemaWALStagedArtifactKind kind) noexcept
{
    return kind == DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord
        || kind == DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord
        || kind == DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar;
}

struct StartupRepairCandidateKey
{
    DatabaseSchemaWALStagedArtifactKind kind{};
    SchemaObjectID object;

    bool operator<(const StartupRepairCandidateKey & other) const noexcept
    {
        return std::tuple{static_cast<UInt8>(kind), object} < std::tuple{static_cast<UInt8>(other.kind), other.object};
    }
};

/// Retains at most one latest committed local-WAL After image per repairable
/// artifact. This cache is optional: crossing the exact-repair transaction
/// caps disables startup repair without affecting ordinary WAL recovery.
class StartupRepairCandidateCache
{
public:
    void applyCommittedTransition(const DatabaseSchemaWALValidatedTransition & transition, const DatabaseSchemaWALLimits & limits)
    {
        if (!available)
            return;

        const auto & prepare = transition.getPrepare();
        for (const auto & delta : prepare.authority_record_deltas)
        {
            const auto kind = delta.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition
                ? DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord
                : DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord;
            erase(kind, delta.key.object_uuid);
        }
        for (const auto & delta : prepare.dependent_object_deltas)
            erase(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, delta.object.object_uuid);

        const auto bytes = transition.getStagedArtifactBytes();
        if (bytes.size() != prepare.staged_artifacts.size())
        {
            disable();
            return;
        }
        for (size_t index = 0; index < prepare.staged_artifacts.size(); ++index)
        {
            const auto & ref = prepare.staged_artifacts[index];
            if (ref.image != DatabaseSchemaWALStagedArtifactImage::After || !isStartupRepairArtifactKind(ref.kind))
                continue;
            replace(
                StartupRepairCandidateKey{ref.kind, ref.object},
                DatabaseSchemaWALStagedArtifact{
                    .kind = ref.kind,
                    .image = DatabaseSchemaWALStagedArtifactImage::After,
                    .object = ref.object,
                    .revision = ref.revision,
                    .canonical_bytes = bytes[index],
                },
                limits);
            if (!available)
                return;
        }
    }

    bool isAvailable() const noexcept { return available; }
    bool empty() const noexcept { return candidates.empty(); }

    const DatabaseSchemaWALStagedArtifact * find(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) const noexcept
    {
        const auto it = candidates.find({kind, object});
        return it == candidates.end() ? nullptr : std::addressof(it->second);
    }

    const std::map<StartupRepairCandidateKey, DatabaseSchemaWALStagedArtifact> & get() const noexcept { return candidates; }

private:
    void erase(DatabaseSchemaWALStagedArtifactKind kind, UUID object_uuid)
    {
        for (auto it = candidates.begin(); it != candidates.end();)
        {
            if (it->first.kind == kind && it->first.object.object_uuid == object_uuid)
            {
                total_bytes -= it->second.canonical_bytes.size();
                it = candidates.erase(it);
            }
            else
                ++it;
        }
    }

    void replace(StartupRepairCandidateKey key, DatabaseSchemaWALStagedArtifact artifact, const DatabaseSchemaWALLimits & limits)
    {
        const auto existing = candidates.find(key);
        const UInt64 existing_bytes = existing == candidates.end() ? 0 : existing->second.canonical_bytes.size();
        const UInt64 candidate_bytes = artifact.canonical_bytes.size();
        if ((existing == candidates.end() && candidates.size() >= limits.maximum_staged_artifacts)
            || candidate_bytes > limits.maximum_total_staged_artifact_bytes - (total_bytes - existing_bytes))
        {
            disable();
            return;
        }
        total_bytes = total_bytes - existing_bytes + candidate_bytes;
        candidates.insert_or_assign(std::move(key), std::move(artifact));
    }

    void disable() noexcept
    {
        available = false;
        total_bytes = 0;
        candidates.clear();
    }

    bool available = true;
    UInt64 total_bytes = 0;
    std::map<StartupRepairCandidateKey, DatabaseSchemaWALStagedArtifact> candidates;
};

std::optional<AuthorityState> makeStartupExactRepairSuccessor(const AuthorityState & before, const AuthorityStateLimits & limits)
{
    if (before.database_catalog_epoch == std::numeric_limits<UInt64>::max())
        return std::nullopt;
    try
    {
        return makeAuthorityState(
            before.database_uuid,
            before.database_catalog_epoch + 1,
            before.persistent_capability_mask,
            before.leaf_count,
            before.inventory_root,
            before.schema_graph_root,
            limits);
    }
    catch (const AuthorityStateError &)
    {
        return std::nullopt;
    }
}

struct StartupRepairSelection
{
    std::vector<DatabaseSchemaWALStagedArtifact> damaged_artifacts;
    std::vector<DatabaseSchemaWALStagedArtifact> proof_artifacts;
};

bool validatesAsStartupExactRepairGroup(
    UInt64 transaction_id,
    const DatabaseSchemaWALTransitionBase & base,
    const AuthorityState & successor,
    const std::vector<DatabaseSchemaWALStagedArtifact> & damaged_artifacts,
    const std::vector<DatabaseSchemaWALStagedArtifact> & proof_artifacts,
    const DatabaseSchemaWALLimits & limits)
{
    if (!base.authority_state || damaged_artifacts.empty())
        return false;
    try
    {
        const DatabaseSchemaWALExactRepairProvenance provenance{
            .transaction_id = transaction_id,
            .damaged_artifact_count = static_cast<UInt64>(damaged_artifacts.size()),
            .damaged_artifact_manifest_digest = computeDatabaseSchemaWALExactRepairArtifactManifestDigest(damaged_artifacts, limits),
            .local_wal_sources = static_cast<UInt64>(damaged_artifacts.size()),
            .replicated_authority_sources = 0,
            .verified_backup_sources = 0,
            .previous_catalog_epoch = base.authority_state->database_catalog_epoch,
            .previous_authority_anchor = base.authority_state->anchor_hash,
            .repaired_catalog_epoch = successor.database_catalog_epoch,
            .repaired_authority_anchor = successor.anchor_hash,
        };
        std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts = damaged_artifacts;
        staged_artifacts.insert(staged_artifacts.end(), proof_artifacts.begin(), proof_artifacts.end());
        static_cast<void>(DatabaseSchemaWALTransitionBuilder::buildExactRepair(
            transaction_id, base, successor, std::move(staged_artifacts), limits, provenance));
        return true;
    }
    catch (const DatabaseSchemaWALError &)
    {
        return false;
    }
}

StartupRepairSelection selectValidStartupRepairCandidates(
    UInt64 transaction_id,
    AtomicDatabaseSchemaMutationStorage & storage,
    const DatabaseSchemaWALTransitionBase & base,
    const AuthorityState & successor,
    const StartupRepairCandidateCache & cache,
    const DatabaseSchemaWALLimits & limits)
{
    StartupRepairSelection selected;
    selected.damaged_artifacts.reserve(cache.get().size());
    selected.proof_artifacts.reserve(cache.get().size());
    for (const auto & [key, candidate] : cache.get())
    {
        if (key.kind == DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar)
            continue;
        if (key.kind == DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord)
        {
            if (!storage.startupExactRepairArtifactNeedsInstallation(candidate))
                continue;
            const std::vector<DatabaseSchemaWALStagedArtifact> damaged_group{candidate};
            const std::vector<DatabaseSchemaWALStagedArtifact> proof_group;
            if (validatesAsStartupExactRepairGroup(transaction_id, base, successor, damaged_group, proof_group, limits))
                selected.damaged_artifacts.push_back(candidate);
            continue;
        }

        const bool expectation_needs_installation = storage.startupExactRepairArtifactNeedsInstallation(candidate);
        const auto * sidecar = cache.find(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, key.object);
        const bool sidecar_needs_installation = sidecar && storage.startupExactRepairArtifactNeedsInstallation(*sidecar);
        if (!expectation_needs_installation && !sidecar_needs_installation)
            continue;

        std::vector<DatabaseSchemaWALStagedArtifact> damaged_group;
        std::vector<DatabaseSchemaWALStagedArtifact> proof_group;
        if (expectation_needs_installation)
            damaged_group.push_back(candidate);
        else
            proof_group.push_back(candidate);
        if (sidecar_needs_installation)
            damaged_group.push_back(*sidecar);
        if (validatesAsStartupExactRepairGroup(transaction_id, base, successor, damaged_group, proof_group, limits))
        {
            selected.damaged_artifacts.insert(selected.damaged_artifacts.end(), damaged_group.begin(), damaged_group.end());
            selected.proof_artifacts.insert(selected.proof_artifacts.end(), proof_group.begin(), proof_group.end());
        }
        else if (expectation_needs_installation && sidecar_needs_installation)
        {
            damaged_group.assign(1, candidate);
            proof_group.clear();
            if (validatesAsStartupExactRepairGroup(transaction_id, base, successor, damaged_group, proof_group, limits))
                selected.damaged_artifacts.push_back(candidate);
        }
    }
    return selected;
}

bool attemptStartupExactRepairFromLocalWAL(
    AtomicDatabaseSchemaMutationStorage & storage,
    DatabaseSchemaWALTransitionBase & base,
    const StartupRepairCandidateCache & cache,
    const AtomicAuthorityStartupLimits & limits)
{
    if (!cache.isAvailable() || cache.empty() || !base.authority_state || !base.authority_inventory || !base.schema_graph)
        return false;

    auto successor = makeStartupExactRepairSuccessor(*base.authority_state, limits.wal.authority_state);
    if (!successor)
        return false;
    auto guard = storage.issueMutationGuard();
    const UInt64 predecessor = guard.getDurablePredecessorTransactionID();
    if (predecessor == std::numeric_limits<UInt64>::max())
        return false;
    const UInt64 transaction_id = predecessor + 1;
    auto selected = selectValidStartupRepairCandidates(transaction_id, storage, base, *successor, cache, limits.wal);
    if (selected.damaged_artifacts.empty())
        return false;
    const DatabaseSchemaWALExactRepairProvenance repair_provenance{
        .transaction_id = transaction_id,
        .damaged_artifact_count = static_cast<UInt64>(selected.damaged_artifacts.size()),
        .damaged_artifact_manifest_digest
        = computeDatabaseSchemaWALExactRepairArtifactManifestDigest(selected.damaged_artifacts, limits.wal),
        .local_wal_sources = static_cast<UInt64>(selected.damaged_artifacts.size()),
        .replicated_authority_sources = 0,
        .verified_backup_sources = 0,
        .previous_catalog_epoch = base.authority_state->database_catalog_epoch,
        .previous_authority_anchor = base.authority_state->anchor_hash,
        .repaired_catalog_epoch = successor->database_catalog_epoch,
        .repaired_authority_anchor = successor->anchor_hash,
    };

    std::optional<DatabaseSchemaWALValidatedTransition> transition;
    try
    {
        selected.damaged_artifacts.insert(
            selected.damaged_artifacts.end(),
            std::make_move_iterator(selected.proof_artifacts.begin()),
            std::make_move_iterator(selected.proof_artifacts.end()));
        transition.emplace(
            DatabaseSchemaWALTransitionBuilder::buildExactRepair(
                transaction_id, base, *successor, std::move(selected.damaged_artifacts), limits.wal, repair_provenance));
    }
    catch (const DatabaseSchemaWALError &)
    {
        return false;
    }
    static_cast<void>(executeDatabaseSchemaMutation(storage, guard, *transition, limits.wal));
    const auto durable_state = storage.getCurrentAuthorityState();
    if (!durable_state || *durable_state != *successor)
        startupFail(StartupError::Code::AuthorityStateMismatch, "startup exact repair did not become the durable WAL head");
    base.authority_state = std::move(*successor);
    return true;
}

void validateStrictTransactionOrder(std::span<const UInt64> transaction_ids)
{
    UInt64 previous = 0;
    for (const UInt64 transaction_id : transaction_ids)
    {
        if (transaction_id == 0 || transaction_id <= previous)
            startupFail(StartupError::Code::InvalidDurableSequence, "Atomic schema WAL transaction IDs are not strictly ordered");
        previous = transaction_id;
    }
}

String recoveryFailureDiagnostic(AuthorityRecoveryError::Code code)
{
    switch (code)
    {
        case AuthorityRecoveryError::Code::InvalidConfiguration:
            return "authority recovery configuration is outside its supported implementation domain";
        case AuthorityRecoveryError::Code::LimitExceeded:
            return "durable definition or authority structure exceeds its supported recovery domain";
        case AuthorityRecoveryError::Code::InventoryMismatch:
            return "anchored authority inventory does not match its durable definition set";
        case AuthorityRecoveryError::Code::RecordMismatch:
            return "durable definition record does not match its anchored identity or canonical bytes";
        case AuthorityRecoveryError::Code::CanonicalSQLMismatch:
            return "durable definition declaration is not canonical executable ATTACH TYPE metadata";
        case AuthorityRecoveryError::Code::AuthorityStateMismatch: return "reconstructed authority state does not match its durable head";
        case AuthorityRecoveryError::Code::SchemaGraphMismatch: return "anchored authority dependency graph is incomplete or inconsistent";
        case AuthorityRecoveryError::Code::DefinitionMismatch: return "durable definitions do not form one valid checked authority";
    }
    return "durable user-defined type authority is incomplete";
}

bool keyedRecoveryFailureIsInvalidDefinition(AuthorityRecoveryError::Code code) noexcept
{
    switch (code)
    {
        case AuthorityRecoveryError::Code::LimitExceeded:
        case AuthorityRecoveryError::Code::RecordMismatch:
        case AuthorityRecoveryError::Code::CanonicalSQLMismatch:
        case AuthorityRecoveryError::Code::DefinitionMismatch: return true;
        case AuthorityRecoveryError::Code::InvalidConfiguration:
        case AuthorityRecoveryError::Code::InventoryMismatch:
        case AuthorityRecoveryError::Code::AuthorityStateMismatch:
        case AuthorityRecoveryError::Code::SchemaGraphMismatch: return false;
    }
    return false;
}

bool isDegradableDurableStorageError(AtomicDatabaseSchemaMutationStorageError::Code code) noexcept
{
    switch (code)
    {
        case AtomicDatabaseSchemaMutationStorageError::Code::UnsafePath:
        case AtomicDatabaseSchemaMutationStorageError::Code::CorruptDurableState:
        case AtomicDatabaseSchemaMutationStorageError::Code::LimitExceeded: return true;
        case AtomicDatabaseSchemaMutationStorageError::Code::InvalidConfiguration:
        case AtomicDatabaseSchemaMutationStorageError::Code::UnsupportedDisk:
        case AtomicDatabaseSchemaMutationStorageError::Code::DirectorySyncUnavailable:
        case AtomicDatabaseSchemaMutationStorageError::Code::FaultInjected: return false;
    }
    return false;
}

AtomicAuthorityStartupStatusSnapshot::Ptr makeGlobalIncompleteStartupStatus(UUID database_uuid, String stable_error)
{
    return AtomicAuthorityStartupStatusSnapshot::create(
        database_uuid, {}, {}, std::move(stable_error), AtomicAuthorityStartupDependentObjectScope::Unknown);
}

AtomicAuthorityStartupStatusSnapshot::Ptr makeDegradedStartupStatus(
    UUID database_uuid,
    const AuthorityInventory & inventory,
    const AtomicDatabaseSchemaMutationReconciliation * reconciliation,
    const AuthorityRecoveryLimits & recovery_limits,
    DegradedStartupCause cause)
{
    std::vector<AtomicAuthorityStartupDefinitionDiagnostic> diagnostics;
    std::vector<UUID> expected_dependent_object_uuids;
    diagnostics.reserve(inventory.getLeaves().size());
    expected_dependent_object_uuids.reserve(inventory.getLeaves().size());

    for (const auto & leaf : inventory.getLeaves())
    {
        if (leaf.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
        {
            diagnostics.push_back({
                .key = leaf.key,
                .revision = leaf.object_revision,
                .record = std::nullopt,
                .status = AuthorityDefinitionStatus::Incomplete,
                .last_error = cause.incomplete_definition_error,
            });
        }
        else if (leaf.key.record_kind == AuthorityInventoryRecordKind::SidecarExpectation)
            expected_dependent_object_uuids.push_back(leaf.key.object_uuid);
    }

    /// This diagnostic pass is deliberately reached only after recovery has
    /// failed. Successful startup decodes/parses/checks each definition once
    /// in recoverAuthorityRoot and pays no second O(N) catalog traversal.
    if (reconciliation)
    {
        std::vector<const AtomicDatabaseSchemaMutationAuthorityRecordImage *> images;
        images.reserve(reconciliation->authority_records.size());
        for (const auto & image : reconciliation->authority_records)
        {
            if (image.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
                images.push_back(&image);
        }
        std::sort(
            images.begin(), images.end(), [](const auto * lhs, const auto * rhs) { return authorityInventoryKeyLess(lhs->key, rhs->key); });

        auto image_it = images.begin();
        for (auto & diagnostic : diagnostics)
        {
            while (image_it != images.end() && authorityInventoryKeyLess((*image_it)->key, diagnostic.key))
                ++image_it;
            if (image_it == images.end() || (*image_it)->key != diagnostic.key)
                continue;

            const auto leaf_it = std::lower_bound(
                inventory.getLeaves().begin(),
                inventory.getLeaves().end(),
                diagnostic.key,
                [](const AuthorityInventoryLeaf & leaf, const AuthorityInventoryKey & key)
                { return authorityInventoryKeyLess(leaf.key, key); });
            if (leaf_it == inventory.getLeaves().end() || leaf_it->key != diagnostic.key)
                continue;

            try
            {
                Record record = decodeRecord((*image_it)->canonical_bytes, recovery_limits.root.definition_record);
                if (record.identity.database_uuid != database_uuid || record.identity.type_uuid != diagnostic.key.object_uuid
                    || record.identity.revision != diagnostic.revision
                    || computeRecordHash(record, recovery_limits.root.definition_record) != leaf_it->canonical_record_hash)
                {
                    diagnostic.status = AuthorityDefinitionStatus::Invalid;
                    diagnostic.last_error = "durable definition record does not match its anchored identity or hash";
                    continue;
                }

                diagnostic.record.emplace(std::move(record));
                if (BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(diagnostic.record->normalized_local_name))
                {
                    diagnostic.status = AuthorityDefinitionStatus::Conflicted;
                    diagnostic.last_error = "definition name now collides with a registered built-in type family or alias";
                }
            }
            catch (const RecordError &)
            {
                diagnostic.status = AuthorityDefinitionStatus::Invalid;
                diagnostic.last_error = "durable definition record is not canonical supported V1 bytes";
            }
        }

        std::vector<AtomicAuthorityStartupDefinitionDiagnostic *> named_diagnostics;
        named_diagnostics.reserve(diagnostics.size());
        for (auto & diagnostic : diagnostics)
            if (diagnostic.record)
                named_diagnostics.push_back(&diagnostic);
        std::sort(
            named_diagnostics.begin(),
            named_diagnostics.end(),
            [](const auto * lhs, const auto * rhs)
            {
                if (lhs->record->normalized_local_name != rhs->record->normalized_local_name)
                    return lhs->record->normalized_local_name < rhs->record->normalized_local_name;
                return authorityInventoryKeyLess(lhs->key, rhs->key);
            });
        for (size_t begin = 0; begin < named_diagnostics.size();)
        {
            size_t end = begin + 1;
            while (end < named_diagnostics.size()
                   && named_diagnostics[end]->record->normalized_local_name == named_diagnostics[begin]->record->normalized_local_name)
                ++end;
            if (end - begin > 1)
            {
                for (size_t index = begin; index < end; ++index)
                {
                    named_diagnostics[index]->status = AuthorityDefinitionStatus::Conflicted;
                    named_diagnostics[index]->last_error = "multiple durable definitions claim the same normalized local type name";
                }
            }
            begin = end;
        }
    }

    std::vector<AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects;
    AtomicAuthorityStartupDependentObjectScope dependent_object_scope = expected_dependent_object_uuids.empty()
        ? AtomicAuthorityStartupDependentObjectScope::Exact
        : AtomicAuthorityStartupDependentObjectScope::Unknown;
    if (reconciliation)
    {
        expected_dependent_objects.reserve(reconciliation->dependent_objects.size());
        std::vector<UUID> reconciled_uuids;
        reconciled_uuids.reserve(reconciliation->dependent_objects.size());
        for (const auto & dependent_object : reconciliation->dependent_objects)
        {
            reconciled_uuids.push_back(dependent_object.expectation.object.object_uuid);
            const auto kind = dependent_object.expectation.object.kind;
            if (kind != SchemaObjectKind::Table && kind != SchemaObjectKind::View && kind != SchemaObjectKind::Dictionary)
                continue;
            expected_dependent_objects.push_back({
                .object_uuid = dependent_object.expectation.object.object_uuid,
                .object_name = dependent_object.object_name,
            });
        }
        std::vector<std::string_view> reconciled_names;
        reconciled_names.reserve(expected_dependent_objects.size());
        bool identities_are_well_formed = true;
        for (const auto & object : expected_dependent_objects)
        {
            reconciled_names.push_back(object.object_name);
            identities_are_well_formed = identities_are_well_formed && object.object_uuid != UUIDHelpers::Nil && !object.object_name.empty()
                && object.object_name.find('\0') == String::npos;
        }
        std::sort(expected_dependent_object_uuids.begin(), expected_dependent_object_uuids.end());
        std::sort(reconciled_uuids.begin(), reconciled_uuids.end());
        std::sort(reconciled_names.begin(), reconciled_names.end());
        const bool names_are_unique = std::adjacent_find(reconciled_names.begin(), reconciled_names.end()) == reconciled_names.end();
        if (identities_are_well_formed && names_are_unique && reconciled_uuids == expected_dependent_object_uuids)
            dependent_object_scope = AtomicAuthorityStartupDependentObjectScope::Exact;
        else
        {
            dependent_object_scope = AtomicAuthorityStartupDependentObjectScope::Unknown;
            expected_dependent_objects.clear();
        }
    }

    if (cause.invalid_definition)
    {
        const auto it = std::find_if(
            diagnostics.begin(), diagnostics.end(), [&](const auto & diagnostic) { return diagnostic.key == *cause.invalid_definition; });
        if (it != diagnostics.end() && it->status != AuthorityDefinitionStatus::Conflicted)
        {
            it->status = AuthorityDefinitionStatus::Invalid;
            it->last_error = std::move(cause.invalid_definition_error);
        }
    }

    return AtomicAuthorityStartupStatusSnapshot::create(
        database_uuid,
        std::move(diagnostics),
        std::move(expected_dependent_objects),
        cause.incomplete_definition_error,
        dependent_object_scope);
}

std::vector<AtomicAuthorityRecoveredDroppedTable>
findMappedTableDrops(const DatabaseSchemaWALValidatedTransition & transition, const DatabaseSchemaWALLimits & limits)
{
    const auto & prepare = transition.getPrepare();
    const auto bytes = transition.getStagedArtifactBytes();
    if (bytes.size() != prepare.staged_artifacts.size())
        startupFail(StartupError::Code::IncompleteRecovery, "Atomic schema WAL artifact manifest lost its staged bytes");

    std::vector<AtomicAuthorityRecoveredDroppedTable> result;
    for (const auto & delta : prepare.dependent_object_deltas)
    {
        const bool ordinary_storage = delta.object.kind == SchemaObjectKind::Table || delta.object.kind == SchemaObjectKind::View
            || delta.object.kind == SchemaObjectKind::Dictionary;
        if (!ordinary_storage || !delta.before || delta.after)
            continue;

        const DatabaseSchemaWALStagedArtifactRef * installation_artifact = nullptr;
        size_t installation_index = 0;
        for (size_t index = 0; index < prepare.staged_artifacts.size(); ++index)
        {
            const auto & artifact = prepare.staged_artifacts[index];
            if (artifact.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord
                || artifact.image != DatabaseSchemaWALStagedArtifactImage::Before || artifact.object != delta.object
                || artifact.revision != delta.before->object_schema_revision)
            {
                continue;
            }
            if (installation_artifact)
                startupFail(StartupError::Code::IncompleteRecovery, "Atomic mapped DROP has duplicate installation records");
            installation_artifact = &artifact;
            installation_index = index;
        }
        if (!installation_artifact)
            startupFail(StartupError::Code::IncompleteRecovery, "Atomic mapped DROP has no installation record");

        DependentObjectMetadataInstallationRecord installation;
        try
        {
            installation = decodeDependentObjectMetadataInstallationRecord(bytes[installation_index], limits.installation_record);
        }
        catch (const DependentObjectMetadataInstallationRecordError &)
        {
            startupFail(StartupError::Code::IncompleteRecovery, "Atomic mapped DROP installation record is invalid");
        }
        if (installation.object != delta.object || installation.object_schema_revision != delta.before->object_schema_revision
            || installation.object_name.empty())
        {
            startupFail(StartupError::Code::AuthorityStateMismatch, "Atomic mapped DROP installation identity differs from its WAL delta");
        }
        result.push_back({.table_uuid = delta.object.object_uuid, .table_name = std::move(installation.object_name)});
    }
    return result;
}

AtomicAuthorityValidatedDependentObject validateRegisteredDependentObject(
    const SidecarExpectationRecord & expectation, std::string_view canonical_metadata_bytes, std::string_view canonical_sidecar_bytes)
{
    switch (expectation.object.kind)
    {
        case SchemaObjectKind::SyntheticTestObject:
            return validateSyntheticDependentObjectMetadata(expectation, canonical_metadata_bytes, canonical_sidecar_bytes);
        case SchemaObjectKind::Table:
        case SchemaObjectKind::View:
        case SchemaObjectKind::Dictionary:
            return {
                .object = expectation.object,
                .object_schema_revision = expectation.object_schema_revision,
                .physical_schema_fingerprint = expectation.physical_schema_fingerprint,
                .state = AtomicAuthorityDependentObjectValidationState::PendingTable,
            };
        case SchemaObjectKind::TypeDefinition:
            startupFail(StartupError::Code::IncompleteRecovery, "Atomic dependent-object kind has no registered startup validator");
    }
    startupFail(StartupError::Code::IncompleteRecovery, "Atomic dependent-object kind is not registered");
}

AuthorityRoot::Ptr activateRecoveredDependentObjectRoot(
    AtomicDatabaseSchemaMutationStorage & storage, AuthorityRoot::Ptr recovered_root, const AtomicAuthorityStartupLimits & limits)
{
    if (!recovered_root)
        startupFail(
            StartupError::Code::IncompleteRecovery, "Atomic dependent-object capability activation received no recovered authority root");
    if (recovered_root->getDatabaseUUID() != storage.getPaths().getDatabaseUUID())
        startupFail(
            StartupError::Code::AuthorityStateMismatch, "Atomic dependent-object capability activation root belongs to another database");

    const auto durable_state = storage.getCurrentAuthorityState();
    if (!durable_state || *durable_state != recovered_root->getAuthorityState())
        startupFail(
            StartupError::Code::AuthorityStateMismatch, "Atomic dependent-object capability activation root differs from durable state");

    const UInt64 persistent_capabilities = recovered_root->getPersistentCapabilityMask();
    if (persistent_capabilities == dependent_object_authority_capability_mask)
        return recovered_root;
    if (persistent_capabilities != definition_authority_capability_mask)
        startupFail(StartupError::Code::AuthorityStateMismatch, "Atomic startup cannot activate an unknown authority capability set");

    storage.maintainCheckpointBeforeMutation(*recovered_root);
    auto guard = storage.issueMutationGuard();
    const UInt64 durable_predecessor = guard.getDurablePredecessorTransactionID();
    if (durable_predecessor == std::numeric_limits<UInt64>::max())
        startupFail(
            StartupError::Code::InvalidDurableSequence, "Atomic dependent-object capability activation transaction ID domain is exhausted");

    auto activation = DependentObjectActivationPlanner::plan(
        *recovered_root, durable_predecessor + 1, recovered_root->getDatabaseCatalogEpoch(), {.schema_wal = limits.wal});
    auto replacement_root = activation.releaseReplacementRoot();
    static_cast<void>(executeDatabaseSchemaMutation(storage, guard, activation.getValidatedTransition(), limits.wal));

    const auto activated_durable_state = storage.getCurrentAuthorityState();
    if (!activated_durable_state || *activated_durable_state != replacement_root->getAuthorityState())
        startupFail(
            StartupError::Code::AuthorityStateMismatch,
            "Atomic dependent-object capability activation Commit did not become the durable WAL head");
    return replacement_root;
}

}

AtomicAuthorityStartupError::AtomicAuthorityStartupError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

bool isDegradableAtomicAuthorityStartupStorageError(AtomicDatabaseSchemaMutationStorageError::Code code) noexcept
{
    return isDegradableDurableStorageError(code);
}

AtomicAuthorityStartupStatusSnapshot::Ptr makeGlobalIncompleteAtomicAuthorityStartupStatus(UUID database_uuid, String stable_error)
{
    return makeGlobalIncompleteStartupStatus(database_uuid, std::move(stable_error));
}

AtomicAuthorityStartupResult recoverAtomicAuthorityAtStartup(
    AtomicDatabaseSchemaMutationStorage & storage,
    const AtomicAuthorityStartupLimits & limits,
    const AtomicAuthorityDependentObjectValidator & dependent_object_validator)
{
    AtomicAuthorityStartupResult result;
    const UUID database_uuid = storage.getPaths().getDatabaseUUID();
    DatabaseSchemaWALTransitionBase base;
    UInt64 covered_transaction_id = 0;
    StartupRepairCandidateCache startup_repair_candidates;
    const auto global_incomplete = [&](String stable_error)
    {
        result.authority_root.reset();
        result.pending_tables.clear();
        result.recovered_dropped_tables.clear();
        result.degraded_status = makeGlobalIncompleteStartupStatus(database_uuid, std::move(stable_error));
        return std::move(result);
    };

    try
    {
        if (!storage.hasDurableAuthorityMarker())
        {
            storage.cleanupNeverEnabledScaffold();
            return result;
        }

        {
            auto cleanup_guard = storage.issueMutationGuard();
            result.swept_unprepared_transactions = storage.sweepUnpreparedStaging(cleanup_guard);
            result.swept_retired_transactions = storage.sweepRetiredTransactions(cleanup_guard);
        }

        if (!storage.hasDurableAuthorityMarker())
        {
            storage.cleanupNeverEnabledScaffold();
            return result;
        }

        base = makeEmptyBase(database_uuid, limits.wal);
        if (auto checkpoint_image = storage.loadLatestCheckpoint())
        {
            auto checkpoint = DatabaseSchemaWALCheckpointBuilder::validateDecoded(
                std::move(checkpoint_image->checkpoint),
                checkpoint_image->inventory_snapshot_bytes,
                checkpoint_image->schema_graph_snapshot_bytes,
                limits.wal);
            const auto & record = checkpoint.getCheckpoint();
            if (record.authority_state.database_uuid != database_uuid)
                startupFail(StartupError::Code::AuthorityStateMismatch, "Atomic schema checkpoint belongs to another database");

            covered_transaction_id = record.covered_commit.transaction_id;
            base.authority_state = record.authority_state;
            base.authority_inventory = checkpoint.pinInventory();
            base.schema_graph = checkpoint.pinSchemaGraph();
        }

        const UInt64 durable_high_water = storage.getDurableHighWaterMark();
        if (covered_transaction_id > durable_high_water)
            startupFail(StartupError::Code::InvalidDurableSequence, "Atomic schema checkpoint is ahead of the durable high-water mark");

        auto transaction_ids = storage.listDurableTransactionIDs();
        validateStrictTransactionOrder(transaction_ids);
        for (size_t transaction_index = 0; transaction_index < transaction_ids.size(); ++transaction_index)
        {
            const UInt64 transaction_id = transaction_ids[transaction_index];
            if (transaction_id <= covered_transaction_id)
                continue;

            auto image = storage.loadTransactionForRecovery(transaction_id);
            if (image.prepare.transaction_id != transaction_id || image.prepare.after_authority_state.database_uuid != database_uuid)
                startupFail(StartupError::Code::InvalidDurableSequence, "Atomic schema WAL transaction identity does not match its path");

            /// A durable rollback marker proves that the before-image installation
            /// and its directory barrier completed. Its staged images may already
            /// have been collected by a prior retirement attempt, so do not require
            /// them merely to finish the idempotent WAL retirement.
            if (image.recovery_decision == DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
            {
                if (image.commit)
                    startupFail(
                        StartupError::Code::InvalidDurableSequence, "Rolled-back Atomic schema WAL transaction has a Commit marker");
                if (image.prepare.before_authority_state != base.authority_state)
                    startupFail(
                        StartupError::Code::InvalidDurableSequence, "Rolled-back Atomic schema WAL transaction breaks the state chain");
                auto guard = storage.issueMutationGuard();
                storage.retireRolledBackTransaction(guard, transaction_id);
                ++result.rolled_back_transactions;
                continue;
            }

            std::optional<DatabaseSchemaWALValidatedTransition> transition_holder;
            transition_holder.emplace(
                DatabaseSchemaWALTransitionBuilder::validateDecoded(
                    std::move(image.prepare), base, std::move(image.staged_artifact_bytes), limits.wal));
            auto & transition = *transition_holder;
            if (image.commit)
            {
                validateDatabaseSchemaWALCommit(transition, *image.commit, limits.wal);
                if (image.recovery_decision && image.recovery_decision != DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
                    startupFail(
                        StartupError::Code::InvalidDurableSequence, "Committed Atomic schema WAL transaction has a rollback decision");

                /// Successful transactions do not normally need a recovery marker,
                /// and replaying an obsolete After image could overwrite a newer
                /// transition. A mapped DROP is different: its tombstone is
                /// deliberately published after Commit. Only the terminal commit can
                /// have been interrupted in that window without a later transaction
                /// proving convergence.
                if (transaction_index + 1 == transaction_ids.size())
                {
                    auto recovered_drops = findMappedTableDrops(transition, limits.wal);
                    if (!recovered_drops.empty())
                    {
                        if (!image.recovery_decision)
                        {
                            auto guard = storage.issueMutationGuard();
                            const auto decision = recoverDatabaseSchemaMutation(storage, guard, transition, image.commit, limits.wal);
                            if (decision != DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
                                startupFail(
                                    StartupError::Code::InvalidDurableSequence, "Committed Atomic mapped DROP selected rollback recovery");
                            for (auto & recovered_drop : recovered_drops)
                                recovered_drop.tombstone_replayed = true;
                        }
                        result.recovered_dropped_tables.insert(
                            result.recovered_dropped_tables.end(),
                            std::make_move_iterator(recovered_drops.begin()),
                            std::make_move_iterator(recovered_drops.end()));
                    }
                }
                startup_repair_candidates.applyCommittedTransition(transition, limits.wal);
                base.authority_state = transition.getPrepare().after_authority_state;
                base.authority_inventory = transition.pinAfterInventory();
                base.schema_graph = transition.pinAfterGraph();
                ++result.completed_transactions;
                continue;
            }

            if (image.recovery_decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
                startupFail(StartupError::Code::InvalidDurableSequence, "Uncommitted Atomic schema WAL transaction is marked complete");
            if (transaction_index + 1 != transaction_ids.size())
                startupFail(
                    StartupError::Code::InvalidDurableSequence, "Unresolved Atomic schema Prepare is not the terminal WAL transaction");

            auto guard = storage.issueMutationGuard();
            const auto decision = recoverDatabaseSchemaMutation(storage, guard, transition, std::nullopt, limits.wal);
            if (decision != DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
                startupFail(StartupError::Code::InvalidDurableSequence, "Uncommitted Atomic schema recovery selected a non-rollback image");
            retireRolledBackDatabaseSchemaMutation(storage, guard, transaction_id);
            ++result.rolled_back_transactions;
        }

        const auto durable_state = storage.getCurrentAuthorityState();
        if (durable_state != base.authority_state)
            startupFail(StartupError::Code::AuthorityStateMismatch, "Recovered Atomic authority state differs from the durable WAL head");

        if (!base.authority_state)
        {
            if (storage.hasDurableAuthorityMarker())
                return global_incomplete("durable authority marker has no complete anchored authority state");
            storage.cleanupNeverEnabledScaffold();
            return result;
        }
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const DatabaseSchemaWALError & error)
    {
        if (error.code == DatabaseSchemaWALError::Code::InvalidConfiguration)
            throw;
        return global_incomplete("durable authority checkpoint or WAL history is invalid or inconsistent");
    }
    catch (const AtomicDatabaseSchemaMutationStorageError & error)
    {
        if (!isDegradableDurableStorageError(error.code))
            throw;
        return global_incomplete("durable authority storage preflight cannot be read or reconciled safely");
    }
    catch (const StartupError &)
    {
        return global_incomplete("durable authority checkpoint or WAL sequence is inconsistent");
    }

    if (!base.authority_inventory || !base.schema_graph)
        return global_incomplete("recovered authority is missing its anchored inventory or dependency graph");

    std::optional<AtomicDatabaseSchemaMutationReconciliation> reconciliation_holder;
    bool startup_exact_repair_attempted = false;
    try
    {
        reconciliation_holder.emplace(storage.readAndReconcileAuthorityRecords(*base.authority_inventory, *base.schema_graph));
    }
    catch (const AtomicDatabaseSchemaMutationStorageError & error)
    {
        if (!isDegradableDurableStorageError(error.code))
            throw;
        if (error.code == AtomicDatabaseSchemaMutationStorageError::Code::CorruptDurableState)
        {
            startup_exact_repair_attempted = true;
            try
            {
                if (attemptStartupExactRepairFromLocalWAL(storage, base, startup_repair_candidates, limits))
                {
                    ++result.completed_transactions;
                    reconciliation_holder.emplace(storage.readAndReconcileAuthorityRecords(*base.authority_inventory, *base.schema_graph));
                }
            }
            catch (const AtomicDatabaseSchemaMutationStorageError & retry_error)
            {
                if (!isDegradableDurableStorageError(retry_error.code))
                    throw;
            }
        }
        if (!reconciliation_holder)
        {
            result.degraded_status = makeDegradedStartupStatus(
                database_uuid,
                *base.authority_inventory,
                nullptr,
                limits.recovery,
                {
                    .invalid_definition = std::nullopt,
                    .invalid_definition_error = {},
                    .incomplete_definition_error = "durable authority records cannot be reconciled with their anchored inventory",
                });
            return result;
        }
    }
    /// Reconciliation deliberately leaves definition envelopes raw so recovery
    /// can retain a keyed INVALID diagnostic when no trusted repair source is
    /// available. A keyed recovery failure is therefore the first point where
    /// damaged-but-present definition bytes are known. Permit one exact local-
    /// WAL repair and one complete recovery retry; every later failure keeps the
    /// existing degraded startup behavior.
    for (;;)
    {
        auto & reconciliation = *reconciliation_holder;
        if (!reconciliation.dependent_objects.empty() && !dependent_object_validator)
            startupFail(StartupError::Code::IncompleteRecovery, "Atomic dependent objects have no registered metadata validator");

        std::vector<AuthorityRecordImage> recovery_images;
        recovery_images.reserve(reconciliation.authority_records.size());
        for (const auto & record_image : reconciliation.authority_records)
            recovery_images.push_back({record_image.key, record_image.canonical_bytes});

        std::vector<AuthorityDependentObjectResourceImage> resource_dependent_objects;
        resource_dependent_objects.reserve(reconciliation.dependent_objects.size());
        for (const auto & dependent_object : reconciliation.dependent_objects)
        {
            resource_dependent_objects.push_back({
                .object = dependent_object.expectation.object,
                .canonical_metadata_bytes = dependent_object.canonical_metadata_bytes,
                .canonical_sidecar_bytes = dependent_object.canonical_sidecar_bytes,
                .canonical_installation_record_bytes = dependent_object.canonical_installation_record_bytes,
            });
        }

        std::vector<AuthorityInventoryLeaf> leaves(
            base.authority_inventory->getLeaves().begin(), base.authority_inventory->getLeaves().end());
        const auto inventory_snapshot
            = makeAuthorityInventorySnapshot(database_uuid, std::move(leaves), limits.recovery.inventory_snapshot);
        const String inventory_snapshot_bytes = encodeAuthorityInventorySnapshot(inventory_snapshot, limits.recovery.inventory_snapshot);
        const String schema_graph_snapshot_bytes = base.schema_graph->encodeSnapshot();

        const UInt64 recovered_generation = recoveredTypeIndexGeneration(*base.authority_state);
        try
        {
            result.authority_root = recoverAuthorityRoot(
                *base.authority_state,
                recovered_generation,
                inventory_snapshot_bytes,
                schema_graph_snapshot_bytes,
                recovery_images,
                limits.recovery,
                resource_dependent_objects);
        }
        catch (const AuthorityRecoveryError & error)
        {
            const bool keyed_invalid = error.record_key && error.record_key->record_kind == AuthorityInventoryRecordKind::TypeDefinition
                && keyedRecoveryFailureIsInvalidDefinition(error.code);
            if (keyed_invalid && !startup_exact_repair_attempted)
            {
                startup_exact_repair_attempted = true;
                bool exact_repair_completed = false;
                try
                {
                    exact_repair_completed = attemptStartupExactRepairFromLocalWAL(storage, base, startup_repair_candidates, limits);
                    if (exact_repair_completed)
                    {
                        ++result.completed_transactions;
                        auto repaired_reconciliation
                            = storage.readAndReconcileAuthorityRecords(*base.authority_inventory, *base.schema_graph);
                        reconciliation_holder.emplace(std::move(repaired_reconciliation));
                        continue;
                    }
                }
                catch (const AtomicDatabaseSchemaMutationStorageError & retry_error)
                {
                    if (!isDegradableDurableStorageError(retry_error.code))
                        throw;
                }
                if (exact_repair_completed)
                {
                    result.degraded_status = makeDegradedStartupStatus(
                        database_uuid,
                        *base.authority_inventory,
                        nullptr,
                        limits.recovery,
                        {
                            .invalid_definition = std::nullopt,
                            .invalid_definition_error = {},
                            .incomplete_definition_error = "durable authority records cannot be reconciled with their anchored inventory",
                        });
                    return result;
                }
            }

            const String stable_error = recoveryFailureDiagnostic(error.code);
            result.degraded_status = makeDegradedStartupStatus(
                database_uuid,
                *base.authority_inventory,
                &reconciliation,
                limits.recovery,
                {
                    .invalid_definition = keyed_invalid ? error.record_key : std::nullopt,
                    .invalid_definition_error = keyed_invalid ? stable_error : String{},
                    .incomplete_definition_error = stable_error,
                });
            return result;
        }
        break;
    }

    auto & reconciliation = *reconciliation_holder;

    if (!result.authority_root || result.authority_root->getAuthorityState() != *base.authority_state)
        startupFail(StartupError::Code::IncompleteRecovery, "Atomic authority reconstruction did not reproduce the durable state");

    try
    {
        for (auto & dependent_object : reconciliation.dependent_objects)
        {
            AtomicAuthorityValidatedDependentObject validated;
            try
            {
                validated = dependent_object_validator(
                    dependent_object.expectation, dependent_object.canonical_metadata_bytes, dependent_object.canonical_sidecar_bytes);
            }
            catch (const SyntheticObjectMetadataError &)
            {
                startupFail(StartupError::Code::IncompleteRecovery, "Atomic dependent-object metadata validation failed");
            }
            const auto & expectation = dependent_object.expectation;
            if (validated.object != expectation.object || validated.object_schema_revision != expectation.object_schema_revision
                || validated.physical_schema_fingerprint != expectation.physical_schema_fingerprint)
                startupFail(StartupError::Code::AuthorityStateMismatch, "Atomic dependent-object metadata differs from its expectation");
            if (validated.state == AtomicAuthorityDependentObjectValidationState::Validated)
                continue;
            if (validated.state == AtomicAuthorityDependentObjectValidationState::PendingTable)
            {
                if (expectation.object.kind != SchemaObjectKind::Table && expectation.object.kind != SchemaObjectKind::View
                    && expectation.object.kind != SchemaObjectKind::Dictionary)
                    startupFail(StartupError::Code::IncompleteRecovery, "Atomic non-storage dependent object cannot remain pending");
                if (result.pending_tables.size() >= limits.recovery.root.maximum_expectation_records)
                    startupFail(StartupError::Code::IncompleteRecovery, "Atomic pending Table count exceeds the recovery limit");
                result.pending_tables.push_back({
                    .expectation = expectation,
                    .object_name = std::move(dependent_object.object_name),
                    .canonical_metadata_bytes = std::move(dependent_object.canonical_metadata_bytes),
                    .canonical_sidecar_bytes = std::move(dependent_object.canonical_sidecar_bytes),
                });
                continue;
            }
            startupFail(StartupError::Code::IncompleteRecovery, "Atomic dependent-object validator returned an unknown state");
        }
    }
    catch (const StartupError &)
    {
        result.authority_root.reset();
        result.pending_tables.clear();
        result.degraded_status = makeDegradedStartupStatus(
            database_uuid,
            *base.authority_inventory,
            &reconciliation,
            limits.recovery,
            {
                .invalid_definition = std::nullopt,
                .invalid_definition_error = {},
                .incomplete_definition_error = "dependent-object metadata does not match the recovered authority expectation",
            });
        return result;
    }

    return result;
}

AtomicAuthorityStartupResult
recoverAndActivateAtomicAuthorityAtStartup(AtomicDatabaseSchemaMutationStorage & storage, const AtomicAuthorityStartupLimits & limits)
{
    auto result = recoverAtomicAuthorityAtStartup(storage, limits, validateRegisteredDependentObject);
    if (result.authority_root)
        result.authority_root = activateRecoveredDependentObjectRoot(storage, std::move(result.authority_root), limits);
    return result;
}

}
