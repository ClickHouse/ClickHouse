#pragma once

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <Databases/UDT/AuthorityStorageOperationGate.h>
#include <Interpreters/StorageID.h>

#include <mutex>
#include <optional>
#include <set>
#include <tuple>
#include <vector>

namespace DB::UDT
{

/// Execution identity retained by a physical query-result-cache entry.  It is
/// deliberately not part of the cache hash: it is evidence attached to the
/// stored value and is revalidated against the live catalog on a cache hit.
enum class QueryResultCacheStorageKind : UInt8
{
    Storage,
    View,
};

/// Closed syntax classes whose semantic activation cannot be decided before
/// QueryAnalyzer has selected an exact bound column/use.  The mask is retained
/// in a cache entry so a pre-analyzer hit never reuses a proof collected for a
/// less demanding syntax surface.
enum class QueryResultCacheContextualSinkCandidate : UInt8
{
    Equality = 1U << 0,
    In = 1U << 1,
    Has = 1U << 2,
};

using QueryResultCacheContextualSinkCandidateMask = UInt8;

constexpr QueryResultCacheContextualSinkCandidateMask
queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate candidate) noexcept
{
    return static_cast<QueryResultCacheContextualSinkCandidateMask>(candidate);
}

inline constexpr QueryResultCacheContextualSinkCandidateMask all_query_result_cache_contextual_sink_candidates
    = queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::Equality)
    | queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::In)
    | queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::Has);

struct QueryResultCacheUDTBindingIdentity
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};
    SemanticCapabilityMask semantic_capabilities = 0;
    AuthorityRootGraphIdentity authority_root;

    bool operator==(const QueryResultCacheUDTBindingIdentity &) const = default;
};

struct QueryResultCacheStorageDependency
{
    StorageID storage_id;
    String engine_name;
    QueryResultCacheStorageKind kind = QueryResultCacheStorageKind::Storage;
    std::optional<QueryResultCacheUDTBindingIdentity> udt_binding;

    bool operator==(const QueryResultCacheStorageDependency & other) const noexcept
    {
        return std::tie(storage_id.database_name, storage_id.table_name, storage_id.uuid, engine_name, kind, udt_binding)
            == std::tie(
                   other.storage_id.database_name,
                   other.storage_id.table_name,
                   other.storage_id.uuid,
                   other.engine_name,
                   other.kind,
                   other.udt_binding);
    }
};

struct QueryResultCacheStorageDependencyLess
{
    bool operator()(const QueryResultCacheStorageDependency & lhs, const QueryResultCacheStorageDependency & rhs) const noexcept
    {
        /// Storage UUID is the catalog identity.  Keying the collector by the
        /// complete descriptive image would admit two names/engines/bindings
        /// for the same UUID as independent dependencies instead of detecting
        /// that one query observed two metadata generations.
        return lhs.storage_id.uuid < rhs.storage_id.uuid;
    }
};

struct QueryResultCacheStorageDependencyProof
{
    std::vector<QueryResultCacheStorageDependency> dependencies;
    QueryResultCacheContextualSinkCandidateMask contextual_sink_candidates = 0;

    bool operator==(const QueryResultCacheStorageDependencyProof &) const = default;
};

/// Shared by the root analyzer and trusted nested View analyzers.  The set is
/// populated only at exact storage-open hooks, stays sorted/deduplicated, and
/// degrades to an uncacheable proof instead of widening its finite bound.
class QueryResultCacheStorageDependencyCollector final
{
public:
    static constexpr size_t maximum_dependencies = 16'384;
    static constexpr size_t maximum_retained_bytes = 4ULL << 20;

    explicit QueryResultCacheStorageDependencyCollector(
        bool boundary_saw_storage_reference_, QueryResultCacheContextualSinkCandidateMask contextual_sink_candidates_ = 0) noexcept
        : boundary_saw_storage_reference(boundary_saw_storage_reference_)
        , contextual_sink_candidates(contextual_sink_candidates_)
    {
        if (contextual_sink_candidates & ~all_query_result_cache_contextual_sink_candidates)
            proof_complete = false;
    }

    void record(
        const StorageID & storage_id,
        String engine_name,
        QueryResultCacheStorageKind kind,
        const BoundObjectTypeReferences::Ptr & bound_references,
        const AuthorityStorageReadContinuationEvidence::Ptr & exact_root_evidence = {})
    {
        std::lock_guard lock(mutex);
        if (!proof_complete)
            return;

        /// A name-only/generated storage cannot be revalidated exactly at the
        /// pre-analyzer cache-read boundary.
        if (!storage_id.hasUUID() || engine_name.empty())
        {
            proof_complete = false;
            resolution_complete = false;
            return;
        }

        /// Storage UUID, engine and storage-local bound metadata do not identify
        /// the SELECT definition generation of a View. ALTER VIEW can therefore
        /// preserve this entire image while changing the execution closure. Do
        /// not mint a reusable pre-analyzer cache proof until that definition
        /// generation has an immutable identity which can be revalidated.
        if (kind == QueryResultCacheStorageKind::View)
        {
            proof_complete = false;
            resolution_complete = false;
            return;
        }

        std::optional<QueryResultCacheUDTBindingIdentity> udt_binding;
        if (bound_references)
        {
            /// A mapped object image alone does not identify its execution
            /// semantics. Definition rename/comment publications can change
            /// the exact authority root without changing the storage UUID,
            /// sidecar or physical bytes, so only snapshot-bound current-root
            /// evidence may mint a reusable cache dependency.
            if (!exact_root_evidence)
            {
                proof_complete = false;
                resolution_complete = false;
                return;
            }

            const AuthorityObjectImageIdentity expected_object{
                .object = bound_references->getObject(),
                .object_schema_revision = bound_references->getObjectSchemaRevision(),
                .sidecar_hash = bound_references->getSidecarHash(),
                .physical_schema_fingerprint = bound_references->getPhysicalSchemaFingerprint(),
            };
            const auto & verification_stamp = exact_root_evidence->getVerificationStamp();
            if (exact_root_evidence->getObjectImage() != expected_object || !verification_stamp
                || verification_stamp->getVerifiedObject() != expected_object
                || verification_stamp->getVerifiedRoot() != exact_root_evidence->getPinnedRoot().authority_root)
            {
                proof_complete = false;
                resolution_complete = false;
                return;
            }

            udt_binding = QueryResultCacheUDTBindingIdentity{
                .object = bound_references->getObject(),
                .object_schema_revision = bound_references->getObjectSchemaRevision(),
                .sidecar_hash = bound_references->getSidecarHash(),
                .physical_schema_fingerprint = bound_references->getPhysicalSchemaFingerprint(),
                .semantic_capabilities = bound_references->getSemanticCapabilities(),
                .authority_root = exact_root_evidence->getPinnedRoot(),
            };
        }
        else if (exact_root_evidence)
        {
            proof_complete = false;
            resolution_complete = false;
            return;
        }

        saw_resolved_storage_reference = true;
        QueryResultCacheStorageDependency dependency{storage_id, std::move(engine_name), kind, std::move(udt_binding)};
        const auto existing = dependencies.find(dependency);
        if (existing != dependencies.end())
        {
            /// Opening two metadata generations for one UUID cannot produce a
            /// single exact execution image.
            if (*existing != dependency)
            {
                proof_complete = false;
                resolution_complete = false;
            }
            return;
        }
        if (dependencies.size() == maximum_dependencies)
        {
            proof_complete = false;
            resolution_complete = false;
            return;
        }
        const size_t string_bytes
            = dependency.storage_id.database_name.size() + dependency.storage_id.table_name.size() + dependency.engine_name.size();
        /// Budget both the collector's ordered-set image and the eventual
        /// vector proof copied from it.  The fixed allowance includes the two
        /// dependency values, set links/allocation slack and vector storage;
        /// dynamic strings are retained independently by both images.
        static constexpr size_t fixed_dependency_bytes = 2 * sizeof(QueryResultCacheStorageDependency) + 8 * sizeof(void *) + 64;
        if (fixed_dependency_bytes > maximum_retained_bytes || string_bytes > (maximum_retained_bytes - fixed_dependency_bytes) / 2)
        {
            proof_complete = false;
            resolution_complete = false;
            return;
        }
        const size_t dependency_bytes = fixed_dependency_bytes + 2 * string_bytes;
        if (retained_bytes > maximum_retained_bytes - dependency_bytes)
        {
            proof_complete = false;
            resolution_complete = false;
            return;
        }
        /// Any newly opened exact storage extends the execution closure and
        /// invalidates an earlier analyzer scope's completion publication.
        resolution_complete = false;
        dependencies.emplace(std::move(dependency));
        retained_bytes += dependency_bytes;
    }

    /// Analyzer-created table-function storages such as numbers() and eval()
    /// have no catalog UUID to revalidate. They are nevertheless a complete
    /// physical boundary observation when they carry no persisted UDT binding
    /// or authority continuation evidence. Keep them out of the dependency
    /// vector while recording that the syntactic storage reference was
    /// resolved; UUID-bearing and UDT-bearing storages must use record().
    void recordResolvedTransientTableFunction(
        const StorageID & storage_id,
        const String & engine_name,
        const BoundObjectTypeReferences::Ptr & bound_references,
        const AuthorityStorageReadContinuationEvidence::Ptr & exact_root_evidence = {})
    {
        std::lock_guard lock(mutex);
        if (!proof_complete)
            return;
        if (storage_id.hasUUID() || engine_name.empty() || bound_references || exact_root_evidence)
        {
            proof_complete = false;
            resolution_complete = false;
            return;
        }

        saw_resolved_storage_reference = true;
        resolution_complete = false;
    }

    bool tryBeginResolution(const void * owner) noexcept
    {
        std::lock_guard lock(mutex);
        if (!owner)
            return false;
        if (resolution_owner && resolution_owner != owner)
            return false;
        resolution_owner = owner;
        resolution_complete = false;
        return true;
    }

    void markResolutionComplete(const void * owner) noexcept
    {
        std::lock_guard lock(mutex);
        if (!owner || resolution_owner != owner)
            return;
        resolution_complete = true;
        resolution_owner = nullptr;
    }

    /// Exception-safe counterpart to tryBeginResolution.  An abandoned pass
    /// permanently makes this collector uncacheable: dependencies accumulated
    /// before an exception are not a complete proof for any later generation.
    void abandonResolution(const void * owner) noexcept
    {
        std::lock_guard lock(mutex);
        if (!owner)
            return;
        proof_complete = false;
        resolution_complete = false;
        if (resolution_owner == owner)
            resolution_owner = nullptr;
    }

    [[nodiscard]] std::optional<QueryResultCacheStorageDependencyProof> snapshotIfComplete() const
    {
        std::lock_guard lock(mutex);
        if (!proof_complete || !resolution_complete || (boundary_saw_storage_reference && !saw_resolved_storage_reference))
            return std::nullopt;

        QueryResultCacheStorageDependencyProof result;
        result.dependencies.assign(dependencies.begin(), dependencies.end());
        result.contextual_sink_candidates = contextual_sink_candidates;
        return result;
    }

    [[nodiscard]] QueryResultCacheContextualSinkCandidateMask getContextualSinkCandidates() const noexcept
    {
        return contextual_sink_candidates;
    }

private:
    mutable std::mutex mutex;
    std::set<QueryResultCacheStorageDependency, QueryResultCacheStorageDependencyLess> dependencies;
    const bool boundary_saw_storage_reference;
    const QueryResultCacheContextualSinkCandidateMask contextual_sink_candidates;
    size_t retained_bytes = 0;
    const void * resolution_owner = nullptr;
    bool resolution_complete = false;
    bool proof_complete = true;
    bool saw_resolved_storage_reference = false;
};

}
