#pragma once

#include <DataTypes/UDT/Catalog.h>
#include <DataTypes/UDT/ResourceLimits.h>

#include <Core/Types.h>

#include <cstddef>
#include <memory>
#include <string_view>
#include <variant>
#include <vector>

namespace DB::UDT
{

enum class TypeAuthorityCapability : UInt64
{
    TransientResolution = 1ULL << 0,
    DurableAlias = 1ULL << 1,
    Policy = 1ULL << 2,
    Replication = 1ULL << 3,
    BackupRecovery = 1ULL << 4,
    Limits = 1ULL << 5,
    Templates = 1ULL << 6,
    DecreasingRecursion = 1ULL << 7,
    Manifest = 1ULL << 8,
};

using TypeAuthorityCapabilityMask = UInt64;

constexpr TypeAuthorityCapabilityMask typeAuthorityCapabilityBit(TypeAuthorityCapability capability) noexcept
{
    return static_cast<TypeAuthorityCapabilityMask>(capability);
}

struct TypeAuthorityLimits
{
    UInt64 maximum_definitions = 0;
    /// Per-definition logical retained footprint: immutable object/vector
    /// elements plus owned string payloads, independent of allocator capacity.
    UInt64 maximum_definition_bytes = 0;
    UInt64 maximum_template_nodes = 0;
    UInt64 maximum_direct_dependencies = 0;
    /// Attempt-wide distinct-definition admission budget, including each
    /// requested root. Compositional certificates do not materialize an O(N)
    /// transitive identity set at authority creation, so specialization
    /// enforces this dynamically while traversing the pinned generation.
    UInt64 maximum_transitive_dependencies = 0;
    UInt64 maximum_checker_work = 0;

    bool operator==(const TypeAuthorityLimits &) const = default;
};

struct TypeAuthorityCapabilities
{
    UInt16 adapter_abi = 1;
    TypeAuthorityCapabilityMask mask = 0;
    TypeAuthorityLimits limits;

    [[nodiscard]] constexpr bool contains(TypeAuthorityCapability capability) const noexcept
    {
        return (mask & typeAuthorityCapabilityBit(capability)) != 0;
    }

    [[nodiscard]] constexpr bool containsAll(TypeAuthorityCapabilityMask required) const noexcept { return (mask & required) == required; }

    bool operator==(const TypeAuthorityCapabilities &) const = default;
};

/// Context-free authority boundary. It exposes one already-validated immutable
/// catalog session to a synthetic binder; it owns no persistence or DDL method.
/// Durable hooks are added by the owning persistent adapter ABI, never inferred
/// from a database engine name.
class IAuthorityAdapter
{
protected:
    using SnapshotFindByIdentity = Definition::Ptr (*)(const void *, const DefinitionIdentity &);
    using SnapshotFindByName = Definition::Ptr (*)(const void *, std::string_view);
    using SnapshotGetGeneration = UInt64 (*)(const void *) noexcept;
    using SnapshotGetEffectiveResourceLimits = const EffectiveResourceLimits * (*)(const void *) noexcept;
    using SnapshotRelease = void (*)(void *, std::size_t) noexcept;

    struct SnapshotResolutionOperations final
    {
        SnapshotFindByIdentity find_by_identity;
        SnapshotFindByName find_by_name;
        SnapshotGetGeneration get_generation;
        /// Null for transient/test authorities which have no persisted
        /// database quota. A non-null tuple belongs to `view` and is valid for
        /// exactly the lifetime of this snapshot session.
        SnapshotGetEffectiveResourceLimits get_effective_resource_limits = nullptr;
    };

public:
    /// Snapshot-consistent authority lookup scope. A transient backend hazards
    /// one catalog root and retains its test authority state. A durable backend
    /// carries one inline hazard lease whose O(1) destructor only releases its
    /// preallocated slot; reclamation remains an explicit admin operation.
    class ResolutionSession final
    {
    public:
        ResolutionSession(const ResolutionSession &) = delete;
        ResolutionSession & operator=(const ResolutionSession &) = delete;
        ResolutionSession(ResolutionSession &&) noexcept = default;
        ResolutionSession & operator=(ResolutionSession &&) = delete;
        ~ResolutionSession() = default;

        Definition::Ptr findByIdentity(const DefinitionIdentity & identity) const;
        Definition::Ptr findByName(std::string_view normalized_local_name) const;
        UInt64 getGeneration() const noexcept;
        /// Returns the immutable implementation/server/database/adapter
        /// minimum belonging to the same generation as every lookup in this
        /// session. The pointer must not escape the session lifetime.
        const EffectiveResourceLimits * getEffectiveResourceLimits() const noexcept;

    private:
        ResolutionSession(std::shared_ptr<const void> lifetime_token_, Catalog::ResolutionSession catalog_session_);

        struct CatalogBackend final
        {
            CatalogBackend(std::shared_ptr<const void> lifetime_token_, Catalog::ResolutionSession catalog_session_);

            std::shared_ptr<const void> lifetime_token;
            Catalog::ResolutionSession catalog_session;
        };

        struct SnapshotBackend final
        {
            SnapshotBackend(
                const void * view_,
                SnapshotResolutionOperations operations_,
                void * release_context_,
                std::size_t release_token_,
                SnapshotRelease release_) noexcept;
            SnapshotBackend(const SnapshotBackend &) = delete;
            SnapshotBackend & operator=(const SnapshotBackend &) = delete;
            SnapshotBackend(SnapshotBackend && other) noexcept;
            SnapshotBackend & operator=(SnapshotBackend &&) = delete;
            ~SnapshotBackend();

            const void * view;
            SnapshotResolutionOperations operations;
            void * release_context;
            std::size_t release_token;
            SnapshotRelease release;
        };

        ResolutionSession(
            const void * view_,
            SnapshotResolutionOperations operations_,
            void * release_context_,
            std::size_t release_token_,
            SnapshotRelease release_) noexcept;

        friend class IAuthorityAdapter;
        std::variant<CatalogBackend, SnapshotBackend> backend;
    };

    virtual ~IAuthorityAdapter() = default;

    [[nodiscard]] virtual const TypeAuthorityCapabilities & getCapabilities() const noexcept = 0;
    [[nodiscard]] virtual UUID getDatabaseUUID() const noexcept = 0;
    /// Pins exactly one immutable generation for all lookups made by one
    /// resolution attempt. It never copies or exposes the O(N) catalog.
    [[nodiscard]] virtual ResolutionSession beginResolutionSession() const = 0;

    /// Throws before lookup, binding, or publication when a required
    /// capability is absent. There is no physical-only fallback.
    virtual void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const = 0;

protected:
    static ResolutionSession
    makeResolutionSession(std::shared_ptr<const void> lifetime_token, Catalog::ResolutionSession catalog_session);

    /// Builds a no-allocation session over one immutable snapshot. The caller
    /// must keep the release context alive until it has closed admission and
    /// drained all leases. A null release callback denotes static empty state.
    static ResolutionSession makeSnapshotResolutionSession(
        const void * view,
        SnapshotResolutionOperations operations,
        void * release_context = nullptr,
        std::size_t release_token = 0,
        SnapshotRelease release = nullptr) noexcept;
};

using AuthorityAdapterPtr = std::shared_ptr<const IAuthorityAdapter>;

/// Process-stable, allocation-free fail-closed authority for engines that do
/// not support UDTs.
const IAuthorityAdapter & getUnsupportedAuthorityAdapter() noexcept;
AuthorityAdapterPtr makeUnsupportedAuthorityAdapter() noexcept;

/// Synthetic in-memory authority for focused tests. It can advertise only the
/// transient/checker capabilities passed by the caller; durable capabilities
/// are rejected because this adapter never implements durable contracts.
AuthorityAdapterPtr makeTransientAuthorityAdapter(
    UUID database_uuid, TypeAuthorityCapabilities capabilities, std::vector<Definition::Ptr> definitions);

}
