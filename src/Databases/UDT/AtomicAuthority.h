#pragma once

#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/ServerResourceQuotaTracker.h>

#include <DataTypes/UDT/IAuthorityAdapter.h>

#include <Core/Types.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

class ILifecycleSnapshot;

/// Optional database-owned observer that can prepare an admission-state
/// transition together with an authority-root publication. Preparation is
/// fallible and happens before durable mutation; publish is allocation-free
/// and no-throw after the authority root becomes visible.
class IAtomicAuthorityPublicationObserver
{
public:
    class PreparedTransition
    {
    public:
        virtual ~PreparedTransition() = default;
        virtual void publish() noexcept = 0;
    };

    virtual ~IAtomicAuthorityPublicationObserver() = default;
    virtual std::unique_ptr<PreparedTransition> prepareAuthorityPublication(const AuthorityRoot & before, const AuthorityRoot & after) = 0;
};

/// Fixed definition-only profile advertised by a local Atomic database. Persistent
/// capability activation remains a property of the published composite root.
constexpr TypeAuthorityCapabilities atomicDatabaseAuthorityCapabilities() noexcept
{
    return {
        .adapter_abi = 1,
        .mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::DecreasingRecursion),
        .limits = {
            /// Adapter capability is the implementation ceiling. The
            /// normative database layer remains 10,000 and may be raised by a
            /// persisted database override only up to this 100,000 maximum.
            .maximum_definitions = 100'000,
            .maximum_definition_bytes = 256ULL << 10,
            .maximum_template_nodes = 4'096,
            .maximum_direct_dependencies = 256,
            .maximum_transitive_dependencies = 1'024,
            .maximum_checker_work = 65'536,
        },
    };
}

struct AtomicAuthorityPublicationLimits
{
    std::size_t hazard_slot_count = 256;
    UInt64 maximum_retired_root_count = 8;
    /// Deterministic logical ownership charge, not allocator-resident bytes.
    UInt64 maximum_retired_root_charged_bytes = 4ULL << 30;
};

struct AtomicAuthorityRetirementState
{
    UInt64 retired_root_count = 0;
    UInt64 retired_root_charged_bytes = 0;
    /// Logical charge carried by the current authority-owned root. This stays
    /// at the complete wrapper-plus-payload charge across content-neutral
    /// activation, retirement scans, and shutdown draining.
    UInt64 current_root_charged_bytes = 0;
    UInt64 active_hazard_slots = 0;
    bool shutdown = false;

    bool operator==(const AtomicAuthorityRetirementState &) const = default;
};

/// Bounded-work evidence for one publication preparation. A dependent-object
/// edit which retains the exact definition content must validate no unchanged
/// definition record under the writer mutex.
struct AtomicAuthorityPublicationStatistics
{
    UInt64 definition_records_validated = 0;
    bool reused_definition_validation = false;
};

class AtomicAuthorityError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        DatabaseMismatch,
        EpochMismatch,
        CapabilityMismatch,
        GenerationMismatch,
        LimitExceeded,
        HazardSlotsExhausted,
        Shutdown,
    };

    AtomicAuthorityError(Code code_, std::string_view message);

    const Code code;
};

/// Atomic-database authority publication kernel. It deliberately owns one
/// atomic pointer to the complete catalog/inventory/graph value, so a reader
/// cannot observe components from different database catalog epochs.
class AtomicAuthority final : public IAuthorityAdapter
{
public:
    using CompositeRoot = AuthorityRoot;
    using CompositeRootPtr = CompositeRoot::Ptr;

    /// Writer/admin snapshot for replacement construction and diagnostics. It
    /// exposes one immutable root while holding a fixed hazard slot. Releasing
    /// it is O(1) and never reclaims a root.
    class RootSnapshot final
    {
    public:
        RootSnapshot(const RootSnapshot &) = delete;
        RootSnapshot & operator=(const RootSnapshot &) = delete;
        RootSnapshot(RootSnapshot && other) noexcept;
        RootSnapshot & operator=(RootSnapshot &&) = delete;
        ~RootSnapshot();

        explicit operator bool() const noexcept { return root != nullptr; }
        const CompositeRoot & get() const;
        const CompositeRoot * operator->() const noexcept { return root; }

    private:
        RootSnapshot(void * hazard_domain_, std::size_t hazard_slot_, const CompositeRoot * root_) noexcept;

        friend class AtomicAuthority;
        void * hazard_domain;
        std::size_t hazard_slot;
        const CompositeRoot * root;
    };

    class PreparedPublication final
    {
    public:
        PreparedPublication(const PreparedPublication &) = delete;
        PreparedPublication & operator=(const PreparedPublication &) = delete;
        PreparedPublication(PreparedPublication &&) noexcept = default;
        PreparedPublication & operator=(PreparedPublication &&) = delete;
        ~PreparedPublication() = default;

        const AtomicAuthorityPublicationStatistics & getStatistics() const noexcept { return statistics; }

    private:
        PreparedPublication(
            const AtomicAuthority * owner_,
            const CompositeRoot * expected_root_,
            UInt64 expected_database_catalog_epoch_,
            CompositeRootPtr replacement_root_,
            UInt64 replacement_retirement_charged_bytes_,
            UInt64 expected_retiring_root_charged_bytes_,
            bool transfers_content_payload_charge_,
            ServerResourceQuotaTracker::PreparedReservation server_quota_reservation_,
            IAtomicAuthorityPublicationObserver * publication_observer_,
            std::unique_ptr<IAtomicAuthorityPublicationObserver::PreparedTransition> observer_transition_,
            AtomicAuthorityPublicationStatistics statistics_) noexcept;

        friend class AtomicAuthority;
        const AtomicAuthority * owner;
        const CompositeRoot * expected_root;
        UInt64 expected_database_catalog_epoch;
        /// Declared before the prepared root so rollback destroys the root
        /// before returning its prospective process-wide charge.
        ServerResourceQuotaTracker::PreparedReservation server_quota_reservation;
        CompositeRootPtr replacement_root;
        UInt64 replacement_retirement_charged_bytes;
        UInt64 expected_retiring_root_charged_bytes;
        bool transfers_content_payload_charge;
        IAtomicAuthorityPublicationObserver * publication_observer;
        std::unique_ptr<IAtomicAuthorityPublicationObserver::PreparedTransition> observer_transition;
        AtomicAuthorityPublicationStatistics statistics;
    };

    AtomicAuthority(
        UUID database_uuid_,
        TypeAuthorityCapabilities capabilities_,
        CompositeRootPtr initial_root = {},
        const AtomicAuthorityPublicationLimits & publication_limits = {});

    AtomicAuthority(const AtomicAuthority &) = delete;
    AtomicAuthority & operator=(const AtomicAuthority &) = delete;
    AtomicAuthority(AtomicAuthority &&) = delete;
    AtomicAuthority & operator=(AtomicAuthority &&) = delete;
    ~AtomicAuthority() override;

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return database_uuid; }
    ResolutionSession beginResolutionSession() const override;
    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override;

    /// Pins the complete current authority value for schema preparation. An
    /// empty result is the initialized-but-not-yet-published state.
    [[nodiscard]] RootSnapshot acquireCurrentRoot() const;

    /// One exact lifecycle/introspection view over the same composite-root
    /// hazard used by resolution. An empty holder yields an empty snapshot.
    [[nodiscard]] std::unique_ptr<const ILifecycleSnapshot> acquireLifecycleSnapshot() const;

    /// Performs every fallible publication check and captures the exact root
    /// against which the replacement was prepared.
    [[nodiscard]] PreparedPublication preparePublication(CompositeRoot::Ptr replacement_root) const;

    /// DatabaseAtomic installs its runtime observer only while both objects
    /// are owned under schema serialization, and clears it before draining the
    /// runtime. No prepared publication may cross this boundary.
    void setPublicationObserver(IAtomicAuthorityPublicationObserver * observer) noexcept;

    /// Publishes with one raw-pointer exchange plus ownership moves. A mismatch
    /// means the schema-mutation serialization contract was violated after
    /// preparation. No root is destroyed on this path.
    void publish(PreparedPublication && publication) noexcept;

    /// True only while the currently published value is the exact initial
    /// definition-only root. DatabaseAtomic uses this as the post-commit activation
    /// invariant: an empty holder or a later catalog epoch cannot be exposed
    /// as an active adapter through this seam.
    bool isFirstPublicationReadyForActivation() const noexcept;

    /// Explicit writer/admin reclamation. Potentially large destruction occurs
    /// outside the internal writer mutex and must be called outside a schema
    /// mutation guard.
    AtomicAuthorityRetirementState scanRetired();
    AtomicAuthorityRetirementState getRetirementState() const;

    /// Detach lifecycle: closes lease admission and drains every hazard before
    /// destroying the local domain. A live session may outlive publication,
    /// but not completed teardown; callers must not retain one on the thread
    /// performing this drain. The destructor invokes the same operation.
    void shutdownAndDrain() noexcept;
    bool isShutdown() const noexcept;

private:
    static Definition::Ptr findByIdentityInSnapshot(const void * view, const DefinitionIdentity & identity);
    static Definition::Ptr findByNameInSnapshot(const void * view, std::string_view normalized_local_name);
    static UInt64 getSnapshotGeneration(const void * view) noexcept;
    static const EffectiveResourceLimits * getSnapshotEffectiveResourceLimits(const void * view) noexcept;

    const UUID database_uuid;
    const TypeAuthorityCapabilities capabilities;
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
