#include <Databases/UDT/AuthorityVerificationRuntimeState.h>

#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <IO/WriteHelpers.h>

#include <Common/CacheLine.h>
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace ProfileEvents
{
extern const Event UDTAuthorityQuarantinePublications;
extern const Event UDTAuthorityQuarantinedObjects;
extern const Event UDTAuthorityQuarantineFailClosedPublications;
extern const Event UDTAuthorityQuarantineReleases;
extern const Event UDTAuthorityQuarantineReleasedObjects;
}

namespace DB::FailPoints
{
extern const char udt_authority_runtime_pause_after_publication_waiter_registration[];
}

namespace DB::UDT
{
namespace
{

using RuntimeError = AuthorityVerificationRuntimeStateError;
using ConsumeStatus = AuthorityVerificationRuntimeConsumeStatus;
using CursorStatus = AuthorityVerificationCursorDecisionStatus;

constexpr std::size_t maximum_supported_hazard_slots = 65'536;
constexpr UInt64 maximum_supported_retired_snapshots = 65'537;
constexpr UInt64 maximum_thread_operation_commit_fence_depth = 65'536;

void incrementProfileEventNoThrow(ProfileEvents::Event event, ProfileEvents::Count amount = 1) noexcept
{
    try
    {
        ProfileEvents::incrementNoTrace(event, amount);
    }
    catch (...)
    {
    }
}

void logQuarantinePublicationNoThrow(
    UUID database_uuid,
    UInt64 revision,
    UInt64 damaged_targets,
    UInt64 failing_seeds,
    UInt64 quarantined_objects,
    bool fail_closed) noexcept
{
    try
    {
        if (fail_closed)
        {
            LOG_ERROR(
                getLogger("UDTAuthorityVerificationRuntime"),
                "UDT authority integrity damage for database UUID {} published fail-closed runtime revision {} after "
                "quarantine construction failed (damaged targets: {}, failing seeds observed before failure: {})",
                database_uuid,
                revision,
                damaged_targets,
                failing_seeds);
        }
        else
        {
            LOG_WARNING(
                getLogger("UDTAuthorityVerificationRuntime"),
                "UDT authority integrity damage for database UUID {} published quarantine runtime revision {} "
                "(damaged targets: {}, failing seeds: {}, quarantined objects: {})",
                database_uuid,
                revision,
                damaged_targets,
                failing_seeds,
                quarantined_objects);
        }
    }
    catch (...)
    {
    }
}

void logQuarantineAuditPublicationNoThrow(UUID database_uuid, UInt64 revision, UInt64 quarantined_objects) noexcept
{
    try
    {
        LOG_WARNING(
            getLogger("UDTAuthorityVerificationRuntime"),
            "Complete UDT repair audit for database UUID {} published exact-root quarantine runtime revision {} "
            "({} quarantined objects)",
            database_uuid,
            revision,
            quarantined_objects);
    }
    catch (...)
    {
    }
}

void logQuarantineReleaseNoThrow(UUID database_uuid, UInt64 revision, UInt64 released_objects) noexcept
{
    try
    {
        LOG_INFO(
            getLogger("UDTAuthorityVerificationRuntime"),
            "Complete exact-root UDT verification for database UUID {} released quarantine at runtime revision {} "
            "({} released objects)",
            database_uuid,
            revision,
            released_objects);
    }
    catch (...)
    {
    }
}

class ThreadOperationCommitFenceState final
{
public:
    ~ThreadOperationCommitFenceState()
    {
        /// A live entry is backed by a commit guard which owns the database and
        /// therefore the runtime. Moving that guard to another thread would
        /// strand the counted fence, so fail fast instead of leaving a writer
        /// blocked forever.
        if (runtime || depth != 0)
            std::terminate();
    }

    const AuthorityVerificationRuntimeState * runtime = nullptr;
    UInt64 depth = 0;
};

ThreadOperationCommitFenceState & threadOperationCommitFenceState()
{
    /// Function-local TLS is initialized only at a mapped commit boundary and
    /// contains no dynamic storage.
    thread_local ThreadOperationCommitFenceState state;
    return state;
}

[[noreturn]] void fail(RuntimeError::Code code, std::string_view message)
{
    throw RuntimeError(code, message);
}

bool isZeroDigest(const Digest & digest) noexcept
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
}

bool isKnownInventoryRecordKind(AuthorityInventoryRecordKind kind) noexcept
{
    switch (kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition:
        case AuthorityInventoryRecordKind::SidecarExpectation: return true;
    }
    return false;
}

bool isValidCursor(const AuthorityVerificationScheduleCursor & cursor, UUID database_uuid) noexcept
{
    if (cursor.contract_abi != authority_verification_schedule_contract_abi || cursor.database_uuid != database_uuid
        || cursor.database_uuid == UUIDHelpers::Nil || cursor.bucket_count == 0
        || cursor.bucket_count > AuthorityVerificationScheduleLimits{}.maximum_buckets || cursor.current_bucket >= cursor.bucket_count)
        return false;
    if (!cursor.resume_after)
        return true;
    return cursor.resume_after->format_version == authority_inventory_format_version
        && isKnownInventoryRecordKind(cursor.resume_after->record_kind) && cursor.resume_after->object_uuid != UUIDHelpers::Nil;
}

void validateLimits(const AuthorityVerificationRuntimeStateLimits & limits)
{
    constexpr AuthorityQuarantinePlanLimits quarantine_maxima;
    const auto valid_quarantine_limit = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };
    if (limits.hazard_slot_count == 0 || limits.hazard_slot_count > maximum_supported_hazard_slots
        || limits.maximum_retired_snapshot_count > maximum_supported_retired_snapshots
        || limits.maximum_retired_snapshot_count < limits.hazard_slot_count + 1
        || !std::in_range<std::size_t>(limits.maximum_retired_snapshot_count)
        || !valid_quarantine_limit(limits.quarantine.maximum_seed_objects, quarantine_maxima.maximum_seed_objects)
        || !valid_quarantine_limit(limits.quarantine.maximum_closure_objects, quarantine_maxima.maximum_closure_objects)
        || !valid_quarantine_limit(limits.quarantine.maximum_reverse_edges_per_object, quarantine_maxima.maximum_reverse_edges_per_object)
        || !valid_quarantine_limit(limits.quarantine.maximum_walked_edges, quarantine_maxima.maximum_walked_edges)
        || !valid_quarantine_limit(limits.quarantine.maximum_work_units, quarantine_maxima.maximum_work_units)
        || !valid_quarantine_limit(limits.quarantine.maximum_retained_canonical_bytes, quarantine_maxima.maximum_retained_canonical_bytes)
        || limits.quarantine.maximum_seed_objects > limits.quarantine.maximum_closure_objects
        || limits.quarantine.maximum_reverse_edges_per_object > limits.quarantine.maximum_walked_edges)
    {
        fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime limits are invalid");
    }
}

AuthorityRootGraphIdentity identifyRoot(const AuthorityRoot & root)
{
    const auto & state = root.getAuthorityState();
    const auto inventory = root.pinAuthorityInventory();
    const auto graph = root.pinSchemaObjectDependencyGraph();
    if (state.database_uuid == UUIDHelpers::Nil || state.database_catalog_epoch == 0 || isZeroDigest(state.anchor_hash) || !inventory
        || !graph || root.getDatabaseUUID() != state.database_uuid || inventory->getSummary() != root.getInventorySummary()
        || inventory->getSummary().leaf_count != state.leaf_count || inventory->getSummary().merkle_radix_root != state.inventory_root
        || graph->getDatabaseUUID() != state.database_uuid || graph->computeRoot() != state.schema_graph_root
        || isZeroDigest(state.schema_graph_root))
    {
        fail(RuntimeError::Code::InvalidRoot, "authority verification runtime received an invalid exact root");
    }
    return {
        .authority_root = {
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
        },
        .schema_graph_root = state.schema_graph_root,
    };
}

SchemaObjectID resolveDamagedSeed(const AuthorityRoot & root, const AuthorityInventoryLeaf & damaged_leaf)
{
    const auto inventory = root.pinAuthorityInventory();
    const auto * rooted_leaf = inventory ? inventory->find(damaged_leaf.key) : nullptr;
    if (!rooted_leaf || *rooted_leaf != damaged_leaf)
        fail(RuntimeError::Code::InvalidRoot, "damaged verification target is absent from the exact root inventory");

    switch (damaged_leaf.key.record_kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition: {
            const auto * record = root.findDefinitionRecord(damaged_leaf.key.object_uuid);
            if (!record || record->identity.database_uuid != root.getDatabaseUUID()
                || record->identity.type_uuid != damaged_leaf.key.object_uuid || record->identity.revision != damaged_leaf.object_revision
                || computeRecordHash(*record) != damaged_leaf.canonical_record_hash)
            {
                fail(RuntimeError::Code::InvalidRoot, "damaged definition target does not resolve to its exact rooted record");
            }
            return {
                .kind = SchemaObjectKind::TypeDefinition,
                .database_uuid = record->identity.database_uuid,
                .object_uuid = record->identity.type_uuid,
            };
        }
        case AuthorityInventoryRecordKind::SidecarExpectation: {
            /// The inventory key intentionally omits object kind. This helper
            /// probes the complete closed V1 kind registry and rejects an
            /// ambiguous/corrupt radix store instead of guessing a kind.
            const auto * expectation = root.findExpectationRecord(damaged_leaf.key.object_uuid);
            if (!expectation || expectation->object.database_uuid != root.getDatabaseUUID()
                || expectation->object.object_uuid != damaged_leaf.key.object_uuid
                || expectation->object_schema_revision != damaged_leaf.object_revision
                || computeSidecarExpectationRecordHash(*expectation) != damaged_leaf.canonical_record_hash)
            {
                fail(RuntimeError::Code::InvalidRoot, "damaged sidecar target does not resolve to its exact rooted expectation");
            }
            return expectation->object;
        }
    }
    fail(RuntimeError::Code::InvalidRoot, "damaged verification target has an unknown inventory record kind");
}

const AuthorityInventoryLeaf * findInventoryLeafForObject(const AuthorityRoot & root, const SchemaObjectID & object) noexcept
{
    const auto inventory = root.pinAuthorityInventory();
    if (!inventory)
        return nullptr;
    const AuthorityInventoryKey key{
        .record_kind = object.kind == SchemaObjectKind::TypeDefinition ? AuthorityInventoryRecordKind::TypeDefinition
                                                                       : AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = object.object_uuid,
    };
    return inventory->find(key);
}

bool sameNeighbors(std::span<const SchemaObjectDependencyNeighbor> lhs, std::span<const SchemaObjectDependencyNeighbor> rhs) noexcept
{
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

}

struct AuthorityVerificationRuntimeState::PublishedState final
{
    UInt64 revision = 0;
    AuthorityVerificationScheduleCursor cursor;
    AuthorityQuarantinePlan::Ptr quarantine;
    bool fail_closed = false;
    AuthorityVerificationRuntimeLastErrorKind last_error = AuthorityVerificationRuntimeLastErrorKind::None;
};

class AuthorityVerificationRuntimeState::Impl final
{
public:
    class ExclusiveOperationPublicationFence final
    {
    public:
        explicit ExclusiveOperationPublicationFence(Impl & owner_)
            : owner(owner_)
        {
            owner.acquireExclusiveOperationPublicationFence();
        }

        ExclusiveOperationPublicationFence(const ExclusiveOperationPublicationFence &) = delete;
        ExclusiveOperationPublicationFence & operator=(const ExclusiveOperationPublicationFence &) = delete;
        ~ExclusiveOperationPublicationFence() { owner.releaseExclusiveOperationPublicationFence(); }

    private:
        Impl & owner;
    };

    class HazardDomain final
    {
    public:
        explicit HazardDomain(std::size_t slot_count_)
            : slot_count(slot_count_)
            , slots(std::make_unique<Slot[]>(slot_count_))
        {
            for (std::size_t index = 0; index < slot_count; ++index)
            {
                const UInt32 next = index + 1 == slot_count ? 0 : static_cast<UInt32>(index + 2);
                slots[index].next_free.store(next, std::memory_order_relaxed);
            }
            free_head.store(slot_count == 0 ? 0 : 1, std::memory_order_relaxed);
        }

        std::optional<std::size_t> tryClaim() noexcept
        {
            if (!enterClaim())
                return std::nullopt;
            UInt64 observed = free_head.load(std::memory_order_acquire);
            for (;;)
            {
                const UInt32 code = static_cast<UInt32>(observed & free_index_mask);
                if (code == 0)
                {
                    leaveClaim();
                    return std::nullopt;
                }
                const std::size_t index = static_cast<std::size_t>(code - 1);
                const UInt32 next = slots[index].next_free.load(std::memory_order_relaxed);
                const UInt64 replacement = advanceFreeTag(observed, next);
                if (free_head.compare_exchange_weak(observed, replacement, std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    active_count.fetch_add(1, std::memory_order_relaxed);
                    leaveClaim();
                    return index;
                }
            }
        }

        void publish(std::size_t slot, const PublishedState * state) noexcept
        {
            slots[slot].hazard.store(state, std::memory_order_seq_cst);
        }

        void clear(std::size_t slot) noexcept { slots[slot].hazard.store(nullptr, std::memory_order_seq_cst); }

        void release(std::size_t slot) noexcept
        {
            clear(slot);
            UInt64 observed = free_head.load(std::memory_order_relaxed);
            for (;;)
            {
                slots[slot].next_free.store(static_cast<UInt32>(observed & free_index_mask), std::memory_order_relaxed);
                const UInt64 replacement = advanceFreeTag(observed, static_cast<UInt32>(slot + 1));
                if (free_head.compare_exchange_weak(observed, replacement, std::memory_order_release, std::memory_order_relaxed))
                    break;
            }
            active_count.fetch_sub(1, std::memory_order_release);
        }

        bool isHazarded(const PublishedState * state) const noexcept
        {
            for (std::size_t index = 0; index < slot_count; ++index)
            {
                if (slots[index].hazard.load(std::memory_order_seq_cst) == state)
                    return true;
            }
            return false;
        }

        UInt64 activeSlots() const noexcept { return active_count.load(std::memory_order_acquire); }

        void close() noexcept
        {
            claim_gate.fetch_or(closed_mask, std::memory_order_acq_rel);
            while ((claim_gate.load(std::memory_order_acquire) & claim_count_mask) != 0)
                std::this_thread::yield();
        }

        bool isAccepting() const noexcept { return (claim_gate.load(std::memory_order_acquire) & closed_mask) == 0; }

    private:
        struct alignas(CH_CACHE_LINE_SIZE) Slot final
        {
            std::atomic<const PublishedState *> hazard{nullptr};
            std::atomic<UInt32> next_free{0};
        };

        static constexpr UInt64 free_index_bits = 17;
        static constexpr UInt64 free_index_mask = (1ULL << free_index_bits) - 1;
        static constexpr UInt64 free_tag_increment = 1ULL << free_index_bits;
        static constexpr UInt64 closed_mask = 1ULL << 63;
        static constexpr UInt64 claim_count_mask = ~closed_mask;

        static UInt64 advanceFreeTag(UInt64 observed, UInt32 next) noexcept
        {
            return ((observed + free_tag_increment) & ~free_index_mask) | next;
        }

        bool enterClaim() noexcept
        {
            UInt64 observed = claim_gate.load(std::memory_order_acquire);
            for (;;)
            {
                if ((observed & closed_mask) != 0)
                    return false;
                if ((observed & claim_count_mask) == claim_count_mask)
                    std::terminate();
                if (claim_gate.compare_exchange_weak(observed, observed + 1, std::memory_order_acq_rel, std::memory_order_acquire))
                    return true;
            }
        }

        void leaveClaim() noexcept { claim_gate.fetch_sub(1, std::memory_order_release); }

        const std::size_t slot_count;
        std::unique_ptr<Slot[]> slots;
        alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt64> free_head{0};
        alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt64> active_count{0};
        alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt64> claim_gate{0};
    };

    Impl(std::unique_ptr<const PublishedState> initial_state, const AuthorityVerificationRuntimeStateLimits & limits_)
        : limits(limits_)
        , hazard_domain(std::make_unique<HazardDomain>(limits.hazard_slot_count))
        , current_owner(std::move(initial_state))
    {
        retired.reserve(static_cast<std::size_t>(limits.maximum_retired_snapshot_count));
        minimum_safe_admission_revision.store(current_owner->revision, std::memory_order_relaxed);
        current.store(current_owner.get(), std::memory_order_release);
    }

    Snapshot acquireSnapshot()
    {
        auto * domain = hazard_domain.get();
        const auto claimed = domain->tryClaim();
        if (!claimed)
        {
            if (!domain->isAccepting())
                fail(RuntimeError::Code::Shutdown, "authority verification runtime is shut down");
            fail(RuntimeError::Code::HazardSlotsExhausted, "authority verification runtime hazard slots are exhausted");
        }

        const std::size_t slot = *claimed;
        for (;;)
        {
            const PublishedState * observed = current.load(std::memory_order_acquire);
            if (!observed)
            {
                domain->release(slot);
                fail(RuntimeError::Code::Shutdown, "authority verification runtime is shut down");
            }
            domain->publish(slot, observed);
            if (observed == current.load(std::memory_order_acquire))
                return Snapshot(this, slot, observed);
            domain->clear(slot);
        }
    }

    void release(std::size_t slot) noexcept { hazard_domain->release(slot); }

    bool isEmergencyFailClosed() const noexcept { return emergency_fail_closed.load(std::memory_order_acquire); }

    bool isSnapshotAdmissionFailClosed(const PublishedState * state) const noexcept
    {
        if (!state || shutdown.load(std::memory_order_acquire) || isEmergencyFailClosed())
            return true;
        return state->revision < minimum_safe_admission_revision.load(std::memory_order_acquire);
    }

    void acquireOperationCommitFence()
    {
        std::unique_lock lock(operation_publication_mutex);
        operation_publication_cv.wait(lock, [this] { return waiting_operation_publications == 0 && !operation_publication_active; });
        if (shutdown.load(std::memory_order_acquire) || !current.load(std::memory_order_acquire))
            fail(RuntimeError::Code::Shutdown, "authority verification runtime is shut down");
        if (active_operation_commits == std::numeric_limits<UInt64>::max())
            fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime commit-fence domain is exhausted");
        ++active_operation_commits;
    }

    void releaseOperationCommitFence() noexcept
    {
        std::lock_guard lock(operation_publication_mutex);
        if (active_operation_commits == 0)
            std::terminate();
        --active_operation_commits;
        if (active_operation_commits == 0)
            operation_publication_cv.notify_all();
    }

    void acquireExclusiveOperationPublicationFence()
    {
        std::unique_lock lock(operation_publication_mutex);
        if (waiting_operation_publications == std::numeric_limits<UInt64>::max())
            fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime publication-fence domain is exhausted");
        ++waiting_operation_publications;
        try
        {
            if (FailPointInjection::hasAnyFailPointBeenRegistered())
            {
                FailPointInjection::pauseFailPoint(
                    FailPoints::udt_authority_runtime_pause_after_publication_waiter_registration);
            }
            operation_publication_cv.wait(lock, [this] { return active_operation_commits == 0 && !operation_publication_active; });
        }
        catch (...)
        {
            --waiting_operation_publications;
            operation_publication_cv.notify_all();
            throw;
        }
        --waiting_operation_publications;
        operation_publication_active = true;
    }

    void releaseExclusiveOperationPublicationFence() noexcept
    {
        std::lock_guard lock(operation_publication_mutex);
        if (!operation_publication_active)
            std::terminate();
        operation_publication_active = false;
        operation_publication_cv.notify_all();
    }

    void publishUnlocked(std::unique_ptr<const PublishedState> replacement) noexcept
    {
        if (!replacement || shutdown.load(std::memory_order_acquire) || !current_owner
            || retired.size() >= limits.maximum_retired_snapshot_count || retired.size() == retired.capacity())
            std::terminate();
        const PublishedState * previous = current.exchange(replacement.get(), std::memory_order_acq_rel);
        if (previous != current_owner.get())
            std::terminate();
        retired.push_back(std::move(current_owner));
        current_owner = std::move(replacement);
    }

    void scanRetired()
    {
        for (;;)
        {
            std::unique_ptr<const PublishedState> reclaim;
            {
                std::lock_guard lock(writer_mutex);
                for (std::size_t index = 0; index < retired.size(); ++index)
                {
                    if (hazard_domain->isHazarded(retired[index].get()))
                        continue;
                    reclaim = std::move(retired[index]);
                    if (index + 1 != retired.size())
                        retired[index] = std::move(retired.back());
                    retired.pop_back();
                    break;
                }
                if (!reclaim && shutdown.load(std::memory_order_acquire) && current_owner
                    && !hazard_domain->isHazarded(current_owner.get()))
                {
                    reclaim = std::move(current_owner);
                }
            }
            if (!reclaim)
                return;
            reclaim.reset();
        }
    }

    void beginShutdown() noexcept
    {
        std::lock_guard lock(writer_mutex);
        if (shutdown.exchange(true, std::memory_order_acq_rel))
            return;
        hazard_domain->close();
        const PublishedState * previous = current.exchange(nullptr, std::memory_order_acq_rel);
        if (previous != current_owner.get())
            std::terminate();
    }

    void shutdownAndDrain() noexcept
    {
        beginShutdown();
        for (;;)
        {
            scanRetired();
            std::lock_guard lock(writer_mutex);
            if (retired.empty() && !current_owner && hazard_domain->activeSlots() == 0)
                return;
            std::this_thread::yield();
        }
    }

    const AuthorityVerificationRuntimeStateLimits limits;
    std::mutex operation_publication_mutex;
    std::condition_variable operation_publication_cv;
    UInt64 active_operation_commits = 0;
    UInt64 waiting_operation_publications = 0;
    bool operation_publication_active = false;
    std::unique_ptr<HazardDomain> hazard_domain;
    std::atomic<const PublishedState *> current{nullptr};
    /// Allocation-free safety net for the interval between accepting a
    /// damaged receipt and publishing its quarantine snapshot. It is raised
    /// while the exclusive final-operation fence is held, before any
    /// quarantine/fail-closed allocation, and cleared only after a stricter
    /// immutable state has become visible.
    std::atomic<bool> emergency_fail_closed{false};
    /// A hazard-protected snapshot may outlive the emergency latch interval.
    /// Advancing this floor before a stricter admission state is built keeps
    /// every older permissive snapshot fail closed even after the latch can be
    /// cleared. Cursor-only publications deliberately do not advance it.
    std::atomic<UInt64> minimum_safe_admission_revision{0};
    std::atomic<bool> shutdown{false};
    std::mutex writer_mutex;
    std::unique_ptr<const PublishedState> current_owner;
    std::vector<std::unique_ptr<const PublishedState>> retired;
};

class AuthorityVerificationRuntimeState::PreparedAuthorityPublication final : public IAtomicAuthorityPublicationObserver::PreparedTransition
{
public:
    PreparedAuthorityPublication(
        Impl & owner_,
        std::unique_ptr<Impl::ExclusiveOperationPublicationFence> publication_fence_,
        const PublishedState * expected_state_,
        UInt64 expected_revision_,
        std::unique_ptr<const PublishedState> replacement_) noexcept
        : owner(owner_)
        , publication_fence(std::move(publication_fence_))
        , expected_state(expected_state_)
        , expected_revision(expected_revision_)
        , replacement(std::move(replacement_))
    {
    }

    void publish() noexcept override
    {
        {
            std::lock_guard lock(owner.writer_mutex);
            if (!publication_fence || !replacement || owner.shutdown.load(std::memory_order_acquire)
                || owner.current_owner.get() != expected_state || expected_state->revision != expected_revision)
            {
                std::terminate();
            }
            owner.publishUnlocked(std::move(replacement));
        }
        /// Admit new final operations immediately after both immutable values
        /// are visible; the caller may retain its consumed publication object.
        publication_fence.reset();
    }

private:
    Impl & owner;
    std::unique_ptr<Impl::ExclusiveOperationPublicationFence> publication_fence;
    const PublishedState * expected_state;
    UInt64 expected_revision;
    std::unique_ptr<const PublishedState> replacement;
};

AuthorityVerificationRuntimeStateError::AuthorityVerificationRuntimeStateError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityVerificationRuntimeState::Snapshot::Snapshot(Impl * owner_, std::size_t hazard_slot_, const PublishedState * state_) noexcept
    : owner(owner_)
    , hazard_slot(hazard_slot_)
    , state(state_)
{
}

AuthorityVerificationRuntimeState::Snapshot::Snapshot(Snapshot && other) noexcept
    : owner(std::exchange(other.owner, nullptr))
    , hazard_slot(other.hazard_slot)
    , state(std::exchange(other.state, nullptr))
{
}

AuthorityVerificationRuntimeState::Snapshot::~Snapshot()
{
    if (state)
        owner->release(hazard_slot);
}

const AuthorityVerificationScheduleCursor & AuthorityVerificationRuntimeState::Snapshot::getCursor() const
{
    if (!state)
        std::terminate();
    return state->cursor;
}

const AuthorityQuarantinePlan::Ptr & AuthorityVerificationRuntimeState::Snapshot::getQuarantine() const
{
    if (!state)
        std::terminate();
    return state->quarantine;
}

UInt64 AuthorityVerificationRuntimeState::Snapshot::getRevision() const noexcept
{
    return state ? state->revision : 0;
}

bool AuthorityVerificationRuntimeState::Snapshot::isFailClosed() const noexcept
{
    return !owner || owner->isSnapshotAdmissionFailClosed(state) || state->fail_closed;
}

AuthorityVerificationRuntimeLastErrorKind AuthorityVerificationRuntimeState::Snapshot::getLastErrorKind() const noexcept
{
    return state && owner && !owner->isSnapshotAdmissionFailClosed(state)
        ? state->last_error
        : AuthorityVerificationRuntimeLastErrorKind::QuarantineConstructionFailed;
}

AuthorityVerificationRuntimeState::AuthorityVerificationRuntimeState(
    UUID database_uuid_, AuthorityVerificationScheduleCursor initial_cursor, const AuthorityVerificationRuntimeStateLimits & limits)
    : database_uuid(database_uuid_)
{
    if (database_uuid == UUIDHelpers::Nil)
        fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime database UUID is nil");
    validateLimits(limits);
    if (!isValidCursor(initial_cursor, database_uuid))
        fail(RuntimeError::Code::InvalidCursor, "authority verification runtime initial cursor is invalid");
    auto initial = std::make_unique<PublishedState>();
    initial->revision = 1;
    initial->cursor = std::move(initial_cursor);
    impl = std::make_unique<Impl>(std::move(initial), limits);
}

AuthorityVerificationRuntimeState::~AuthorityVerificationRuntimeState()
{
    shutdownAndDrain();
}

AuthorityVerificationRuntimeState::Snapshot AuthorityVerificationRuntimeState::acquireSnapshot() const
{
    return impl->acquireSnapshot();
}

AuthorityVerificationScheduleCursor AuthorityVerificationRuntimeState::getCursor() const
{
    auto snapshot = acquireSnapshot();
    return snapshot.getCursor();
}

void AuthorityVerificationRuntimeState::acquireNewOperationCommitFence() const
{
    auto & thread_fence = threadOperationCommitFenceState();
    if (thread_fence.runtime)
    {
        if (thread_fence.runtime != this)
        {
            fail(RuntimeError::Code::InvalidConfiguration, "cross-runtime nested authority commit fences are not supported");
        }
        if (thread_fence.depth == 0 || thread_fence.depth == maximum_thread_operation_commit_fence_depth)
            fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime reentrant commit-fence depth is exhausted");
        ++thread_fence.depth;
        return;
    }

    /// The blocking operation happens before publishing the TLS owner, so an
    /// exception leaves no per-thread state and no global counter to unwind.
    impl->acquireOperationCommitFence();
    thread_fence.runtime = this;
    thread_fence.depth = 1;
}

void AuthorityVerificationRuntimeState::releaseNewOperationCommitFence() const noexcept
{
    auto & thread_fence = threadOperationCommitFenceState();
    if (thread_fence.runtime != this || thread_fence.depth == 0)
        std::terminate();

    --thread_fence.depth;
    if (thread_fence.depth != 0)
        return;

    /// Remove the TLS key before the owning guard can release its database.
    /// Consequently a destroyed runtime can never leave an address which a
    /// later runtime instance could mistake for a reentrant acquisition.
    thread_fence.runtime = nullptr;
    impl->releaseOperationCommitFence();
}

std::unique_ptr<IAtomicAuthorityPublicationObserver::PreparedTransition>
AuthorityVerificationRuntimeState::prepareAuthorityPublication(const AuthorityRoot & before, const AuthorityRoot & after)
{
    if (before.getDatabaseUUID() != database_uuid || after.getDatabaseUUID() != database_uuid)
        fail(RuntimeError::Code::InvalidRoot, "authority quarantine re-anchor received a foreign root");

    auto publication_fence = std::make_unique<Impl::ExclusiveOperationPublicationFence>(*impl);
    impl->scanRetired();
    std::lock_guard lock(impl->writer_mutex);
    if (impl->shutdown.load(std::memory_order_acquire) || !impl->current_owner)
        fail(RuntimeError::Code::Shutdown, "authority verification runtime is shut down");
    const auto & current = *impl->current_owner;
    if (current.fail_closed)
        fail(RuntimeError::Code::InvalidRoot, "authority root cannot change while verification admission is fail closed");
    if (!current.quarantine)
        return {};

    const AuthorityRootGraphIdentity before_identity = identifyRoot(before);
    if (current.quarantine->getRoot() != before_identity)
        fail(RuntimeError::Code::InvalidRoot, "published quarantine is not anchored to the authority mutation predecessor");
    static_cast<void>(identifyRoot(after));

    if (current.revision == std::numeric_limits<UInt64>::max())
        fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime revision domain is exhausted");
    if (impl->retired.size() >= impl->limits.maximum_retired_snapshot_count || impl->retired.size() == impl->retired.capacity())
        fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime retirement capacity is exhausted");

    const auto before_graph = before.pinSchemaObjectDependencyGraph();
    const auto after_graph = after.pinSchemaObjectDependencyGraph();
    if (!before_graph || !after_graph)
        fail(RuntimeError::Code::InvalidRoot, "authority quarantine re-anchor cannot pin both schema graphs");

    for (const auto & object : current.quarantine->getQuarantinedObjects())
    {
        if (!object.isValid() || object.database_uuid != database_uuid || !before_graph->containsNode(object)
            || !after_graph->containsNode(object))
        {
            fail(RuntimeError::Code::InvalidRoot, "authority mutation changed a quarantined schema object");
        }
        const auto * before_leaf = findInventoryLeafForObject(before, object);
        const auto * after_leaf = findInventoryLeafForObject(after, object);
        if (!before_leaf || !after_leaf || *before_leaf != *after_leaf
            || !sameNeighbors(before_graph->getDependencies(object), after_graph->getDependencies(object))
            || !sameNeighbors(before_graph->getDependents(object), after_graph->getDependents(object)))
        {
            fail(RuntimeError::Code::InvalidRoot, "authority mutation changed a quarantined image or its dependency boundary");
        }
    }

    auto reanchored = AuthorityQuarantinePlan::build(after, current.quarantine->getFailingSeeds(), impl->limits.quarantine);
    const auto old_closure = current.quarantine->getQuarantinedObjects();
    const auto new_closure = reanchored->getQuarantinedObjects();
    if (old_closure.size() != new_closure.size() || !std::equal(old_closure.begin(), old_closure.end(), new_closure.begin()))
        fail(RuntimeError::Code::InvalidRoot, "authority mutation changed the quarantined reverse-dependency closure");

    auto replacement = std::make_unique<PublishedState>();
    replacement->revision = current.revision + 1;
    replacement->cursor = current.cursor;
    replacement->quarantine = std::move(reanchored);
    replacement->fail_closed = false;
    replacement->last_error = current.last_error;
    return std::make_unique<PreparedAuthorityPublication>(
        *impl, std::move(publication_fence), std::addressof(current), current.revision, std::move(replacement));
}

AuthorityVerificationRuntimeConsumeResult AuthorityVerificationRuntimeState::consume(
    const AuthorityRoot & exact_current_root,
    const AuthorityVerificationBatchPlan & plan,
    const AuthorityVerificationBatchReceipt & receipt,
    const std::function<void(const AuthorityVerificationScheduleCursor &)> & persist_advanced_cursor)
{
    const AuthorityRootGraphIdentity exact_root = identifyRoot(exact_current_root);
    if (exact_root.authority_root.database_uuid != database_uuid)
        fail(RuntimeError::Code::InvalidRoot, "authority verification runtime root belongs to another database");

    const Digest exact_target_set = computeAuthorityVerificationTargetSetDigest(exact_current_root.getInventorySummary());
    if (plan.getRoot() != exact_root.authority_root || plan.getTargetSetDigest() != exact_target_set
        || plan.getChargeABI() != authority_verification_charge_abi || receipt.getRoot() != exact_root.authority_root
        || receipt.getTargetSetDigest() != exact_target_set || receipt.getChargeABI() != authority_verification_charge_abi)
    {
        return {
            .status = ConsumeStatus::RetryRootChanged,
            .cursor_decision = {CursorStatus::RetryRootChanged, plan.getRetryCursor()},
        };
    }

    UInt64 damaged_target_count = 0;
    for (const auto & completion : receipt.getTerminalCompletions())
    {
        if (completion.disposition == AuthorityVerificationTargetDisposition::Damaged)
            ++damaged_target_count;
    }

    /// Advancing the periodic cursor does not change operation admission: it
    /// preserves both quarantine and fail-closed state.  Keep that durable
    /// cursor write off the final-operation fence, otherwise its fsync latency
    /// would unnecessarily stall every Memory/MergeTree commit.  A damaged
    /// receipt does change admission and therefore retains the exclusive fence
    /// through construction and publication of quarantine/fail-closed state.
    std::optional<Impl::ExclusiveOperationPublicationFence> operation_publication_fence;
    if (damaged_target_count != 0)
        operation_publication_fence.emplace(*impl);
    impl->scanRetired();
    std::unique_lock lock(impl->writer_mutex);
    if (impl->shutdown.load(std::memory_order_acquire) || !impl->current_owner)
        fail(RuntimeError::Code::Shutdown, "authority verification runtime is shut down");
    const auto & current = *impl->current_owner;
    if (current.cursor != plan.getRetryCursor())
    {
        return {
            .status = ConsumeStatus::RetryIncompleteRotation,
            .cursor_decision = {CursorStatus::RetryIncompleteRotation, current.cursor},
            .published_revision = current.revision,
        };
    }
    if (current.fail_closed)
    {
        return {
            .status = ConsumeStatus::RetryIncompleteRotation,
            .cursor_decision = {CursorStatus::RetryIncompleteRotation, current.cursor},
            .published_revision = current.revision,
        };
    }
    if (current.quarantine && current.quarantine->getRoot() != exact_root)
    {
        return {
            .status = ConsumeStatus::RetryRootChanged,
            .cursor_decision = {CursorStatus::RetryRootChanged, current.cursor},
            .published_revision = current.revision,
        };
    }

    if (damaged_target_count != 0)
    {
        /// No admitted final operation can cross this point: the exclusive
        /// fence has drained all existing commit guards and blocks new ones.
        /// Raise the preallocated latch before the first allocation so OOM or
        /// any other exception cannot restore the old permissive boundary.
        impl->emergency_fail_closed.store(true, std::memory_order_release);
        if (current.revision == std::numeric_limits<UInt64>::max())
            fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime revision domain is exhausted");
        impl->minimum_safe_admission_revision.store(current.revision + 1, std::memory_order_release);

        auto fail_closed = std::make_unique<PublishedState>();
        fail_closed->revision = current.revision + 1;
        fail_closed->cursor = current.cursor;
        fail_closed->quarantine = current.quarantine;
        fail_closed->fail_closed = true;
        fail_closed->last_error = AuthorityVerificationRuntimeLastErrorKind::QuarantineConstructionFailed;

        AuthorityQuarantinePlan::Ptr quarantine;
        std::vector<SchemaObjectID> seeds;
        bool construction_failed = false;
        try
        {
            const UInt64 existing_seed_count = current.quarantine ? static_cast<UInt64>(current.quarantine->getFailingSeeds().size()) : 0;
            if (existing_seed_count > std::numeric_limits<UInt64>::max() - damaged_target_count
                || existing_seed_count + damaged_target_count > impl->limits.quarantine.maximum_seed_objects
                || !std::in_range<std::size_t>(existing_seed_count + damaged_target_count))
            {
                fail(RuntimeError::Code::InvalidConfiguration, "authority verification damaged seed set exceeds its limit");
            }
            seeds.reserve(static_cast<std::size_t>(existing_seed_count + damaged_target_count));
            if (current.quarantine)
                seeds.insert(seeds.end(), current.quarantine->getFailingSeeds().begin(), current.quarantine->getFailingSeeds().end());
            for (const auto & completion : receipt.getTerminalCompletions())
            {
                if (completion.disposition == AuthorityVerificationTargetDisposition::Damaged)
                    seeds.push_back(resolveDamagedSeed(exact_current_root, completion.leaf));
            }
            std::sort(seeds.begin(), seeds.end());
            seeds.erase(std::unique(seeds.begin(), seeds.end()), seeds.end());
            quarantine = AuthorityQuarantinePlan::build(exact_current_root, seeds, impl->limits.quarantine);
        }
        catch (...)
        {
            construction_failed = true;
        }

        AuthorityVerificationRuntimeConsumeResult result{
            .status = construction_failed ? ConsumeStatus::DamagedFailClosed : ConsumeStatus::DamagedQuarantined,
            .cursor_decision = {CursorStatus::RetryIncompleteRotation, current.cursor},
            .damaged_target_count = damaged_target_count,
            .failing_seed_count = static_cast<UInt64>(seeds.size()),
            .quarantined_object_count = quarantine ? static_cast<UInt64>(quarantine->getQuarantinedObjects().size()) : 0,
            .published_revision = current.revision + 1,
        };

        if (construction_failed)
        {
            impl->publishUnlocked(std::move(fail_closed));
        }
        else
        {
            auto replacement = std::make_unique<PublishedState>();
            replacement->revision = current.revision + 1;
            replacement->cursor = current.cursor;
            replacement->quarantine = std::move(quarantine);
            replacement->last_error = AuthorityVerificationRuntimeLastErrorKind::IntegrityDamageQuarantined;
            impl->publishUnlocked(std::move(replacement));
        }
        impl->emergency_fail_closed.store(false, std::memory_order_release);
        lock.unlock();
        operation_publication_fence.reset();
        if (construction_failed)
        {
            incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityQuarantineFailClosedPublications);
        }
        else
        {
            incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityQuarantinePublications);
            incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityQuarantinedObjects, result.quarantined_object_count);
        }
        logQuarantinePublicationNoThrow(
            database_uuid,
            result.published_revision,
            result.damaged_target_count,
            result.failing_seed_count,
            result.quarantined_object_count,
            construction_failed);
        return result;
    }

    const auto cursor_decision = finalizePeriodicAuthorityVerificationBatch(plan, receipt);
    if (cursor_decision.status == CursorStatus::RetryRootChanged)
    {
        return {
            .status = ConsumeStatus::RetryRootChanged,
            .cursor_decision = {CursorStatus::RetryRootChanged, current.cursor},
            .published_revision = current.revision,
        };
    }
    if (cursor_decision.status == CursorStatus::RetryIncompleteRotation)
    {
        return {
            .status = ConsumeStatus::RetryIncompleteRotation,
            .cursor_decision = {CursorStatus::RetryIncompleteRotation, current.cursor},
            .published_revision = current.revision,
        };
    }
    if (cursor_decision.status == CursorStatus::NoWork)
    {
        return {
            .status = ConsumeStatus::NoWork,
            .cursor_decision = cursor_decision,
            .published_revision = current.revision,
        };
    }

    if (current.revision == std::numeric_limits<UInt64>::max())
        fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime revision domain is exhausted");
    auto replacement = std::make_unique<PublishedState>();
    replacement->revision = current.revision + 1;
    replacement->cursor = cursor_decision.cursor;
    replacement->quarantine = current.quarantine;
    replacement->last_error = current.last_error;
    AuthorityVerificationRuntimeConsumeResult result{
        .status = ConsumeStatus::CursorAdvanced,
        .cursor_decision = cursor_decision,
        .published_revision = replacement->revision,
    };
    /// Cursor progress is operational state rather than authority. Persist it
    /// while the exact-root/schema serialization is still held and before the
    /// lock-free runtime snapshot becomes visible. A persistence failure leaves
    /// the retry cursor published in-process; a crash after persistence merely
    /// resumes after an already-complete clean receipt.
    if (persist_advanced_cursor)
        persist_advanced_cursor(cursor_decision.cursor);
    impl->publishUnlocked(std::move(replacement));
    return result;
}

AuthorityQuarantineAdmissionDecision AuthorityVerificationRuntimeState::decideOperation(
    const AuthorityQuarantineOperationView & operation, const AuthorityQuarantineAdmissionLimits & limits) const noexcept
{
    try
    {
        if (impl->isEmergencyFailClosed())
            return {
                .status = AuthorityQuarantineAdmissionStatus::RuntimeFailClosed,
                .statistics = {},
            };
        auto snapshot = acquireSnapshot();
        if (snapshot.isFailClosed())
            return {
                .status = AuthorityQuarantineAdmissionStatus::RuntimeFailClosed,
                .statistics = {},
            };
        const auto & quarantine = snapshot.getQuarantine();
        if (!quarantine)
            return {
                .status = AuthorityQuarantineAdmissionStatus::AllowedUnaffected,
                .statistics = {},
            };
        return decideAuthorityQuarantineAdmission(*quarantine, operation, limits);
    }
    catch (...)
    {
        return {
            .status = AuthorityQuarantineAdmissionStatus::RuntimeFailClosed,
            .statistics = {},
        };
    }
}

void AuthorityVerificationRuntimeState::publishAutomaticRepairAuditQuarantine(
    const AuthorityRoot & exact_audited_root, const AuthorityQuarantinePlan::Ptr & audited_quarantine)
{
    if (!audited_quarantine)
        fail(RuntimeError::Code::InvalidRoot, "automatic repair audit has no quarantine to publish");
    const AuthorityRootGraphIdentity audited_root = identifyRoot(exact_audited_root);
    if (audited_root.authority_root.database_uuid != database_uuid || audited_quarantine->getRoot() != audited_root)
        fail(RuntimeError::Code::InvalidRoot, "automatic repair audit quarantine is not bound to its exact root");

    std::optional<Impl::ExclusiveOperationPublicationFence> operation_publication_fence;
    operation_publication_fence.emplace(*impl);
    impl->scanRetired();
    std::unique_lock lock(impl->writer_mutex);
    if (impl->shutdown.load(std::memory_order_acquire) || !impl->current_owner)
        fail(RuntimeError::Code::Shutdown, "authority verification runtime is shut down");
    const auto & current = *impl->current_owner;
    if (current.fail_closed || !current.quarantine)
        fail(RuntimeError::Code::InvalidRoot, "automatic repair audit cannot replace absent or fail-closed quarantine state");
    for (const auto & object : current.quarantine->getQuarantinedObjects())
    {
        if (!audited_quarantine->contains(object))
            fail(RuntimeError::Code::InvalidRoot, "automatic repair audit would release a previously quarantined object");
    }
    /// The audited closure may expand the currently published closure. Raise
    /// the allocation-free latch and invalidate every older admission snapshot
    /// before constructing the replacement so failure cannot reopen an object
    /// that the complete audit has classified as damaged.
    impl->emergency_fail_closed.store(true, std::memory_order_release);
    if (current.revision == std::numeric_limits<UInt64>::max())
        fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime revision domain is exhausted");
    impl->minimum_safe_admission_revision.store(current.revision + 1, std::memory_order_release);

    auto replacement = std::make_unique<PublishedState>();
    replacement->revision = current.revision + 1;
    replacement->cursor = current.cursor;
    replacement->quarantine = audited_quarantine;
    replacement->fail_closed = false;
    replacement->last_error = AuthorityVerificationRuntimeLastErrorKind::IntegrityDamageQuarantined;
    impl->publishUnlocked(std::move(replacement));
    impl->emergency_fail_closed.store(false, std::memory_order_release);
    const UInt64 quarantined_objects = static_cast<UInt64>(audited_quarantine->getQuarantinedObjects().size());
    const UInt64 publication_revision = current.revision + 1;
    lock.unlock();
    operation_publication_fence.reset();
    incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityQuarantinePublications);
    incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityQuarantinedObjects, quarantined_objects);
    logQuarantineAuditPublicationNoThrow(database_uuid, publication_revision, quarantined_objects);
}

void AuthorityVerificationRuntimeState::releaseQuarantineAfterCompleteVerification(
    const AuthorityRoot & exact_repaired_root, const AuthorityQuarantinePlan::Ptr & expected_quarantine)
{
    if (!expected_quarantine)
        fail(RuntimeError::Code::InvalidRoot, "authority complete verification has no quarantine to release");
    const AuthorityRootGraphIdentity repaired_root = identifyRoot(exact_repaired_root);
    const auto & quarantined_root = expected_quarantine->getRoot();
    if (repaired_root.authority_root.database_uuid != database_uuid || quarantined_root.authority_root.database_uuid != database_uuid
        || repaired_root.authority_root.database_catalog_epoch < quarantined_root.authority_root.database_catalog_epoch)
    {
        fail(RuntimeError::Code::InvalidRoot, "authority repair release root predates the quarantined closure");
    }

    const auto inventory = exact_repaired_root.pinAuthorityInventory();
    const auto graph = exact_repaired_root.pinSchemaObjectDependencyGraph();
    if (!inventory || !graph)
        fail(RuntimeError::Code::InvalidRoot, "authority quarantine release cannot pin its completely verified root");
    for (const auto & object : expected_quarantine->getQuarantinedObjects())
    {
        if (!graph->containsNode(object))
            fail(RuntimeError::Code::InvalidRoot, "authority quarantine release closure changed in the verified root");
        const AuthorityInventoryKey key{
            .record_kind = object.kind == SchemaObjectKind::TypeDefinition ? AuthorityInventoryRecordKind::TypeDefinition
                                                                           : AuthorityInventoryRecordKind::SidecarExpectation,
            .object_uuid = object.object_uuid,
        };
        if (!inventory->find(key))
            fail(RuntimeError::Code::InvalidRoot, "authority quarantine release closure has no fully verified inventory target");
    }

    std::optional<Impl::ExclusiveOperationPublicationFence> operation_publication_fence;
    operation_publication_fence.emplace(*impl);
    impl->scanRetired();
    std::unique_lock lock(impl->writer_mutex);
    if (impl->shutdown.load(std::memory_order_acquire) || !impl->current_owner)
        fail(RuntimeError::Code::Shutdown, "authority verification runtime is shut down");
    const auto & current = *impl->current_owner;
    if (current.fail_closed || current.quarantine != expected_quarantine)
        fail(RuntimeError::Code::InvalidRoot, "authority complete verification no longer matches the published quarantine");
    if (current.revision == std::numeric_limits<UInt64>::max())
        fail(RuntimeError::Code::InvalidConfiguration, "authority verification runtime revision domain is exhausted");

    auto replacement = std::make_unique<PublishedState>();
    replacement->revision = current.revision + 1;
    replacement->cursor = current.cursor;
    replacement->quarantine.reset();
    replacement->fail_closed = false;
    replacement->last_error = AuthorityVerificationRuntimeLastErrorKind::None;
    const UInt64 released_objects = static_cast<UInt64>(expected_quarantine->getQuarantinedObjects().size());
    const UInt64 release_revision = replacement->revision;
    impl->publishUnlocked(std::move(replacement));
    lock.unlock();
    operation_publication_fence.reset();
    incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityQuarantineReleases);
    incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityQuarantineReleasedObjects, released_objects);
    logQuarantineReleaseNoThrow(database_uuid, release_revision, released_objects);
}

void AuthorityVerificationRuntimeState::scanRetired()
{
    Impl::ExclusiveOperationPublicationFence operation_publication_fence(*impl);
    impl->scanRetired();
}

void AuthorityVerificationRuntimeState::shutdownAndDrain() noexcept
{
    Impl::ExclusiveOperationPublicationFence operation_publication_fence(*impl);
    impl->shutdownAndDrain();
}

bool AuthorityVerificationRuntimeState::isShutdown() const noexcept
{
    return impl->shutdown.load(std::memory_order_acquire);
}

}
