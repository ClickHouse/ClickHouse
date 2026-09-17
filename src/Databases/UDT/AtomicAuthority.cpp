#include <Databases/UDT/AtomicAuthority.h>

#include <DataTypes/UDT/AuthorityState.h>

#include <Common/CacheLine.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>

#include <atomic>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::FailPoints
{
extern const char udt_authority_prepared_publication_failure[];
}

namespace ProfileEvents
{
extern const Event UDTAuthorityRootPublications;
extern const Event UDTAuthorityPublishedDeterministicCatalogBytes;
}

namespace DB::UDT
{
namespace
{

using AuthorityError = AtomicAuthorityError;
using CompositeRoot = AtomicAuthority::CompositeRoot;
using CompositeRootPtr = AtomicAuthority::CompositeRootPtr;
using PublicationLimits = AtomicAuthorityPublicationLimits;
using RetirementState = AtomicAuthorityRetirementState;

constexpr TypeAuthorityCapabilityMask required_capabilities = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
    | typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits)
    | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);

constexpr TypeAuthorityCapabilityMask allowed_capabilities
    = required_capabilities | typeAuthorityCapabilityBit(TypeAuthorityCapability::DecreasingRecursion);

constexpr std::size_t maximum_supported_hazard_slots = 65'536;
constexpr UInt64 maximum_supported_retired_roots = 1ULL << 20;

[[noreturn]] void fail(AuthorityError::Code code, std::string_view message)
{
    throw AuthorityError(code, message);
}

UInt64 toUInt64(std::size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(AuthorityError::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(AuthorityError::Code::LimitExceeded, message);
    return lhs + rhs;
}

void validateCapabilities(const TypeAuthorityCapabilities & capabilities)
{
    if (capabilities.adapter_abi != 1)
        fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority adapter ABI is unsupported");
    if ((capabilities.mask & ~allowed_capabilities) != 0 || !capabilities.containsAll(required_capabilities))
        fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority capability set is unsupported or incomplete");

    const auto & limits = capabilities.limits;
    if (limits.maximum_definitions == 0 || limits.maximum_definition_bytes == 0 || limits.maximum_template_nodes == 0
        || limits.maximum_direct_dependencies == 0 || limits.maximum_transitive_dependencies == 0 || limits.maximum_checker_work == 0)
        fail(AuthorityError::Code::InvalidConfiguration, "every Atomic authority limit must be nonzero");
}

void validatePublicationLimits(const PublicationLimits & limits)
{
    if (limits.hazard_slot_count == 0 || limits.hazard_slot_count > maximum_supported_hazard_slots)
        fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority hazard-slot count is unsupported");
    if (limits.maximum_retired_root_count == 0 || limits.maximum_retired_root_count > maximum_supported_retired_roots
        || !std::in_range<std::size_t>(limits.maximum_retired_root_count))
        fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority retired-root count is unsupported");
    if (limits.maximum_retired_root_charged_bytes == 0)
        fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority logical retirement-charge limit is zero");
}

void validateDatabase(const CompositeRoot & root, UUID database_uuid)
{
    if (root.getDatabaseUUID() != database_uuid)
        fail(AuthorityError::Code::DatabaseMismatch, "Atomic authority root belongs to another database");
}

void validateRecoveredRoot(const CompositeRoot & root, UUID database_uuid)
{
    validateDatabase(root, database_uuid);
    const UInt64 persistent_capabilities = root.getPersistentCapabilityMask();
    if (persistent_capabilities != definition_authority_capability_mask
        && persistent_capabilities != dependent_object_authority_capability_mask)
        fail(AuthorityError::Code::CapabilityMismatch, "recovered Atomic authority root has an unsupported persistent capability set");
    if (persistent_capabilities == dependent_object_authority_capability_mask && root.getDatabaseCatalogEpoch() < 2)
        fail(
            AuthorityError::Code::EpochMismatch,
            "recovered dependent-object-capable authority root predates the required activation transition");
}

void validateFirstPublication(const CompositeRoot & root, UUID database_uuid)
{
    validateRecoveredRoot(root, database_uuid);
    if (root.getDatabaseCatalogEpoch() != 1)
        fail(AuthorityError::Code::EpochMismatch, "initial Atomic authority root must have database catalog epoch 1");
    if (root.getPersistentCapabilityMask() != definition_authority_capability_mask)
        fail(AuthorityError::Code::CapabilityMismatch, "initial Atomic authority root must activate the complete definition-only format");
    if (!root.getDefinitionRecords().empty() && root.getTypeIndexGeneration() == 0)
        fail(AuthorityError::Code::GenerationMismatch, "initial nonempty Atomic type index must advance beyond generation zero");
}

bool isValidCapabilityTransition(UInt64 previous, UInt64 next) noexcept
{
    return previous == next || (previous == definition_authority_capability_mask && next == dependent_object_authority_capability_mask);
}

bool isDependentObjectActivation(const CompositeRoot & previous, const CompositeRoot & next) noexcept
{
    return previous.getPersistentCapabilityMask() == definition_authority_capability_mask
        && next.getPersistentCapabilityMask() == dependent_object_authority_capability_mask;
}

bool transfersContentPayloadCharge(const CompositeRoot & previous, const CompositeRoot & next) noexcept
{
    if (!previous.sharesContentPayloadWith(next))
        return false;
    return isDependentObjectActivation(previous, next) || previous.getPersistentCapabilityMask() == next.getPersistentCapabilityMask();
}

void validateReplacement(const CompositeRoot * current, const CompositeRoot & replacement, UUID database_uuid)
{
    if (!current)
    {
        validateFirstPublication(replacement, database_uuid);
        return;
    }

    validateDatabase(replacement, database_uuid);
    if (current->getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max()
        || replacement.getDatabaseCatalogEpoch() != current->getDatabaseCatalogEpoch() + 1)
        fail(AuthorityError::Code::EpochMismatch, "Atomic authority replacement must advance the database catalog epoch exactly once");
    if (!isValidCapabilityTransition(current->getPersistentCapabilityMask(), replacement.getPersistentCapabilityMask()))
        fail(AuthorityError::Code::CapabilityMismatch, "Atomic authority persistent capability transition is invalid");
    if (isDependentObjectActivation(*current, replacement))
    {
        if (!current->sharesContentPayloadWith(replacement))
            fail(
                AuthorityError::Code::CapabilityMismatch,
                "Atomic dependent-object capability activation rebuilt or changed the authority content payload");
        const AuthorityState expected = activateDependentObjectAuthority(current->getAuthorityState());
        if (replacement.getAuthorityState() != expected)
            fail(
                AuthorityError::Code::CapabilityMismatch,
                "Atomic dependent-object capability activation is not the exact content-neutral V1 transition");
    }
    if (replacement.getTypeIndexGeneration() < current->getTypeIndexGeneration())
        fail(AuthorityError::Code::GenerationMismatch, "Atomic authority type-index generation regressed");
    if (replacement.getTypeIndexContentDigest() != current->getTypeIndexContentDigest()
        && replacement.getTypeIndexGeneration() == current->getTypeIndexGeneration())
        fail(AuthorityError::Code::GenerationMismatch, "Atomic authority changed its type index without advancing its generation");
}

UInt64 validateRootLimitsAndComputeRetirementCharge(
    const CompositeRoot & root,
    const TypeAuthorityCapabilities & capabilities,
    const PublicationLimits & publication_limits,
    bool reuse_definition_validation,
    AtomicAuthorityPublicationStatistics * statistics = nullptr)
{
    const auto & authority_limits = capabilities.limits;
    const UInt64 definition_count = root.getDefinitionRecordCount();
    if (definition_count > authority_limits.maximum_definitions)
        fail(AuthorityError::Code::LimitExceeded, "Atomic authority definition count exceeds the advertised limit");

    const UInt64 retirement_charge = checkedAdd(
        CompositeRoot::getWrapperLogicalCharge(), root.getContentPayloadLogicalCharge(), "Atomic authority retirement charge overflow");

    if (statistics)
        statistics->reused_definition_validation = reuse_definition_validation;
    if (!reuse_definition_validation)
    {
        const auto records = root.getDefinitionRecords();
        if (toUInt64(records.size(), "Atomic authority definition count does not fit UInt64") != definition_count)
            fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority definition count differs from its record store");
        for (const auto & record : records)
        {
            const auto definition = root.findByIdentity(record.identity);
            if (!definition)
                fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority record has no checked definition");
            if (definition->getNodes().size() > authority_limits.maximum_template_nodes)
                fail(AuthorityError::Code::LimitExceeded, "Atomic authority definition exceeds the template-node limit");
            if (definition->getDependencies().size() > authority_limits.maximum_direct_dependencies)
                fail(AuthorityError::Code::LimitExceeded, "Atomic authority definition exceeds the direct-dependency limit");
            if (definition->getCertificate().transitive_dependency_count > authority_limits.maximum_transitive_dependencies)
                fail(AuthorityError::Code::LimitExceeded, "Atomic authority definition exceeds the transitive-dependency limit");
            if (definition->getCertificate().charged_work > authority_limits.maximum_checker_work)
                fail(AuthorityError::Code::LimitExceeded, "Atomic authority definition exceeds the checker-work limit");

            const auto definition_bytes = tryCountLogicalRetainedDefinitionBytes(*definition, authority_limits.maximum_definition_bytes);
            if (!definition_bytes)
                fail(AuthorityError::Code::LimitExceeded, "Atomic authority definition exceeds the retained-byte limit");
            if (statistics)
                ++statistics->definition_records_validated;
        }
    }

    if (retirement_charge > publication_limits.maximum_retired_root_charged_bytes)
        fail(AuthorityError::Code::LimitExceeded, "Atomic authority root exceeds the logical retirement-charge limit");
    return retirement_charge;
}

UInt64 getRootServerLiveBytesMaximum(const CompositeRoot & root) noexcept
{
    return root.getDatabaseResourceQuota().getLimits().get(ResourceLimit::LiveCatalogAndCacheBytesPerServer);
}

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

void recordRootPublication(const CompositeRoot & root) noexcept
{
    incrementProfileEventNoThrow(ProfileEvents::UDTAuthorityRootPublications);
    incrementProfileEventNoThrow(
        ProfileEvents::UDTAuthorityPublishedDeterministicCatalogBytes,
        root.getDatabaseResourceQuota().getUsage().get(ResourceLimit::DeterministicCatalogBytesPerDatabase));
}

ServerResourceQuotaTracker::Ptr acquireServerQuotaTrackerForRoot(const CompositeRoot & root)
{
    return ServerResourceQuotaTracker::acquireProcessTracker(getRootServerLiveBytesMaximum(root));
}

[[noreturn]] void rethrowServerQuotaError(const ServerResourceQuotaTrackerError & error)
{
    switch (error.code)
    {
        case ServerResourceQuotaTrackerError::Code::LimitExceeded:
        case ServerResourceQuotaTrackerError::Code::ArithmeticOverflow: fail(AuthorityError::Code::LimitExceeded, error.what());
        case ServerResourceQuotaTrackerError::Code::InvalidConfiguration: fail(AuthorityError::Code::InvalidConfiguration, error.what());
    }
    std::terminate();
}

class CompositeHazardDomain final
{
public:
    explicit CompositeHazardDomain(std::size_t slot_count_)
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

    void publish(std::size_t slot, const CompositeRoot * root) noexcept { slots[slot].hazard.store(root, std::memory_order_seq_cst); }

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

    bool isHazarded(const CompositeRoot * root) const noexcept
    {
        for (std::size_t index = 0; index < slot_count; ++index)
        {
            if (slots[index].hazard.load(std::memory_order_seq_cst) == root)
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
        std::atomic<const CompositeRoot *> hazard{nullptr};
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

class CompositeRootLease final
{
public:
    CompositeRootLease(CompositeHazardDomain * domain_, std::size_t slot_, const CompositeRoot * root_) noexcept
        : domain(domain_)
        , slot(slot_)
        , root(root_)
    {
    }

    CompositeRootLease(const CompositeRootLease &) = delete;
    CompositeRootLease & operator=(const CompositeRootLease &) = delete;
    CompositeRootLease(CompositeRootLease &&) = delete;
    CompositeRootLease & operator=(CompositeRootLease &&) = delete;

    ~CompositeRootLease()
    {
        if (root)
            domain->release(slot);
    }

    CompositeHazardDomain * getDomain() const noexcept { return domain; }
    std::size_t getSlot() const noexcept { return slot; }
    const CompositeRoot * getRoot() const noexcept { return root; }
    void disarm() noexcept { root = nullptr; }

private:
    CompositeHazardDomain * domain;
    std::size_t slot;
    const CompositeRoot * root;
};

void releaseCompositeLease(void * context, std::size_t slot) noexcept
{
    static_cast<CompositeHazardDomain *>(context)->release(slot);
}

struct EmptyAuthoritySnapshot final
{
};

const EmptyAuthoritySnapshot empty_authority_snapshot;

Definition::Ptr findEmptyByIdentity(const void *, const DefinitionIdentity &)
{
    return {};
}

Definition::Ptr findEmptyByName(const void *, std::string_view)
{
    return {};
}

UInt64 getEmptyGeneration(const void *) noexcept
{
    return 0;
}

}

class AtomicAuthority::Impl final
{
public:
    struct RetiredRoot final
    {
        RetiredRoot(
            CompositeRootPtr root_, UInt64 retirement_charge_, bool owns_content_payload_charge_, bool may_share_content_payload_) noexcept
            : root(std::move(root_))
            , retirement_charge(retirement_charge_)
            , owns_content_payload_charge(owns_content_payload_charge_)
            , may_share_content_payload(may_share_content_payload_)
        {
        }

        CompositeRootPtr root;
        UInt64 retirement_charge;
        bool owns_content_payload_charge;
        bool may_share_content_payload;
    };

    Impl(
        CompositeRootPtr initial_root,
        UInt64 initial_retirement_charge,
        const AtomicAuthorityPublicationLimits & limits_,
        ServerResourceQuotaTracker::Ptr server_quota_tracker_)
        : limits(limits_)
        , hazard_domain(std::make_unique<CompositeHazardDomain>(limits.hazard_slot_count))
        , current_owner(std::move(initial_root))
        , current_retirement_charge(initial_retirement_charge)
        , server_quota_tracker(std::move(server_quota_tracker_))
    {
        retired.reserve(static_cast<std::size_t>(limits.maximum_retired_root_count));
        current.store(current_owner.get(), std::memory_order_release);
    }

    CompositeRootLease acquireRoot() const
    {
        auto * domain = hazard_domain.get();
        const auto claimed = domain->tryClaim();
        if (!claimed)
        {
            if (!domain->isAccepting())
                fail(AuthorityError::Code::Shutdown, "Atomic user-defined type authority is shut down");
            fail(AuthorityError::Code::HazardSlotsExhausted, "Atomic user-defined type authority hazard slots are exhausted");
        }

        const std::size_t slot = *claimed;
        for (;;)
        {
            const CompositeRoot * observed = current.load(std::memory_order_acquire);
            if (!observed)
            {
                domain->release(slot);
                if (shutdown.load(std::memory_order_acquire))
                    fail(AuthorityError::Code::Shutdown, "Atomic user-defined type authority is shut down");
                return CompositeRootLease(nullptr, 0, nullptr);
            }
            domain->publish(slot, observed);
            if (observed == current.load(std::memory_order_acquire))
                return CompositeRootLease(domain, slot, observed);
            domain->clear(slot);
        }
    }

    void ensureRetirementCapacity(UInt64 retiring_root_charge) const
    {
        if (!current_owner)
            return;
        const UInt64 projected_count = checkedAdd(
            toUInt64(retired.size(), "Atomic retired-root count does not fit UInt64"), 1, "Atomic retired-root count overflow");
        const UInt64 projected_charge = checkedAdd(retired_charge, retiring_root_charge, "Atomic retired-root charge overflow");
        if (projected_count > limits.maximum_retired_root_count || projected_charge > limits.maximum_retired_root_charged_bytes)
            fail(AuthorityError::Code::LimitExceeded, "Atomic authority retired-root backlog is full");
        if (retired.size() == retired.capacity())
            std::terminate();
    }

    bool hasOtherRootWithSharedContentPayload(std::size_t candidate_index) const noexcept
    {
        const auto & candidate = *retired[candidate_index].root;
        if (current_owner && candidate.sharesContentPayloadWith(*current_owner))
            return true;
        for (std::size_t index = 0; index < retired.size(); ++index)
        {
            if (index != candidate_index && candidate.sharesContentPayloadWith(*retired[index].root))
                return true;
        }
        return false;
    }

    bool currentSharesContentPayloadWithRetired() const noexcept
    {
        if (!current_owner)
            return false;
        for (const auto & retired_root : retired)
        {
            if (current_owner->sharesContentPayloadWith(*retired_root.root))
                return true;
        }
        return false;
    }

    RetirementState scanRetired()
    {
        for (;;)
        {
            CompositeRootPtr reclaim;
            UInt64 reclaim_server_charge = 0;
            RetirementState state;
            {
                std::lock_guard lock(writer_mutex);
                for (std::size_t index = 0; index < retired.size(); ++index)
                {
                    if (hazard_domain->isHazarded(retired[index].root.get()))
                        continue;
                    /// The one full payload charge cannot disappear while a
                    /// wrapper sharing that payload is still retained. Keep
                    /// its owner until every sibling wrapper is reclaimable.
                    if (retired[index].owns_content_payload_charge && retired[index].may_share_content_payload
                        && hasOtherRootWithSharedContentPayload(index))
                        continue;
                    retired_charge -= retired[index].retirement_charge;
                    reclaim_server_charge = retired[index].retirement_charge;
                    reclaim = std::move(retired[index].root);
                    if (index + 1 != retired.size())
                        retired[index] = std::move(retired.back());
                    retired.pop_back();
                    break;
                }
                if (!reclaim && shutdown.load(std::memory_order_acquire) && current_owner && !hazard_domain->isHazarded(current_owner.get())
                    && (!current_payload_may_share_with_retired || !currentSharesContentPayloadWithRetired()))
                {
                    reclaim_server_charge = current_retirement_charge;
                    reclaim = std::move(current_owner);
                    current_retirement_charge = 0;
                }

                if (!reclaim)
                {
                    state = retirementStateUnlocked();
                }
            }
            if (!reclaim)
                return state;
            reclaim.reset();
            if (!server_quota_tracker || reclaim_server_charge == 0)
                std::terminate();
            server_quota_tracker->releaseCommitted(reclaim_server_charge);
        }
    }

    RetirementState retirementState() const
    {
        std::lock_guard lock(writer_mutex);
        return retirementStateUnlocked();
    }

    RetirementState retirementStateUnlocked() const
    {
        RetirementState state{
            .retired_root_count = static_cast<UInt64>(retired.size()),
            .retired_root_charged_bytes = retired_charge,
            .current_root_charged_bytes = current_owner ? current_retirement_charge : 0,
            .active_hazard_slots = hazard_domain->activeSlots(),
            .shutdown = shutdown.load(std::memory_order_acquire),
        };
        if (state.shutdown && current_owner)
        {
            ++state.retired_root_count;
            state.retired_root_charged_bytes += current_retirement_charge;
        }
        return state;
    }

    void beginShutdown() noexcept
    {
        std::lock_guard lock(writer_mutex);
        if (shutdown.exchange(true, std::memory_order_acq_rel))
            return;
        hazard_domain->close();
        const CompositeRoot * expected_old = current_owner.get();
        const CompositeRoot * exchanged = current.exchange(nullptr, std::memory_order_acq_rel);
        if (exchanged != expected_old)
            std::terminate();
    }

    void shutdownAndDrain() noexcept
    {
        beginShutdown();
        for (;;)
        {
            const auto state = scanRetired();
            if (state.retired_root_count == 0 && state.active_hazard_slots == 0)
                return;
            std::this_thread::yield();
        }
    }

    const AtomicAuthorityPublicationLimits limits;
    std::unique_ptr<CompositeHazardDomain> hazard_domain;
    std::atomic<const CompositeRoot *> current{nullptr};
    std::atomic<bool> shutdown{false};
    mutable std::mutex writer_mutex;
    CompositeRootPtr current_owner;
    UInt64 current_retirement_charge = 0;
    bool current_payload_may_share_with_retired = false;
    std::vector<RetiredRoot> retired;
    UInt64 retired_charge = 0;
    ServerResourceQuotaTracker::Ptr server_quota_tracker;
    IAtomicAuthorityPublicationObserver * publication_observer = nullptr;
};

AtomicAuthorityError::AtomicAuthorityError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AtomicAuthority::RootSnapshot::RootSnapshot(void * hazard_domain_, std::size_t hazard_slot_, const CompositeRoot * root_) noexcept
    : hazard_domain(hazard_domain_)
    , hazard_slot(hazard_slot_)
    , root(root_)
{
}

AtomicAuthority::RootSnapshot::RootSnapshot(RootSnapshot && other) noexcept
    : hazard_domain(std::exchange(other.hazard_domain, nullptr))
    , hazard_slot(other.hazard_slot)
    , root(std::exchange(other.root, nullptr))
{
}

AtomicAuthority::RootSnapshot::~RootSnapshot()
{
    if (root)
        releaseCompositeLease(hazard_domain, hazard_slot);
}

const AtomicAuthority::CompositeRoot & AtomicAuthority::RootSnapshot::get() const
{
    if (!root)
        std::terminate();
    return *root;
}

AtomicAuthority::PreparedPublication::PreparedPublication(
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
    AtomicAuthorityPublicationStatistics statistics_) noexcept
    : owner(owner_)
    , expected_root(expected_root_)
    , expected_database_catalog_epoch(expected_database_catalog_epoch_)
    , server_quota_reservation(std::move(server_quota_reservation_))
    , replacement_root(std::move(replacement_root_))
    , replacement_retirement_charged_bytes(replacement_retirement_charged_bytes_)
    , expected_retiring_root_charged_bytes(expected_retiring_root_charged_bytes_)
    , transfers_content_payload_charge(transfers_content_payload_charge_)
    , publication_observer(publication_observer_)
    , observer_transition(std::move(observer_transition_))
    , statistics(statistics_)
{
}

AtomicAuthority::AtomicAuthority(
    UUID database_uuid_,
    TypeAuthorityCapabilities capabilities_,
    CompositeRootPtr initial_root,
    const AtomicAuthorityPublicationLimits & publication_limits)
    : database_uuid(database_uuid_)
    , capabilities(std::move(capabilities_))
    , impl()
{
    if (database_uuid == UUIDHelpers::Nil)
        fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority database UUID is nil");
    validateCapabilities(capabilities);
    validatePublicationLimits(publication_limits);

    UInt64 initial_retirement_charge = 0;
    ServerResourceQuotaTracker::Ptr server_quota_tracker;
    ServerResourceQuotaTracker::PreparedReservation initial_server_reservation;
    /// Declared after the rollback reservation so a failed construction
    /// destroys the recovered root before returning its process-wide charge.
    CompositeRootPtr owned_initial_root = std::move(initial_root);
    if (owned_initial_root)
    {
        validateRecoveredRoot(*owned_initial_root, database_uuid);
        initial_retirement_charge
            = validateRootLimitsAndComputeRetirementCharge(*owned_initial_root, capabilities, publication_limits, false);
        try
        {
            server_quota_tracker = acquireServerQuotaTrackerForRoot(*owned_initial_root);
            initial_server_reservation = server_quota_tracker->prepare(initial_retirement_charge);
        }
        catch (const ServerResourceQuotaTrackerError & error)
        {
            rethrowServerQuotaError(error);
        }
    }
    impl = std::make_unique<Impl>(
        std::move(owned_initial_root), initial_retirement_charge, publication_limits, std::move(server_quota_tracker));
    initial_server_reservation.commit();
    if (impl->current_owner)
        recordRootPublication(*impl->current_owner);
}

AtomicAuthority::~AtomicAuthority()
{
    impl->shutdownAndDrain();
}

void AtomicAuthority::requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const
{
    if (!capabilities.containsAll(required))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Atomic user-defined type authority lacks capabilities required for {}", operation);
}

AtomicAuthority::RootSnapshot AtomicAuthority::acquireCurrentRoot() const
{
    auto lease = impl->acquireRoot();
    RootSnapshot result(lease.getDomain(), lease.getSlot(), lease.getRoot());
    lease.disarm();
    return result;
}

IAuthorityAdapter::ResolutionSession AtomicAuthority::beginResolutionSession() const
{
    auto lease = impl->acquireRoot();
    if (!lease.getRoot())
    {
        return makeSnapshotResolutionSession(
            &empty_authority_snapshot,
            {
                .find_by_identity = findEmptyByIdentity,
                .find_by_name = findEmptyByName,
                .get_generation = getEmptyGeneration,
                .get_effective_resource_limits = nullptr,
            });
    }

    auto result = makeSnapshotResolutionSession(
        lease.getRoot(),
        {
            .find_by_identity = findByIdentityInSnapshot,
            .find_by_name = findByNameInSnapshot,
            .get_generation = getSnapshotGeneration,
            .get_effective_resource_limits = getSnapshotEffectiveResourceLimits,
        },
        lease.getDomain(),
        lease.getSlot(),
        releaseCompositeLease);
    lease.disarm();
    return result;
}

AtomicAuthority::PreparedPublication AtomicAuthority::preparePublication(CompositeRootPtr replacement_root) const
{
    if (!replacement_root)
        fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority replacement root is null");

    std::lock_guard lock(impl->writer_mutex);
    if (impl->shutdown.load(std::memory_order_acquire))
        fail(AuthorityError::Code::Shutdown, "cannot prepare publication on a shut-down Atomic authority");
    validateReplacement(impl->current_owner.get(), *replacement_root, database_uuid);

    const bool transfers_content_payload_charge
        = impl->current_owner && transfersContentPayloadCharge(*impl->current_owner, *replacement_root);
    const bool reuses_definition_validation = impl->current_owner && impl->current_owner->sharesDefinitionContentWith(*replacement_root);
    AtomicAuthorityPublicationStatistics statistics;
    const UInt64 replacement_retirement_charged_bytes = transfers_content_payload_charge
        ? impl->current_retirement_charge
        : validateRootLimitsAndComputeRetirementCharge(
              *replacement_root, capabilities, impl->limits, reuses_definition_validation, &statistics);
    if (transfers_content_payload_charge)
        statistics.reused_definition_validation = true;
    const UInt64 expected_retiring_root_charged_bytes = !impl->current_owner ? 0
        : transfers_content_payload_charge                                   ? CompositeRoot::getWrapperLogicalCharge()
                                                                             : impl->current_retirement_charge;

    impl->ensureRetirementCapacity(expected_retiring_root_charged_bytes);

    ServerResourceQuotaTracker::PreparedReservation server_quota_reservation;
    try
    {
        auto tracker = acquireServerQuotaTrackerForRoot(*replacement_root);
        if (impl->server_quota_tracker && impl->server_quota_tracker.get() != tracker.get())
            fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority changed its process-wide server quota tracker");
        if (!impl->server_quota_tracker)
            impl->server_quota_tracker = std::move(tracker);

        const UInt64 current_server_charge = impl->current_owner ? impl->current_retirement_charge : 0;
        const UInt64 prospective_server_charge = checkedAdd(
            replacement_retirement_charged_bytes,
            expected_retiring_root_charged_bytes,
            "Atomic authority publication server charge overflows UInt64");
        if (prospective_server_charge < current_server_charge)
            fail(AuthorityError::Code::InvalidConfiguration, "Atomic authority publication server charge unexpectedly decreases");
        server_quota_reservation = impl->server_quota_tracker->prepare(prospective_server_charge - current_server_charge);
    }
    catch (const ServerResourceQuotaTrackerError & error)
    {
        rethrowServerQuotaError(error);
    }
    try
    {
        fiu_do_on(DB::FailPoints::udt_authority_prepared_publication_failure, {
            fail(AuthorityError::Code::InvalidConfiguration, "fault injected before prepared authority publication");
        });
        auto * publication_observer = impl->publication_observer;
        std::unique_ptr<IAtomicAuthorityPublicationObserver::PreparedTransition> observer_transition;
        if (publication_observer && impl->current_owner)
            observer_transition = publication_observer->prepareAuthorityPublication(*impl->current_owner, *replacement_root);
        return PreparedPublication(
            this,
            impl->current_owner.get(),
            impl->current_owner ? impl->current_owner->getDatabaseCatalogEpoch() : 0,
            std::move(replacement_root),
            replacement_retirement_charged_bytes,
            expected_retiring_root_charged_bytes,
            transfers_content_payload_charge,
            std::move(server_quota_reservation),
            publication_observer,
            std::move(observer_transition),
            statistics);
    }
    catch (...)
    {
        /// Keep the process-wide charge live until the prepared replacement's
        /// last owning reference has been destroyed. Function parameters
        /// otherwise unwind after locals, which would return the reservation
        /// before releasing the potentially large immutable root.
        replacement_root.reset();
        throw;
    }
}

void AtomicAuthority::setPublicationObserver(IAtomicAuthorityPublicationObserver * observer) noexcept
{
    std::lock_guard lock(impl->writer_mutex);
    if (impl->shutdown.load(std::memory_order_acquire))
    {
        if (observer)
            std::terminate();
        impl->publication_observer = nullptr;
        return;
    }
    impl->publication_observer = observer;
}

void AtomicAuthority::publish(PreparedPublication && publication) noexcept
{
    if (publication.owner != this || !publication.replacement_root)
        std::terminate();

    std::lock_guard lock(impl->writer_mutex);
    if (impl->shutdown.load(std::memory_order_acquire) || impl->current_owner.get() != publication.expected_root
        || publication.publication_observer != impl->publication_observer)
        std::terminate();
    const UInt64 current_epoch = impl->current_owner ? impl->current_owner->getDatabaseCatalogEpoch() : 0;
    if (current_epoch != publication.expected_database_catalog_epoch)
        std::terminate();

    if (impl->current_owner)
    {
        const bool transfers_content_payload_charge = transfersContentPayloadCharge(*impl->current_owner, *publication.replacement_root);
        const UInt64 retiring_root_charge
            = transfers_content_payload_charge ? CompositeRoot::getWrapperLogicalCharge() : impl->current_retirement_charge;
        if (transfers_content_payload_charge != publication.transfers_content_payload_charge
            || retiring_root_charge != publication.expected_retiring_root_charged_bytes
            || (transfers_content_payload_charge && publication.replacement_retirement_charged_bytes != impl->current_retirement_charge))
            std::terminate();

        const UInt64 projected_count = static_cast<UInt64>(impl->retired.size()) + 1;
        if (retiring_root_charge > std::numeric_limits<UInt64>::max() - impl->retired_charge)
            std::terminate();
        const UInt64 projected_charge = impl->retired_charge + retiring_root_charge;
        if (projected_count > impl->limits.maximum_retired_root_count || projected_charge > impl->limits.maximum_retired_root_charged_bytes
            || impl->retired.size() == impl->retired.capacity())
            std::terminate();

        const CompositeRoot * exchanged = impl->current.exchange(publication.replacement_root.get(), std::memory_order_acq_rel);
        if (exchanged != publication.expected_root)
            std::terminate();
        impl->retired.emplace_back(
            std::move(impl->current_owner),
            retiring_root_charge,
            !transfers_content_payload_charge,
            impl->current_payload_may_share_with_retired || transfers_content_payload_charge);
        impl->retired_charge = projected_charge;
    }
    else
    {
        const CompositeRoot * exchanged = impl->current.exchange(publication.replacement_root.get(), std::memory_order_acq_rel);
        if (exchanged != nullptr)
            std::terminate();
    }

    impl->current_owner = std::move(publication.replacement_root);
    impl->current_retirement_charge = publication.replacement_retirement_charged_bytes;
    impl->current_payload_may_share_with_retired = publication.transfers_content_payload_charge;
    publication.server_quota_reservation.commit();
    if (publication.observer_transition)
        publication.observer_transition->publish();
    recordRootPublication(*impl->current_owner);
}

bool AtomicAuthority::isFirstPublicationReadyForActivation() const noexcept
{
    std::lock_guard lock(impl->writer_mutex);
    const auto * root = impl->current_owner.get();
    return !impl->shutdown.load(std::memory_order_acquire) && root && impl->current.load(std::memory_order_acquire) == root
        && root->getDatabaseCatalogEpoch() == 1 && root->getPersistentCapabilityMask() == definition_authority_capability_mask;
}

AtomicAuthorityRetirementState AtomicAuthority::scanRetired()
{
    return impl->scanRetired();
}

AtomicAuthorityRetirementState AtomicAuthority::getRetirementState() const
{
    return impl->retirementState();
}

void AtomicAuthority::shutdownAndDrain() noexcept
{
    impl->shutdownAndDrain();
}

bool AtomicAuthority::isShutdown() const noexcept
{
    return impl->shutdown.load(std::memory_order_acquire);
}

Definition::Ptr AtomicAuthority::findByIdentityInSnapshot(const void * view, const DefinitionIdentity & identity)
{
    return static_cast<const CompositeRoot *>(view)->findByIdentity(identity);
}

Definition::Ptr AtomicAuthority::findByNameInSnapshot(const void * view, std::string_view normalized_local_name)
{
    return static_cast<const CompositeRoot *>(view)->findByName(normalized_local_name);
}

UInt64 AtomicAuthority::getSnapshotGeneration(const void * view) noexcept
{
    return static_cast<const CompositeRoot *>(view)->getTypeIndexGeneration();
}

const EffectiveResourceLimits * AtomicAuthority::getSnapshotEffectiveResourceLimits(const void * view) noexcept
{
    return &static_cast<const CompositeRoot *>(view)->getDatabaseResourceQuota().getLimits();
}
}
