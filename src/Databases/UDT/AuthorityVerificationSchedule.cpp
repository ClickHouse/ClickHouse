#include <Databases/UDT/AuthorityVerificationSchedule.h>

#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <Common/Stopwatch.h>

#include <algorithm>
#include <array>
#include <limits>
#include <tuple>
#include <type_traits>
#include <utility>

namespace DB::UDT
{
namespace
{

using ScheduleError = AuthorityVerificationScheduleError;

constexpr UInt64 canonical_uuid_bytes = 16;
constexpr UInt64 inventory_key_canonical_bytes = sizeof(UInt16) + sizeof(UInt8) + canonical_uuid_bytes;
/// V1 is a nibble Patricia tree (at most two key-nibbles per byte), and each
/// branch searches at most 16 children. Charge four comparisons per visited
/// depth plus the terminal leaf comparison without exposing tree internals.
constexpr UInt64 maximum_inventory_lookup_work = 8 * inventory_key_canonical_bytes + 1;
constexpr UInt64 inventory_leaf_canonical_bytes = inventory_key_canonical_bytes + sizeof(UInt64) + sizeof(Digest);
constexpr UInt64 scheduled_target_canonical_bytes
    = inventory_leaf_canonical_bytes + 4 * sizeof(UInt64) + sizeof(AuthorityVerificationSelectionReasons);
constexpr UInt64 authority_root_identity_canonical_bytes = canonical_uuid_bytes + sizeof(UInt64) + sizeof(Digest);
constexpr UInt64 schedule_cursor_maximum_canonical_bytes = sizeof(UInt16) + canonical_uuid_bytes + sizeof(UInt32) + sizeof(UInt64)
    + sizeof(UInt32) + sizeof(UInt8) + inventory_key_canonical_bytes + 2 * sizeof(UInt64);
constexpr UInt64 schedule_statistics_canonical_bytes = 15 * sizeof(UInt64);
constexpr UInt64 schedule_plan_base_canonical_bytes = authority_root_identity_canonical_bytes + sizeof(Digest) + sizeof(UInt8)
    + 2 * schedule_cursor_maximum_canonical_bytes + sizeof(UInt64) + schedule_statistics_canonical_bytes;

[[noreturn]] void fail(ScheduleError::Code code, std::string_view message)
{
    throw ScheduleError(code, message);
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

size_t toSize(UInt64 value, std::string_view message)
{
    if (!std::in_range<size_t>(value))
        fail(ScheduleError::Code::ArithmeticOverflow, message);
    return static_cast<size_t>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(ScheduleError::Code::ArithmeticOverflow, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(ScheduleError::Code::ArithmeticOverflow, message);
    return lhs * rhs;
}

bool costFits(const AuthorityVerificationTargetCost & actual, const AuthorityVerificationTargetCost & declared) noexcept
{
    return actual.canonical_bytes <= declared.canonical_bytes && actual.work_units <= declared.work_units
        && actual.transient_bytes <= declared.transient_bytes && actual.io_bytes <= declared.io_bytes;
}

bool verificationPassBudgetExpired(const AuthorityVerificationPassBudget & budget) noexcept
{
    if (budget.cancellation.stop_requested())
        return true;
    if (budget.monotonic_deadline && std::chrono::steady_clock::now() >= *budget.monotonic_deadline)
        return true;
    return budget.thread_cpu_deadline_nanoseconds && clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) >= *budget.thread_cpu_deadline_nanoseconds;
}

AuthorityVerificationTargetCost aggregateCosts(const AuthorityVerificationTargetCost & lhs, const AuthorityVerificationTargetCost & rhs)
{
    return {
        .canonical_bytes
        = checkedAdd(lhs.canonical_bytes, rhs.canonical_bytes, "authority verification receipt canonical-byte accounting overflows UInt64"),
        .work_units = checkedAdd(lhs.work_units, rhs.work_units, "authority verification receipt work-unit accounting overflows UInt64"),
        .transient_bytes = std::max(lhs.transient_bytes, rhs.transient_bytes),
        .io_bytes = checkedAdd(lhs.io_bytes, rhs.io_bytes, "authority verification receipt I/O-byte accounting overflows UInt64"),
    };
}

bool isKnownDisposition(AuthorityVerificationTargetDisposition disposition) noexcept
{
    switch (disposition)
    {
        case AuthorityVerificationTargetDisposition::Verified:
        case AuthorityVerificationTargetDisposition::Damaged: return true;
    }
    return false;
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

template <typename T>
std::array<CanonicalByte, sizeof(T)> encodeUnsignedBE(T value) noexcept
{
    static_assert(std::is_unsigned_v<T>);
    std::array<CanonicalByte, sizeof(T)> result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<CanonicalByte>(value >> (8 * (result.size() - index - 1)));
    return result;
}

template <typename T>
void updateUnsigned(CanonicalHasher & hash, T value)
{
    const auto bytes = encodeUnsignedBE(value);
    hash.update(bytes);
}

void updateInventoryKey(CanonicalHasher & hash, const AuthorityInventoryKey & key)
{
    updateUnsigned(hash, key.format_version);
    updateUnsigned(hash, static_cast<UInt8>(key.record_kind));
    hash.updateUUID(key.object_uuid);
}

UInt64 firstDigestUInt64(const Digest & digest) noexcept
{
    UInt64 result = 0;
    for (size_t index = 0; index < sizeof(result); ++index)
        result = (result << 8) | digest[index];
    return result;
}

UInt32 targetBucket(const AuthorityInventoryKey & key, const AuthorityVerificationSchedulePolicy & policy)
{
    CanonicalHasher hash("ClickHouse.UDT.AuthorityVerificationSchedule.Bucket");
    updateUnsigned(hash, authority_verification_schedule_contract_abi);
    updateUnsigned(hash, policy.bucket_seed);
    updateInventoryKey(hash, key);
    return static_cast<UInt32>(firstDigestUInt64(hash.finalize()) % policy.bucket_count);
}

Digest randomCandidateRank(
    const AuthorityRootIdentity & root,
    const AuthorityInventoryKey & key,
    const AuthorityVerificationSchedulePolicy & policy,
    UInt64 batch_sequence)
{
    CanonicalHasher hash("ClickHouse.UDT.AuthorityVerificationSchedule.Sample");
    updateUnsigned(hash, authority_verification_schedule_contract_abi);
    updateUnsigned(hash, policy.bucket_seed);
    updateUnsigned(hash, batch_sequence);
    hash.updateUUID(root.database_uuid);
    updateUnsigned(hash, root.database_catalog_epoch);
    hash.update(root.authority_anchor);
    updateInventoryKey(hash, key);
    return hash.finalize();
}

void validateLimits(const AuthorityVerificationScheduleLimits & limits)
{
    constexpr AuthorityVerificationScheduleLimits maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };
    if (!valid(limits.maximum_snapshot_targets, maxima.maximum_snapshot_targets)
        || !valid(limits.maximum_targets_per_batch, maxima.maximum_targets_per_batch)
        || !valid(limits.maximum_buckets, maxima.maximum_buckets)
        || !valid(limits.maximum_reverse_dependency_count, maxima.maximum_reverse_dependency_count)
        || !valid(limits.maximum_canonical_bytes_per_batch, maxima.maximum_canonical_bytes_per_batch)
        || !valid(limits.maximum_verification_work_units_per_batch, maxima.maximum_verification_work_units_per_batch)
        || !valid(limits.maximum_transient_bytes_per_batch, maxima.maximum_transient_bytes_per_batch)
        || !valid(limits.maximum_io_bytes_per_batch, maxima.maximum_io_bytes_per_batch)
        || !valid(limits.maximum_rooted_target_canonical_bytes, maxima.maximum_rooted_target_canonical_bytes)
        || !valid(limits.maximum_rooted_target_verification_work_units, maxima.maximum_rooted_target_verification_work_units)
        || !valid(limits.maximum_rooted_target_transient_bytes, maxima.maximum_rooted_target_transient_bytes)
        || !valid(limits.maximum_rooted_target_io_bytes, maxima.maximum_rooted_target_io_bytes)
        || limits.maximum_rooted_target_canonical_bytes < limits.maximum_canonical_bytes_per_batch
        || limits.maximum_rooted_target_verification_work_units < limits.maximum_verification_work_units_per_batch
        || limits.maximum_rooted_target_transient_bytes < limits.maximum_transient_bytes_per_batch
        || limits.maximum_rooted_target_io_bytes < limits.maximum_io_bytes_per_batch
        || !valid(limits.maximum_planner_work_units, maxima.maximum_planner_work_units)
        || !valid(limits.maximum_planner_scratch_bytes, maxima.maximum_planner_scratch_bytes)
        || !valid(limits.maximum_retained_canonical_bytes, maxima.maximum_retained_canonical_bytes))
        fail(ScheduleError::Code::InvalidConfiguration, "authority verification schedule limits are invalid");
}

void validatePolicy(const AuthorityVerificationSchedulePolicy & policy, const AuthorityVerificationScheduleLimits & limits)
{
    constexpr AuthorityVerificationScheduleLimits implementation;
    /// Candidate reservoirs may exceed a lowered exact-root target count;
    /// their actual allocation/work is independently covered by planner
    /// requirements. Keep only the immutable implementation-domain bound.
    if (policy.bucket_count == 0 || policy.bucket_count > limits.maximum_buckets || policy.recent_catalog_epoch_window == 0
        || policy.high_dependency_threshold == 0 || policy.maximum_recent_targets_per_batch == 0
        || policy.maximum_high_dependency_targets_per_batch == 0 || policy.random_targets_per_batch == 0
        || policy.maximum_recent_targets_per_batch > implementation.maximum_snapshot_targets
        || policy.maximum_high_dependency_targets_per_batch > implementation.maximum_snapshot_targets
        || policy.random_targets_per_batch > implementation.maximum_snapshot_targets)
        fail(ScheduleError::Code::InvalidConfiguration, "authority verification schedule policy is invalid");
}

AuthorityRootIdentity validateRoot(const AuthorityRoot & authority)
{
    const auto & state = authority.getAuthorityState();
    const auto & inventory = authority.getInventorySummary();
    const AuthorityRootIdentity root{
        .database_uuid = state.database_uuid,
        .database_catalog_epoch = state.database_catalog_epoch,
        .authority_anchor = state.anchor_hash,
    };
    if (root.database_uuid == UUIDHelpers::Nil || root.database_catalog_epoch == 0 || isZeroDigest(root.authority_anchor))
        fail(ScheduleError::Code::InvalidRootIdentity, "authority verification schedule root identity is invalid");
    if (inventory.leaf_count != state.leaf_count || inventory.merkle_radix_root != state.inventory_root)
        fail(ScheduleError::Code::InvalidRootIdentity, "authority verification schedule root has an inconsistent inventory identity");
    return root;
}

void validateInventoryKey(const AuthorityInventoryKey & key, ScheduleError::Code code, std::string_view message)
{
    if (key.format_version != authority_inventory_format_version || !isKnownInventoryRecordKind(key.record_kind)
        || key.object_uuid == UUIDHelpers::Nil)
        fail(code, message);
}

void validateCursor(
    const AuthorityVerificationScheduleCursor & cursor,
    const AuthorityRootIdentity & root,
    const AuthorityVerificationSchedulePolicy & policy)
{
    if (cursor.contract_abi != authority_verification_schedule_contract_abi || cursor.database_uuid != root.database_uuid
        || cursor.bucket_count != policy.bucket_count || cursor.bucket_seed != policy.bucket_seed
        || cursor.current_bucket >= policy.bucket_count || cursor.completed_rotations == std::numeric_limits<UInt64>::max()
        || cursor.planned_batches == std::numeric_limits<UInt64>::max())
        fail(ScheduleError::Code::InvalidCursor, "authority verification schedule cursor is incompatible with its policy or database");
    if (cursor.resume_after)
    {
        validateInventoryKey(
            *cursor.resume_after, ScheduleError::Code::InvalidCursor, "authority verification schedule cursor key is invalid");
        if (targetBucket(*cursor.resume_after, policy) != cursor.current_bucket)
            fail(ScheduleError::Code::InvalidCursor, "authority verification schedule cursor key belongs to another bucket");
    }
}

class ScheduleBudget final
{
public:
    ScheduleBudget(const AuthorityVerificationScheduleLimits & limits_, AuthorityVerificationScheduleStatistics & statistics_)
        : limits(limits_)
        , statistics(statistics_)
    {
    }

    void chargePlannerWork(UInt64 amount)
    {
        statistics.planner_work_units = admit(
            statistics.planner_work_units,
            amount,
            limits.maximum_planner_work_units,
            "authority verification scheduling exceeds its planner-work limit");
    }

    void chargePlannerScratch(UInt64 amount)
    {
        statistics.planner_scratch_bytes = admit(
            statistics.planner_scratch_bytes,
            amount,
            limits.maximum_planner_scratch_bytes,
            "authority verification scheduling exceeds its planner-scratch limit");
    }

    void chargeRetained(UInt64 amount)
    {
        statistics.retained_canonical_bytes = admit(
            statistics.retained_canonical_bytes,
            amount,
            limits.maximum_retained_canonical_bytes,
            "authority verification batch exceeds its retained canonical-byte limit");
    }

    bool canAdmitTarget(const AuthorityVerificationTargetCost & cost, UInt64 selected_count) const noexcept
    {
        if (selected_count >= limits.maximum_targets_per_batch
            || !fits(statistics.retained_canonical_bytes, scheduled_target_canonical_bytes, limits.maximum_retained_canonical_bytes))
            return false;
        if (selected_count == 0 && cost.canonical_bytes <= limits.maximum_rooted_target_canonical_bytes
            && cost.work_units <= limits.maximum_rooted_target_verification_work_units
            && cost.transient_bytes <= limits.maximum_rooted_target_transient_bytes
            && cost.io_bytes <= limits.maximum_rooted_target_io_bytes)
            return true;
        return fits(statistics.verification_canonical_bytes, cost.canonical_bytes, limits.maximum_canonical_bytes_per_batch)
            && fits(statistics.verification_work_units, cost.work_units, limits.maximum_verification_work_units_per_batch)
            && statistics.verification_transient_bytes <= limits.maximum_transient_bytes_per_batch
            && cost.transient_bytes <= limits.maximum_transient_bytes_per_batch
            && fits(statistics.verification_io_bytes, cost.io_bytes, limits.maximum_io_bytes_per_batch);
    }

    void chargeTarget(const AuthorityVerificationTargetCost & cost, bool first_target)
    {
        const bool rooted_indivisible = first_target
            && (cost.canonical_bytes > limits.maximum_canonical_bytes_per_batch
                || cost.work_units > limits.maximum_verification_work_units_per_batch
                || cost.transient_bytes > limits.maximum_transient_bytes_per_batch || cost.io_bytes > limits.maximum_io_bytes_per_batch);
        if (rooted_indivisible)
        {
            if (cost.canonical_bytes > limits.maximum_rooted_target_canonical_bytes
                || cost.work_units > limits.maximum_rooted_target_verification_work_units
                || cost.transient_bytes > limits.maximum_rooted_target_transient_bytes
                || cost.io_bytes > limits.maximum_rooted_target_io_bytes)
                fail(
                    ScheduleError::Code::UnschedulableRotationTarget, "rooted indivisible verification target exceeds its admitted domain");
            statistics.verification_canonical_bytes = cost.canonical_bytes;
            statistics.verification_work_units = cost.work_units;
            statistics.verification_transient_bytes = cost.transient_bytes;
            statistics.verification_io_bytes = cost.io_bytes;
            chargeRetained(scheduled_target_canonical_bytes);
            return;
        }
        statistics.verification_canonical_bytes = admit(
            statistics.verification_canonical_bytes,
            cost.canonical_bytes,
            limits.maximum_canonical_bytes_per_batch,
            "authority verification batch exceeds its canonical-byte limit");
        statistics.verification_work_units = admit(
            statistics.verification_work_units,
            cost.work_units,
            limits.maximum_verification_work_units_per_batch,
            "authority verification batch exceeds its verification-work limit");
        statistics.verification_transient_bytes = std::max(statistics.verification_transient_bytes, cost.transient_bytes);
        statistics.verification_io_bytes = admit(
            statistics.verification_io_bytes,
            cost.io_bytes,
            limits.maximum_io_bytes_per_batch,
            "authority verification batch exceeds its I/O-byte limit");
        chargeRetained(scheduled_target_canonical_bytes);
    }

private:
    static bool fits(UInt64 current, UInt64 amount, UInt64 maximum) noexcept { return current <= maximum && amount <= maximum - current; }

    static UInt64 admit(UInt64 current, UInt64 amount, UInt64 maximum, std::string_view message)
    {
        if (!fits(current, amount, maximum))
            fail(ScheduleError::Code::LimitExceeded, message);
        return current + amount;
    }

    const AuthorityVerificationScheduleLimits & limits;
    AuthorityVerificationScheduleStatistics & statistics;
};

AuthorityInventory::Ptr pinAndValidateTargetSet(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> targets,
    const AuthorityVerificationScheduleLimits & limits)
{
    const auto pinned_inventory = authority.pinAuthorityInventory();
    if (!pinned_inventory || pinned_inventory->getSummary() != authority.getInventorySummary())
        fail(ScheduleError::Code::InvalidRootIdentity, "authority verification schedule cannot pin its exact inventory");
    if (pinned_inventory->getSummary().leaf_count != toUInt64(targets.size()))
        fail(ScheduleError::Code::NonCanonicalTargetSet, "authority verification target set is not the complete anchored inventory");
    if (toUInt64(targets.size()) > limits.maximum_snapshot_targets)
        fail(ScheduleError::Code::LimitExceeded, "authority verification snapshot exceeds its target limit");
    return pinned_inventory;
}

void validateTarget(
    const AuthorityInventory & pinned_inventory,
    const AuthorityRootIdentity & root,
    std::span<const AuthorityVerificationTarget> targets,
    size_t index,
    const AuthorityVerificationScheduleLimits & limits,
    ScheduleBudget & budget)
{
    budget.chargePlannerWork(maximum_inventory_lookup_work);
    const auto & target = targets[index];
    validateInventoryKey(target.leaf.key, ScheduleError::Code::InvalidTarget, "authority verification target inventory key is invalid");
    const AuthorityInventoryLeaf * anchored_leaf = pinned_inventory.find(target.leaf.key);
    if (anchored_leaf == nullptr || *anchored_leaf != target.leaf)
        fail(ScheduleError::Code::NonCanonicalTargetSet, "authority verification target differs from its anchored inventory leaf");
    if (target.leaf.object_revision == 0 || isZeroDigest(target.leaf.canonical_record_hash)
        || target.last_changed_catalog_epoch > root.database_catalog_epoch
        || target.reverse_dependency_count > limits.maximum_reverse_dependency_count || target.cost.canonical_bytes == 0
        || target.cost.work_units == 0 || target.cost.transient_bytes == 0 || target.cost.io_bytes == 0)
        fail(ScheduleError::Code::InvalidTarget, "authority verification target is invalid");
    if (target.cost.canonical_bytes > limits.maximum_rooted_target_canonical_bytes
        || target.cost.work_units > limits.maximum_rooted_target_verification_work_units
        || target.cost.transient_bytes > limits.maximum_rooted_target_transient_bytes
        || target.cost.io_bytes > limits.maximum_rooted_target_io_bytes)
        fail(ScheduleError::Code::UnschedulableRotationTarget, "authority verification target cannot fit into an empty periodic batch");
    if (index != 0 && !authorityInventoryKeyLess(targets[index - 1].leaf.key, target.leaf.key))
        fail(ScheduleError::Code::NonCanonicalTargetSet, "authority verification targets are not strictly inventory-key sorted and unique");
}

void validateTargets(
    const AuthorityRoot & authority,
    const AuthorityRootIdentity & root,
    std::span<const AuthorityVerificationTarget> targets,
    const AuthorityVerificationScheduleLimits & limits,
    ScheduleBudget & budget)
{
    const auto pinned_inventory = pinAndValidateTargetSet(authority, targets, limits);
    for (size_t index = 0; index < targets.size(); ++index)
        validateTarget(*pinned_inventory, root, targets, index, limits, budget);
}

template <typename Candidate, typename Compare>
void insertBoundedCandidate(
    std::vector<Candidate> & candidates, Candidate candidate, UInt64 maximum_candidates, Compare compare, ScheduleBudget & budget)
{
    const UInt64 movement_bound
        = checkedAdd(toUInt64(candidates.size()), 2, "authority verification candidate-selection work overflows UInt64");
    budget.chargePlannerWork(movement_bound);
    const auto position = std::lower_bound(candidates.begin(), candidates.end(), candidate, compare);
    if (toUInt64(candidates.size()) == maximum_candidates)
    {
        if (position == candidates.end())
            return;
        const size_t position_index = static_cast<size_t>(position - candidates.begin());
        candidates.pop_back();
        candidates.insert(candidates.begin() + position_index, std::move(candidate));
        return;
    }
    candidates.insert(position, std::move(candidate));
}

bool keyLess(const AuthorityVerificationTarget & lhs, const AuthorityVerificationTarget & rhs) noexcept
{
    return authorityInventoryKeyLess(lhs.leaf.key, rhs.leaf.key);
}

bool recentCandidateLess(size_t lhs_index, size_t rhs_index, std::span<const AuthorityVerificationTarget> targets) noexcept
{
    const auto & lhs = targets[lhs_index];
    const auto & rhs = targets[rhs_index];
    return std::tuple{
               lhs.last_periodic_verification_sequence,
               std::numeric_limits<UInt64>::max() - lhs.last_changed_catalog_epoch,
               std::numeric_limits<UInt64>::max() - lhs.reverse_dependency_count}
        < std::
            tuple{rhs.last_periodic_verification_sequence, std::numeric_limits<UInt64>::max() - rhs.last_changed_catalog_epoch, std::numeric_limits<UInt64>::max() - rhs.reverse_dependency_count}
    || (lhs.last_periodic_verification_sequence == rhs.last_periodic_verification_sequence
        && lhs.last_changed_catalog_epoch == rhs.last_changed_catalog_epoch && lhs.reverse_dependency_count == rhs.reverse_dependency_count
        && keyLess(lhs, rhs));
}

bool highDependencyCandidateLess(size_t lhs_index, size_t rhs_index, std::span<const AuthorityVerificationTarget> targets) noexcept
{
    const auto & lhs = targets[lhs_index];
    const auto & rhs = targets[rhs_index];
    return std::tuple{
               lhs.last_periodic_verification_sequence,
               std::numeric_limits<UInt64>::max() - lhs.reverse_dependency_count,
               std::numeric_limits<UInt64>::max() - lhs.last_changed_catalog_epoch}
        < std::
            tuple{rhs.last_periodic_verification_sequence, std::numeric_limits<UInt64>::max() - rhs.reverse_dependency_count, std::numeric_limits<UInt64>::max() - rhs.last_changed_catalog_epoch}
    || (lhs.last_periodic_verification_sequence == rhs.last_periodic_verification_sequence
        && lhs.reverse_dependency_count == rhs.reverse_dependency_count && lhs.last_changed_catalog_epoch == rhs.last_changed_catalog_epoch
        && keyLess(lhs, rhs));
}

struct RandomCandidate
{
    size_t index = 0;
    Digest rank{};
};

AuthorityVerificationPlanningRequirements
computePlanningRequirements(UInt64 snapshot_targets, const AuthorityVerificationSchedulePolicy & policy, UInt64 maximum_targets_per_batch)
{
    AuthorityVerificationScheduleLimits domain_limits;
    domain_limits.maximum_targets_per_batch = maximum_targets_per_batch;
    validateLimits(domain_limits);
    validatePolicy(policy, domain_limits);
    if (snapshot_targets > domain_limits.maximum_snapshot_targets)
        fail(ScheduleError::Code::LimitExceeded, "authority verification planning requirement target count exceeds its hard domain");

    AuthorityVerificationPlanningRequirements result;
    result.planner_work_units = 8;
    if (snapshot_targets != 0)
    {
        result.planner_work_units = checkedAdd(
            result.planner_work_units,
            checkedMultiply(policy.bucket_count, 2, "authority verification planning bucket work overflows UInt64"),
            "authority verification planning bucket initialization/search work overflows UInt64");
        UInt64 per_target_work
            = checkedAdd(5, maximum_inventory_lookup_work, "authority verification planning per-target validation work overflows UInt64");
        per_target_work = checkedAdd(
            per_target_work,
            checkedAdd(policy.maximum_recent_targets_per_batch, 2, "authority verification recent-candidate work overflows UInt64"),
            "authority verification planning per-target work overflows UInt64");
        per_target_work = checkedAdd(
            per_target_work,
            checkedAdd(
                policy.maximum_high_dependency_targets_per_batch,
                2,
                "authority verification high-dependency candidate work overflows UInt64"),
            "authority verification planning per-target work overflows UInt64");
        per_target_work = checkedAdd(
            per_target_work,
            checkedAdd(policy.random_targets_per_batch, 2, "authority verification random-candidate work overflows UInt64"),
            "authority verification planning per-target work overflows UInt64");
        result.planner_work_units = checkedAdd(
            result.planner_work_units,
            checkedMultiply(snapshot_targets, per_target_work, "authority verification planning target work overflows UInt64"),
            "authority verification planning work overflows UInt64");

        const UInt64 priority_calls = checkedAdd(
            checkedAdd(
                policy.maximum_recent_targets_per_batch,
                policy.maximum_high_dependency_targets_per_batch,
                "authority verification priority call count overflows UInt64"),
            checkedAdd(policy.random_targets_per_batch, 1, "authority verification priority call count overflows UInt64"),
            "authority verification priority call count overflows UInt64");
        const UInt64 maximum_add_calls = checkedAdd(
            priority_calls,
            checkedAdd(maximum_targets_per_batch, 1, "authority verification rotation call count overflows UInt64"),
            "authority verification add-target call count overflows UInt64");
        result.planner_work_units = checkedAdd(
            result.planner_work_units,
            checkedMultiply(
                maximum_add_calls,
                checkedAdd(maximum_targets_per_batch, 1, "authority verification selected lookup bound overflows UInt64"),
                "authority verification add-target work overflows UInt64"),
            "authority verification planning work overflows UInt64");

        const UInt64 priority_scratch_items = checkedAdd(
            checkedAdd(
                policy.maximum_recent_targets_per_batch,
                policy.maximum_high_dependency_targets_per_batch,
                "authority verification priority scratch count overflows UInt64"),
            policy.random_targets_per_batch,
            "authority verification priority scratch count overflows UInt64");
        result.planner_scratch_bytes = checkedAdd(
            checkedMultiply(snapshot_targets, sizeof(UInt32), "authority verification target-bucket scratch overflows UInt64"),
            checkedMultiply(policy.bucket_count, sizeof(size_t), "authority verification bucket-index scratch overflows UInt64"),
            "authority verification planning scratch overflows UInt64");
        result.planner_scratch_bytes = checkedAdd(
            result.planner_scratch_bytes,
            checkedMultiply(priority_scratch_items, sizeof(RandomCandidate), "authority verification priority scratch overflows UInt64"),
            "authority verification planning scratch overflows UInt64");
        result.planner_scratch_bytes = checkedAdd(
            result.planner_scratch_bytes,
            checkedMultiply(maximum_targets_per_batch, sizeof(size_t), "authority verification selected-index scratch overflows UInt64"),
            "authority verification planning scratch overflows UInt64");
        result.planner_scratch_bytes = checkedAdd(
            result.planner_scratch_bytes,
            checkedMultiply(
                maximum_targets_per_batch,
                sizeof(ScheduledAuthorityVerificationTarget),
                "authority verification selected-target scratch overflows UInt64"),
            "authority verification planning scratch overflows UInt64");
    }
    result.retained_canonical_bytes = checkedAdd(
        schedule_plan_base_canonical_bytes,
        checkedMultiply(
            std::min(snapshot_targets, maximum_targets_per_batch),
            scheduled_target_canonical_bytes,
            "authority verification retained target bytes overflow UInt64"),
        "authority verification retained plan bytes overflow UInt64");
    return result;
}

bool randomCandidateLess(
    const RandomCandidate & lhs, const RandomCandidate & rhs, std::span<const AuthorityVerificationTarget> targets) noexcept
{
    if (lhs.rank != rhs.rank)
        return lhs.rank < rhs.rank;
    return keyLess(targets[lhs.index], targets[rhs.index]);
}

void chargeReason(AuthorityVerificationSelectionReason reason, AuthorityVerificationScheduleStatistics & statistics)
{
    switch (reason)
    {
        case AuthorityVerificationSelectionReason::Rotation: ++statistics.rotation_targets; break;
        case AuthorityVerificationSelectionReason::RecentlyChanged: ++statistics.recent_targets; break;
        case AuthorityVerificationSelectionReason::HighDependency: ++statistics.high_dependency_targets; break;
        case AuthorityVerificationSelectionReason::OutsideBucketSample: ++statistics.random_targets; break;
    }
}

bool addScheduledTarget(
    size_t index,
    AuthorityVerificationSelectionReason reason,
    bool required,
    std::span<const AuthorityVerificationTarget> source,
    std::vector<size_t> & selected_indices,
    std::vector<ScheduledAuthorityVerificationTarget> & selected,
    ScheduleBudget & budget,
    AuthorityVerificationScheduleStatistics & statistics)
{
    budget.chargePlannerWork(
        checkedAdd(toUInt64(selected_indices.size()), 1, "authority verification selected-target lookup work overflows UInt64"));
    const auto existing = std::find(selected_indices.begin(), selected_indices.end(), index);
    const auto reason_mask = authorityVerificationSelectionReasonMask(reason);
    if (existing != selected_indices.end())
    {
        const size_t selected_index = static_cast<size_t>(existing - selected_indices.begin());
        if ((selected[selected_index].reasons & reason_mask) == 0)
        {
            selected[selected_index].reasons |= reason_mask;
            chargeReason(reason, statistics);
        }
        return true;
    }

    const auto & target = source[index];
    if (!budget.canAdmitTarget(target.cost, toUInt64(selected.size())))
    {
        if (required)
            fail(
                ScheduleError::Code::UnschedulableRotationTarget,
                "next rotating authority verification target cannot fit into the remaining batch budget");
        return false;
    }

    budget.chargeTarget(target.cost, selected.empty());
    selected_indices.push_back(index);
    selected.push_back({target.leaf, target.cost, reason_mask});
    chargeReason(reason, statistics);
    return true;
}

bool isRecent(
    const AuthorityVerificationTarget & target,
    const AuthorityRootIdentity & root,
    const AuthorityVerificationSchedulePolicy & policy) noexcept
{
    if (target.last_changed_catalog_epoch == 0)
        return false;
    const UInt64 age = root.database_catalog_epoch - target.last_changed_catalog_epoch;
    return age < policy.recent_catalog_epoch_window;
}

void incrementChecked(UInt64 & value, std::string_view message)
{
    if (value == std::numeric_limits<UInt64>::max())
        fail(ScheduleError::Code::ArithmeticOverflow, message);
    ++value;
}

}

class AuthorityVerificationPlanningContinuation::Impl final
{
public:
    enum class Phase : UInt8
    {
        Initialize,
        InitializeBuckets,
        ValidateAndBucket,
        FindRotation,
        PrepareSelection,
        SelectPriority,
        AddInitialRotation,
        AddRecent,
        AddHighDependency,
        AddRandom,
        ScanRotation,
        Finish,
    };

    static constexpr size_t no_index = std::numeric_limits<size_t>::max();

    AuthorityRootIdentity root;
    Digest target_set_digest{};
    AuthorityVerificationScheduleCursor cursor;
    AuthorityVerificationScheduleCursor next_cursor;
    AuthorityVerificationSchedulePolicy policy;
    AuthorityVerificationScheduleLimits limits;
    const AuthorityVerificationTarget * snapshot_data = nullptr;
    size_t snapshot_size = 0;
    AuthorityVerificationScheduleStatistics statistics;
    Phase phase = Phase::Initialize;
    size_t index = 0;
    std::vector<UInt32> target_buckets;
    std::vector<size_t> first_in_bucket;
    size_t first_after_cursor = no_index;
    UInt64 bucket_search_offset = 1;
    size_t rotation_index = no_index;
    UInt32 rotation_bucket = 0;
    UInt64 rotation_bucket_offset = 0;
    UInt64 next_batch_sequence = 0;
    std::vector<size_t> recent_candidates;
    std::vector<size_t> high_dependency_candidates;
    std::vector<RandomCandidate> random_candidates;
    std::vector<size_t> selected_indices;
    std::vector<ScheduledAuthorityVerificationTarget> selected;
    size_t last_rotation_index = no_index;
    size_t next_rotation_index = no_index;
};

AuthorityVerificationPlanningContinuation::AuthorityVerificationPlanningContinuation(std::unique_ptr<Impl> impl_) noexcept
    : impl(std::move(impl_))
{
}

AuthorityVerificationPlanningContinuation::~AuthorityVerificationPlanningContinuation() = default;

AuthorityVerificationScheduleError::AuthorityVerificationScheduleError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityVerificationPlanningRequirements computeAuthorityVerificationPlanningRequirements(
    UInt64 snapshot_targets, const AuthorityVerificationSchedulePolicy & policy, UInt64 maximum_targets_per_batch)
{
    return computePlanningRequirements(snapshot_targets, policy, maximum_targets_per_batch);
}

Digest computeAuthorityVerificationTargetSetDigest(const AuthorityInventorySummary & inventory)
{
    CanonicalHasher hash("ClickHouse.UDT.AuthorityVerificationSchedule.TargetSet");
    updateUnsigned(hash, authority_verification_schedule_contract_abi);
    updateUnsigned(hash, inventory.leaf_count);
    hash.update(inventory.merkle_radix_root);
    return hash.finalize();
}

AuthorityVerificationBatchPlan::AuthorityVerificationBatchPlan(
    AuthorityRootIdentity root_,
    Digest target_set_digest_,
    AuthorityVerificationScheduleStatus status_,
    AuthorityVerificationScheduleCursor retry_cursor_,
    AuthorityVerificationScheduleCursor next_cursor_,
    std::vector<ScheduledAuthorityVerificationTarget> targets_,
    AuthorityVerificationScheduleStatistics statistics_) noexcept
    : root(std::move(root_))
    , target_set_digest(std::move(target_set_digest_))
    , status(status_)
    , retry_cursor(std::move(retry_cursor_))
    , next_cursor(std::move(next_cursor_))
    , targets(std::move(targets_))
    , statistics(std::move(statistics_))
{
}

AuthorityVerificationBatchReceipt::AuthorityVerificationBatchReceipt(
    AuthorityRootIdentity root_,
    Digest target_set_digest_,
    AuthorityVerificationScheduleCursor retry_cursor_,
    UInt64 planned_target_count_,
    std::vector<AuthorityVerificationTargetCompletion> terminal_completions_,
    AuthorityVerificationTargetCost actual_charged_cost_) noexcept
    : root(std::move(root_))
    , target_set_digest(std::move(target_set_digest_))
    , retry_cursor(std::move(retry_cursor_))
    , planned_target_count(planned_target_count_)
    , terminal_completions(std::move(terminal_completions_))
    , actual_charged_cost(std::move(actual_charged_cost_))
{
}

AuthorityVerificationBatchReceipt::Ptr AuthorityVerificationBatchReceiptFactory::issue(
    const AuthorityVerificationBatchPlan & plan,
    const AuthorityRootIdentity & executed_root,
    const Digest & executed_target_set_digest,
    std::vector<AuthorityVerificationTargetCompletion> && ordered_terminal_completions,
    const AuthorityVerificationTargetCost & total_charged_cost)
{
    const auto scheduled_targets = plan.getTargets();
    if (ordered_terminal_completions.size() > scheduled_targets.size())
        fail(ScheduleError::Code::InvalidReceipt, "authority verification receipt has more completions than scheduled targets");

    const AuthorityRootIdentity receipt_root = executed_root;
    const Digest receipt_target_set_digest = executed_target_set_digest;
    const AuthorityVerificationTargetCost receipt_charged_cost = total_charged_cost;
    std::vector<AuthorityVerificationTargetCompletion> terminal_completions(std::move(ordered_terminal_completions));

    if (receipt_root != plan.getRoot() || receipt_target_set_digest != plan.getTargetSetDigest()
        || plan.getChargeABI() != authority_verification_charge_abi)
        fail(ScheduleError::Code::InvalidReceipt, "authority verification receipt is not for the plan's exact root and target set");

    AuthorityVerificationTargetCost completed_cost;
    for (size_t index = 0; index < terminal_completions.size(); ++index)
    {
        const auto & completion = terminal_completions[index];
        const auto & scheduled = scheduled_targets[index];
        if (!isKnownDisposition(completion.disposition) || completion.leaf != scheduled.leaf)
            fail(ScheduleError::Code::InvalidReceipt, "authority verification receipt completions are not an ordered plan prefix");
        if (!costFits(completion.actual_charged_cost, scheduled.cost))
            fail(ScheduleError::Code::InvalidReceipt, "authority verification target exceeded its declared cost");
        completed_cost = aggregateCosts(completed_cost, completion.actual_charged_cost);
    }

    const auto & statistics = plan.getStatistics();
    const AuthorityVerificationTargetCost declared_batch_cost{
        .canonical_bytes = statistics.verification_canonical_bytes,
        .work_units = statistics.verification_work_units,
        .transient_bytes = statistics.verification_transient_bytes,
        .io_bytes = statistics.verification_io_bytes,
    };
    if (!costFits(completed_cost, receipt_charged_cost) || !costFits(receipt_charged_cost, declared_batch_cost))
        fail(ScheduleError::Code::InvalidReceipt, "authority verification receipt has inconsistent charged costs");
    if (terminal_completions.size() == scheduled_targets.size() && receipt_charged_cost != completed_cost)
        fail(ScheduleError::Code::InvalidReceipt, "complete authority verification receipt has unattributed charged costs");

    return AuthorityVerificationBatchReceipt::Ptr(new AuthorityVerificationBatchReceipt(
        receipt_root,
        receipt_target_set_digest,
        plan.getRetryCursor(),
        toUInt64(scheduled_targets.size()),
        std::move(terminal_completions),
        receipt_charged_cost));
}

void AuthorityVerificationBatchReceiptFactory::adoptVerifiedPrefix(
    const AuthorityVerificationBatchPlan & plan,
    const AuthorityVerificationBatchReceipt & prefix,
    std::vector<AuthorityVerificationTargetCompletion> & terminal_completions,
    AuthorityVerificationTargetCost & completed_cost)
{
    const auto scheduled_targets = plan.getTargets();
    if (prefix.root != plan.getRoot() || prefix.target_set_digest != plan.getTargetSetDigest() || prefix.charge_abi != plan.getChargeABI()
        || prefix.retry_cursor != plan.getRetryCursor() || prefix.planned_target_count != toUInt64(scheduled_targets.size())
        || prefix.terminal_completions.size() > scheduled_targets.size())
    {
        fail(ScheduleError::Code::InvalidReceipt, "authority verification prefix receipt is not bound to the resumed plan");
    }

    AuthorityVerificationTargetCost verified_cost;
    for (size_t index = 0; index < prefix.terminal_completions.size(); ++index)
    {
        const auto & completion = prefix.terminal_completions[index];
        const auto & scheduled = scheduled_targets[index];
        if (completion.leaf != scheduled.leaf || completion.disposition != AuthorityVerificationTargetDisposition::Verified
            || !costFits(completion.actual_charged_cost, scheduled.cost))
        {
            fail(ScheduleError::Code::InvalidReceipt, "authority verification resumed prefix is not ordered, clean completion evidence");
        }
        verified_cost = aggregateCosts(verified_cost, completion.actual_charged_cost);
    }
    if (!costFits(verified_cost, prefix.actual_charged_cost))
        fail(ScheduleError::Code::InvalidReceipt, "authority verification resumed prefix has inconsistent charged costs");

    terminal_completions = prefix.terminal_completions;
    completed_cost = verified_cost;
}

AuthorityVerificationScheduleCursor makeAuthorityVerificationScheduleCursor(
    const UUID & database_uuid, const AuthorityVerificationSchedulePolicy & policy, const AuthorityVerificationScheduleLimits & limits)
{
    validateLimits(limits);
    validatePolicy(policy, limits);
    if (database_uuid == UUIDHelpers::Nil)
        fail(ScheduleError::Code::InvalidCursor, "authority verification schedule database UUID is invalid");

    AuthorityVerificationScheduleCursor result;
    result.database_uuid = database_uuid;
    result.bucket_count = policy.bucket_count;
    result.bucket_seed = policy.bucket_seed;
    return result;
}

AuthorityVerificationBatchPlan::Ptr planPeriodicAuthorityVerification(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    const AuthorityVerificationScheduleCursor & cursor,
    const AuthorityVerificationSchedulePolicy & policy,
    const AuthorityVerificationScheduleLimits & limits)
{
    AuthorityVerificationPlanningContinuation::Ptr continuation;
    while (true)
    {
        auto result
            = resumePeriodicAuthorityVerificationPlanning(authority, sorted_unique_targets, cursor, policy, limits, continuation, {});
        if (result.status == AuthorityVerificationPlanningStatus::Complete)
            return std::move(result.plan);
    }
}

AuthorityVerificationPlanningResult resumePeriodicAuthorityVerificationPlanning(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    const AuthorityVerificationScheduleCursor & cursor,
    const AuthorityVerificationSchedulePolicy & policy,
    const AuthorityVerificationScheduleLimits & limits,
    std::unique_ptr<AuthorityVerificationPlanningContinuation> & continuation,
    const AuthorityVerificationPassBudget & pass_budget)
{
    if (pass_budget.maximum_work_items == 0)
        fail(ScheduleError::Code::InvalidConfiguration, "authority verification planning pass budget is zero");
    if (verificationPassBudgetExpired(pass_budget))
        return {};

    validateLimits(limits);
    validatePolicy(policy, limits);
    const AuthorityRootIdentity root = validateRoot(authority);
    validateCursor(cursor, root, policy);
    const auto pinned_inventory = pinAndValidateTargetSet(authority, sorted_unique_targets, limits);
    const Digest target_set_digest = computeAuthorityVerificationTargetSetDigest(authority.getInventorySummary());

    if (!continuation)
    {
        auto state = std::make_unique<AuthorityVerificationPlanningContinuation::Impl>();
        state->root = root;
        state->target_set_digest = target_set_digest;
        state->cursor = cursor;
        state->next_cursor = cursor;
        state->policy = policy;
        state->limits = limits;
        state->snapshot_data = sorted_unique_targets.data();
        state->snapshot_size = sorted_unique_targets.size();
        state->statistics.snapshot_targets = toUInt64(sorted_unique_targets.size());
        state->statistics.maximum_batches_per_full_rotation = state->statistics.snapshot_targets;
        continuation.reset(new AuthorityVerificationPlanningContinuation(std::move(state)));
    }

    auto & state = *continuation->impl;
    if (state.root != root || state.target_set_digest != target_set_digest || state.cursor != cursor || state.policy != policy
        || state.limits != limits || state.snapshot_data != sorted_unique_targets.data()
        || state.snapshot_size != sorted_unique_targets.size())
    {
        fail(ScheduleError::Code::InvalidTarget, "authority verification planning continuation input changed");
    }

    ScheduleBudget schedule_budget(state.limits, state.statistics);
    UInt64 pass_work_items = 0;
    const auto take_work_item = [&]()
    {
        if (pass_work_items >= pass_budget.maximum_work_items || verificationPassBudgetExpired(pass_budget))
            return false;
        ++pass_work_items;
        return true;
    };
    const auto make_in_progress_result = [&]()
    {
        return AuthorityVerificationPlanningResult{
            .status = AuthorityVerificationPlanningStatus::InProgress,
            .plan = {},
            .consumed_work_items = pass_work_items,
        };
    };

    while (true)
    {
        switch (state.phase)
        {
            case AuthorityVerificationPlanningContinuation::Impl::Phase::Initialize: {
                if (!take_work_item())
                    return make_in_progress_result();
                const auto requirements
                    = computePlanningRequirements(state.statistics.snapshot_targets, state.policy, state.limits.maximum_targets_per_batch);
                if (requirements.planner_work_units > state.limits.maximum_planner_work_units
                    || requirements.planner_scratch_bytes > state.limits.maximum_planner_scratch_bytes
                    || requirements.retained_canonical_bytes > state.limits.maximum_retained_canonical_bytes)
                {
                    fail(
                        ScheduleError::Code::LimitExceeded,
                        "authority verification planning requirements exceed the exact-root planning domain");
                }
                schedule_budget.chargePlannerWork(8);
                schedule_budget.chargeRetained(schedule_plan_base_canonical_bytes);
                if (state.snapshot_size == 0)
                {
                    auto plan = AuthorityVerificationBatchPlan::Ptr(new AuthorityVerificationBatchPlan(
                        state.root,
                        state.target_set_digest,
                        AuthorityVerificationScheduleStatus::EmptySnapshot,
                        state.cursor,
                        state.cursor,
                        {},
                        state.statistics));
                    continuation.reset();
                    return {
                        .status = AuthorityVerificationPlanningStatus::Complete,
                        .plan = std::move(plan),
                        .consumed_work_items = pass_work_items,
                    };
                }

                schedule_budget.chargePlannerScratch(checkedMultiply(
                    toUInt64(state.snapshot_size), sizeof(UInt32), "authority verification bucket scratch bytes overflow UInt64"));
                schedule_budget.chargePlannerScratch(checkedMultiply(
                    state.policy.bucket_count, sizeof(size_t), "authority verification bucket-index scratch bytes overflow UInt64"));
                state.target_buckets.reserve(state.snapshot_size);
                state.first_in_bucket.reserve(state.policy.bucket_count);
                state.index = 0;
                state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::InitializeBuckets;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::InitializeBuckets: {
                if (state.index == state.policy.bucket_count)
                {
                    state.index = 0;
                    state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::ValidateAndBucket;
                    continue;
                }
                if (!take_work_item())
                    return make_in_progress_result();
                schedule_budget.chargePlannerWork(1);
                state.first_in_bucket.push_back(AuthorityVerificationPlanningContinuation::Impl::no_index);
                ++state.index;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::ValidateAndBucket: {
                if (state.index == state.snapshot_size)
                {
                    if (state.first_after_cursor != AuthorityVerificationPlanningContinuation::Impl::no_index)
                    {
                        state.rotation_index = state.first_after_cursor;
                        state.rotation_bucket = state.cursor.current_bucket;
                        ++state.statistics.buckets_examined;
                        state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::PrepareSelection;
                    }
                    else
                    {
                        state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::FindRotation;
                    }
                    continue;
                }
                if (!take_work_item())
                    return make_in_progress_result();
                schedule_budget.chargePlannerWork(3);
                validateTarget(*pinned_inventory, state.root, sorted_unique_targets, state.index, state.limits, schedule_budget);
                ++state.statistics.targets_examined;
                const UInt32 bucket = targetBucket(sorted_unique_targets[state.index].leaf.key, state.policy);
                state.target_buckets.push_back(bucket);
                if (state.first_in_bucket[bucket] == AuthorityVerificationPlanningContinuation::Impl::no_index)
                    state.first_in_bucket[bucket] = state.index;
                if (bucket == state.cursor.current_bucket
                    && (!state.cursor.resume_after
                        || authorityInventoryKeyLess(*state.cursor.resume_after, sorted_unique_targets[state.index].leaf.key))
                    && state.first_after_cursor == AuthorityVerificationPlanningContinuation::Impl::no_index)
                    state.first_after_cursor = state.index;
                ++state.index;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::FindRotation: {
                if (state.bucket_search_offset > state.policy.bucket_count)
                    fail(ScheduleError::Code::InvalidTarget, "nonempty authority verification snapshot has no schedulable bucket");
                if (!take_work_item())
                    return make_in_progress_result();
                schedule_budget.chargePlannerWork(1);
                ++state.statistics.buckets_examined;
                const UInt64 absolute_bucket
                    = checkedAdd(state.cursor.current_bucket, state.bucket_search_offset, "verification bucket offset overflows UInt64");
                const UInt32 candidate_bucket = static_cast<UInt32>(absolute_bucket % state.policy.bucket_count);
                if (state.first_in_bucket[candidate_bucket] != AuthorityVerificationPlanningContinuation::Impl::no_index)
                {
                    state.rotation_index = state.first_in_bucket[candidate_bucket];
                    state.rotation_bucket = candidate_bucket;
                    state.rotation_bucket_offset = state.bucket_search_offset;
                    state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::PrepareSelection;
                }
                ++state.bucket_search_offset;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::PrepareSelection: {
                if (!take_work_item())
                    return make_in_progress_result();
                state.next_cursor = state.cursor;
                if (checkedAdd(state.cursor.current_bucket, state.rotation_bucket_offset, "verification bucket offset overflows UInt64")
                    >= state.policy.bucket_count)
                    incrementChecked(state.next_cursor.completed_rotations, "authority verification rotation counter overflows UInt64");
                state.next_cursor.current_bucket = state.rotation_bucket;
                state.next_cursor.resume_after.reset();

                const UInt64 priority_scratch_items = checkedAdd(
                    checkedAdd(
                        state.policy.maximum_recent_targets_per_batch,
                        state.policy.maximum_high_dependency_targets_per_batch,
                        "authority verification priority scratch count overflows UInt64"),
                    state.policy.random_targets_per_batch,
                    "authority verification priority scratch count overflows UInt64");
                schedule_budget.chargePlannerScratch(checkedMultiply(
                    priority_scratch_items, sizeof(RandomCandidate), "authority verification priority scratch bytes overflow UInt64"));
                schedule_budget.chargePlannerScratch(checkedMultiply(
                    state.limits.maximum_targets_per_batch,
                    sizeof(size_t),
                    "authority verification selected-index scratch bytes overflow UInt64"));
                schedule_budget.chargePlannerScratch(checkedMultiply(
                    state.limits.maximum_targets_per_batch,
                    sizeof(ScheduledAuthorityVerificationTarget),
                    "authority verification selected-target scratch bytes overflow UInt64"));
                state.recent_candidates.reserve(
                    toSize(state.policy.maximum_recent_targets_per_batch, "recent-target reserve exceeds size_t"));
                state.high_dependency_candidates.reserve(
                    toSize(state.policy.maximum_high_dependency_targets_per_batch, "high-dependency-target reserve exceeds size_t"));
                state.random_candidates.reserve(toSize(state.policy.random_targets_per_batch, "random-target reserve exceeds size_t"));
                state.selected_indices.reserve(toSize(state.limits.maximum_targets_per_batch, "selected-target reserve exceeds size_t"));
                state.selected.reserve(toSize(state.limits.maximum_targets_per_batch, "selected-target reserve exceeds size_t"));
                state.next_batch_sequence
                    = checkedAdd(state.cursor.planned_batches, 1, "authority verification batch counter overflows UInt64");
                state.index = 0;
                state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::SelectPriority;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::SelectPriority: {
                if (state.index == state.snapshot_size)
                {
                    state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::AddInitialRotation;
                    continue;
                }
                if (!take_work_item())
                    return make_in_progress_result();
                schedule_budget.chargePlannerWork(1);
                ++state.statistics.targets_examined;
                if (state.target_buckets[state.index] != state.rotation_bucket)
                {
                    const auto & target = sorted_unique_targets[state.index];
                    if (isRecent(target, state.root, state.policy))
                    {
                        insertBoundedCandidate(
                            state.recent_candidates,
                            state.index,
                            state.policy.maximum_recent_targets_per_batch,
                            [&](size_t lhs, size_t rhs) { return recentCandidateLess(lhs, rhs, sorted_unique_targets); },
                            schedule_budget);
                    }
                    if (target.reverse_dependency_count >= state.policy.high_dependency_threshold)
                    {
                        insertBoundedCandidate(
                            state.high_dependency_candidates,
                            state.index,
                            state.policy.maximum_high_dependency_targets_per_batch,
                            [&](size_t lhs, size_t rhs) { return highDependencyCandidateLess(lhs, rhs, sorted_unique_targets); },
                            schedule_budget);
                    }
                    insertBoundedCandidate(
                        state.random_candidates,
                        RandomCandidate{
                            state.index, randomCandidateRank(state.root, target.leaf.key, state.policy, state.next_batch_sequence)},
                        state.policy.random_targets_per_batch,
                        [&](const RandomCandidate & lhs, const RandomCandidate & rhs)
                        { return randomCandidateLess(lhs, rhs, sorted_unique_targets); },
                        schedule_budget);
                }
                ++state.index;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::AddInitialRotation: {
                if (!take_work_item())
                    return make_in_progress_result();
                addScheduledTarget(
                    state.rotation_index,
                    AuthorityVerificationSelectionReason::Rotation,
                    true,
                    sorted_unique_targets,
                    state.selected_indices,
                    state.selected,
                    schedule_budget,
                    state.statistics);
                state.last_rotation_index = state.rotation_index;
                state.index = 0;
                state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::AddRecent;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::AddRecent: {
                if (state.index == state.recent_candidates.size())
                {
                    state.index = 0;
                    state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::AddHighDependency;
                    continue;
                }
                if (!take_work_item())
                    return make_in_progress_result();
                addScheduledTarget(
                    state.recent_candidates[state.index++],
                    AuthorityVerificationSelectionReason::RecentlyChanged,
                    false,
                    sorted_unique_targets,
                    state.selected_indices,
                    state.selected,
                    schedule_budget,
                    state.statistics);
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::AddHighDependency: {
                if (state.index == state.high_dependency_candidates.size())
                {
                    state.index = 0;
                    state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::AddRandom;
                    continue;
                }
                if (!take_work_item())
                    return make_in_progress_result();
                addScheduledTarget(
                    state.high_dependency_candidates[state.index++],
                    AuthorityVerificationSelectionReason::HighDependency,
                    false,
                    sorted_unique_targets,
                    state.selected_indices,
                    state.selected,
                    schedule_budget,
                    state.statistics);
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::AddRandom: {
                if (state.index == state.random_candidates.size())
                {
                    state.index = state.rotation_index + 1;
                    state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::ScanRotation;
                    continue;
                }
                if (!take_work_item())
                    return make_in_progress_result();
                addScheduledTarget(
                    state.random_candidates[state.index++].index,
                    AuthorityVerificationSelectionReason::OutsideBucketSample,
                    false,
                    sorted_unique_targets,
                    state.selected_indices,
                    state.selected,
                    schedule_budget,
                    state.statistics);
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::ScanRotation: {
                if (state.index == state.snapshot_size)
                {
                    state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::Finish;
                    continue;
                }
                if (!take_work_item())
                    return make_in_progress_result();
                schedule_budget.chargePlannerWork(1);
                if (state.target_buckets[state.index] == state.rotation_bucket)
                {
                    if (!schedule_budget.canAdmitTarget(sorted_unique_targets[state.index].cost, toUInt64(state.selected.size())))
                    {
                        state.next_rotation_index = state.index;
                        state.phase = AuthorityVerificationPlanningContinuation::Impl::Phase::Finish;
                        continue;
                    }
                    addScheduledTarget(
                        state.index,
                        AuthorityVerificationSelectionReason::Rotation,
                        true,
                        sorted_unique_targets,
                        state.selected_indices,
                        state.selected,
                        schedule_budget,
                        state.statistics);
                    state.last_rotation_index = state.index;
                }
                ++state.index;
                continue;
            }
            case AuthorityVerificationPlanningContinuation::Impl::Phase::Finish: {
                if (!take_work_item())
                    return make_in_progress_result();
                if (state.next_rotation_index != AuthorityVerificationPlanningContinuation::Impl::no_index)
                {
                    state.next_cursor.current_bucket = state.rotation_bucket;
                    state.next_cursor.resume_after = sorted_unique_targets[state.last_rotation_index].leaf.key;
                }
                else
                {
                    const UInt32 following_bucket
                        = static_cast<UInt32>((static_cast<UInt64>(state.rotation_bucket) + 1) % state.policy.bucket_count);
                    if (following_bucket == 0)
                        incrementChecked(state.next_cursor.completed_rotations, "authority verification rotation counter overflows UInt64");
                    state.next_cursor.current_bucket = following_bucket;
                    state.next_cursor.resume_after.reset();
                }
                state.next_cursor.planned_batches = state.next_batch_sequence;
                auto plan = AuthorityVerificationBatchPlan::Ptr(new AuthorityVerificationBatchPlan(
                    state.root,
                    state.target_set_digest,
                    AuthorityVerificationScheduleStatus::Scheduled,
                    state.cursor,
                    std::move(state.next_cursor),
                    std::move(state.selected),
                    state.statistics));
                continuation.reset();
                return {
                    .status = AuthorityVerificationPlanningStatus::Complete,
                    .plan = std::move(plan),
                    .consumed_work_items = pass_work_items,
                };
            }
        }
    }
}

AuthorityRepairReverificationBatchPlanningResult planNextAuthorityRepairReverificationBatch(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    UInt64 begin_target,
    const AuthorityVerificationScheduleCursor & retry_cursor,
    const AuthorityVerificationScheduleLimits & limits,
    const AuthorityVerificationPassBudget & pass_budget)
{
    if (pass_budget.maximum_work_items == 0)
        fail(ScheduleError::Code::InvalidConfiguration, "repair re-verification planning pass budget is zero");
    if (verificationPassBudgetExpired(pass_budget))
        return {};

    validateLimits(limits);
    const AuthorityRootIdentity root = validateRoot(authority);
    const auto pinned_inventory = pinAndValidateTargetSet(authority, sorted_unique_targets, limits);
    const UInt64 target_count = toUInt64(sorted_unique_targets.size());
    if (begin_target > target_count || !std::in_range<size_t>(begin_target))
        fail(ScheduleError::Code::InvalidTarget, "repair re-verification target offset is invalid");
    if (retry_cursor.contract_abi != authority_verification_schedule_contract_abi || retry_cursor.database_uuid != root.database_uuid
        || retry_cursor.bucket_count != 1 || retry_cursor.bucket_seed != 0 || retry_cursor.current_bucket != 0
        || retry_cursor.planned_batches == std::numeric_limits<UInt64>::max())
        fail(ScheduleError::Code::InvalidCursor, "repair re-verification cursor is invalid");

    const size_t begin = static_cast<size_t>(begin_target);
    if (begin == sorted_unique_targets.size())
    {
        if (retry_cursor.completed_rotations != 1 || retry_cursor.resume_after)
            fail(ScheduleError::Code::InvalidCursor, "complete repair re-verification has an incomplete cursor");
        return {
            .status = AuthorityRepairReverificationBatchPlanningStatus::Complete,
            .plan = {},
            .next_cursor = retry_cursor,
            .next_target = begin_target,
        };
    }
    if (retry_cursor.completed_rotations != 0 || (begin == 0 && retry_cursor.resume_after)
        || (begin != 0 && (!retry_cursor.resume_after || *retry_cursor.resume_after != sorted_unique_targets[begin - 1].leaf.key)))
    {
        fail(ScheduleError::Code::InvalidCursor, "repair re-verification cursor does not match its target offset");
    }

    UInt64 pass_work_items = 0;
    const auto take_work_item = [&]()
    {
        if (pass_work_items >= pass_budget.maximum_work_items || verificationPassBudgetExpired(pass_budget))
            return false;
        ++pass_work_items;
        return true;
    };
    if (!take_work_item())
        return {};

    AuthorityVerificationScheduleStatistics statistics;
    statistics.snapshot_targets = target_count;
    statistics.maximum_batches_per_full_rotation = target_count;
    ScheduleBudget budget(limits, statistics);
    budget.chargePlannerWork(8);
    budget.chargeRetained(schedule_plan_base_canonical_bytes);
    budget.chargePlannerScratch(checkedMultiply(
        std::min<UInt64>(limits.maximum_targets_per_batch, target_count - begin_target),
        sizeof(ScheduledAuthorityVerificationTarget),
        "repair re-verification selected-target scratch overflows UInt64"));

    const auto can_fit
        = [&](const AuthorityVerificationTargetCost & accumulated, UInt64 accumulated_targets, const AuthorityVerificationTargetCost & next)
    {
        if (accumulated_targets == 0 && next.canonical_bytes <= limits.maximum_rooted_target_canonical_bytes
            && next.work_units <= limits.maximum_rooted_target_verification_work_units
            && next.transient_bytes <= limits.maximum_rooted_target_transient_bytes
            && next.io_bytes <= limits.maximum_rooted_target_io_bytes)
            return true;
        return accumulated_targets < limits.maximum_targets_per_batch
            && accumulated.canonical_bytes <= limits.maximum_canonical_bytes_per_batch
            && accumulated.work_units <= limits.maximum_verification_work_units_per_batch
            && accumulated.transient_bytes <= limits.maximum_transient_bytes_per_batch
            && accumulated.io_bytes <= limits.maximum_io_bytes_per_batch
            && next.canonical_bytes <= limits.maximum_canonical_bytes_per_batch - accumulated.canonical_bytes
            && next.work_units <= limits.maximum_verification_work_units_per_batch - accumulated.work_units
            && next.transient_bytes <= limits.maximum_transient_bytes_per_batch
            && next.io_bytes <= limits.maximum_io_bytes_per_batch - accumulated.io_bytes;
    };
    const auto add_cost = [](AuthorityVerificationTargetCost & accumulated, const AuthorityVerificationTargetCost & next)
    {
        accumulated.canonical_bytes += next.canonical_bytes;
        accumulated.work_units += next.work_units;
        accumulated.transient_bytes = std::max(accumulated.transient_bytes, next.transient_bytes);
        accumulated.io_bytes += next.io_bytes;
    };

    AuthorityVerificationTargetCost accumulated;
    std::vector<ScheduledAuthorityVerificationTarget> selected;
    selected.reserve(static_cast<size_t>(std::min<UInt64>(limits.maximum_targets_per_batch, target_count - begin_target)));
    size_t end = begin;
    while (end < sorted_unique_targets.size())
    {
        /// The first target shares the already charged setup item. Every later
        /// target consumes one additional cooperative item before inspection.
        if (!selected.empty() && !take_work_item())
            break;
        const auto & target = sorted_unique_targets[end];
        if (!can_fit(accumulated, toUInt64(selected.size()), target.cost))
        {
            if (selected.empty())
                fail(ScheduleError::Code::UnschedulableRotationTarget, "repair re-verification target cannot fit an empty batch");
            break;
        }
        budget.chargePlannerWork(1);
        validateTarget(*pinned_inventory, root, sorted_unique_targets, end, limits, budget);
        budget.chargeTarget(target.cost, selected.empty());
        selected.push_back({
            .leaf = target.leaf,
            .cost = target.cost,
            .reasons = authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::Rotation),
        });
        add_cost(accumulated, target.cost);
        ++statistics.targets_examined;
        ++statistics.rotation_targets;
        ++end;
    }
    if (selected.empty())
    {
        return {
            .status = AuthorityRepairReverificationBatchPlanningStatus::InProgress,
            .plan = {},
            .next_cursor = retry_cursor,
            .next_target = begin_target,
            .consumed_work_items = pass_work_items,
        };
    }

    AuthorityVerificationScheduleCursor next_cursor = retry_cursor;
    incrementChecked(next_cursor.planned_batches, "repair re-verification batch sequence overflows UInt64");
    if (end == sorted_unique_targets.size())
    {
        next_cursor.resume_after.reset();
        next_cursor.completed_rotations = 1;
    }
    else
    {
        next_cursor.resume_after = sorted_unique_targets[end - 1].leaf.key;
    }
    auto plan = AuthorityVerificationBatchPlan::Ptr(new AuthorityVerificationBatchPlan(
        root,
        computeAuthorityVerificationTargetSetDigest(authority.getInventorySummary()),
        AuthorityVerificationScheduleStatus::Scheduled,
        retry_cursor,
        next_cursor,
        std::move(selected),
        statistics));
    return {
        .status = AuthorityRepairReverificationBatchPlanningStatus::Scheduled,
        .plan = std::move(plan),
        .next_cursor = std::move(next_cursor),
        .next_target = static_cast<UInt64>(end),
        .consumed_work_items = pass_work_items,
    };
}

std::vector<AuthorityVerificationBatchPlan::Ptr> planCompleteAuthorityRepairReverification(
    const AuthorityRoot & authority,
    std::span<const AuthorityVerificationTarget> sorted_unique_targets,
    const AuthorityVerificationScheduleLimits & limits)
{
    validateLimits(limits);
    const AuthorityRootIdentity root = validateRoot(authority);
    const UInt64 target_count = toUInt64(sorted_unique_targets.size());
    const Digest target_set_digest = computeAuthorityVerificationTargetSetDigest(authority.getInventorySummary());

    AuthorityVerificationScheduleStatistics validation_statistics;
    ScheduleBudget validation_budget(limits, validation_statistics);
    validation_budget.chargePlannerWork(checkedAdd(8, target_count, "complete repair verification planner work overflows UInt64"));
    validateTargets(authority, root, sorted_unique_targets, limits, validation_budget);
    validation_statistics.snapshot_targets = target_count;
    validation_statistics.targets_examined = target_count;

    AuthorityVerificationScheduleCursor initial_cursor;
    initial_cursor.database_uuid = root.database_uuid;
    initial_cursor.bucket_count = 1;

    if (sorted_unique_targets.empty())
    {
        validation_budget.chargeRetained(schedule_plan_base_canonical_bytes);
        std::vector<AuthorityVerificationBatchPlan::Ptr> empty;
        empty.reserve(1);
        empty.emplace_back(new AuthorityVerificationBatchPlan(
            root,
            target_set_digest,
            AuthorityVerificationScheduleStatus::EmptySnapshot,
            initial_cursor,
            initial_cursor,
            {},
            validation_statistics));
        return empty;
    }

    const auto can_fit
        = [&](const AuthorityVerificationTargetCost & accumulated, UInt64 accumulated_targets, const AuthorityVerificationTargetCost & next)
    {
        if (accumulated_targets == 0 && next.canonical_bytes <= limits.maximum_rooted_target_canonical_bytes
            && next.work_units <= limits.maximum_rooted_target_verification_work_units
            && next.transient_bytes <= limits.maximum_rooted_target_transient_bytes
            && next.io_bytes <= limits.maximum_rooted_target_io_bytes)
            return true;
        return accumulated_targets < limits.maximum_targets_per_batch
            && accumulated.canonical_bytes <= limits.maximum_canonical_bytes_per_batch
            && accumulated.work_units <= limits.maximum_verification_work_units_per_batch
            && accumulated.transient_bytes <= limits.maximum_transient_bytes_per_batch
            && accumulated.io_bytes <= limits.maximum_io_bytes_per_batch
            && next.canonical_bytes <= limits.maximum_canonical_bytes_per_batch - accumulated.canonical_bytes
            && next.work_units <= limits.maximum_verification_work_units_per_batch - accumulated.work_units
            && next.transient_bytes <= limits.maximum_transient_bytes_per_batch
            && next.io_bytes <= limits.maximum_io_bytes_per_batch - accumulated.io_bytes;
    };
    const auto add_cost = [](AuthorityVerificationTargetCost & accumulated, const AuthorityVerificationTargetCost & next)
    {
        accumulated.canonical_bytes += next.canonical_bytes;
        accumulated.work_units += next.work_units;
        accumulated.transient_bytes = std::max(accumulated.transient_bytes, next.transient_bytes);
        accumulated.io_bytes += next.io_bytes;
    };

    UInt64 batch_count = 1;
    UInt64 batch_targets = 0;
    AuthorityVerificationTargetCost batch_cost;
    for (const auto & target : sorted_unique_targets)
    {
        if (batch_targets != 0 && !can_fit(batch_cost, batch_targets, target.cost))
        {
            incrementChecked(batch_count, "complete repair verification batch count overflows UInt64");
            batch_targets = 0;
            batch_cost = {};
        }
        if (!can_fit(batch_cost, batch_targets, target.cost))
            fail(ScheduleError::Code::UnschedulableRotationTarget, "complete repair verification target cannot fit an empty batch");
        add_cost(batch_cost, target.cost);
        ++batch_targets;
    }

    const UInt64 retained = checkedAdd(
        checkedMultiply(batch_count, schedule_plan_base_canonical_bytes, "complete repair verification plan bytes overflow UInt64"),
        checkedMultiply(target_count, scheduled_target_canonical_bytes, "complete repair verification target bytes overflow UInt64"),
        "complete repair verification retained bytes overflow UInt64");
    if (retained > limits.maximum_retained_canonical_bytes)
        fail(ScheduleError::Code::LimitExceeded, "complete repair verification plans exceed their aggregate retained-byte limit");
    validation_budget.chargePlannerScratch(checkedMultiply(
        batch_count, sizeof(AuthorityVerificationBatchPlan::Ptr), "complete repair verification plan-vector bytes overflow UInt64"));

    std::vector<AuthorityVerificationBatchPlan::Ptr> result;
    result.reserve(toSize(batch_count, "complete repair verification batch count exceeds size_t"));
    size_t begin = 0;
    AuthorityVerificationScheduleCursor retry_cursor = initial_cursor;
    for (UInt64 batch_index = 0; batch_index < batch_count; ++batch_index)
    {
        AuthorityVerificationScheduleStatistics statistics;
        if (batch_index == 0)
            statistics = validation_statistics;
        statistics.snapshot_targets = target_count;
        statistics.maximum_batches_per_full_rotation = batch_count;
        ScheduleBudget budget(limits, statistics);
        budget.chargeRetained(schedule_plan_base_canonical_bytes);

        AuthorityVerificationTargetCost accumulated;
        std::vector<ScheduledAuthorityVerificationTarget> selected;
        selected.reserve(toSize(limits.maximum_targets_per_batch, "complete repair verification target reserve exceeds size_t"));
        size_t end = begin;
        while (end < sorted_unique_targets.size() && can_fit(accumulated, toUInt64(selected.size()), sorted_unique_targets[end].cost))
        {
            const auto & target = sorted_unique_targets[end];
            budget.chargeTarget(target.cost, selected.empty());
            selected.push_back({
                .leaf = target.leaf,
                .cost = target.cost,
                .reasons = authorityVerificationSelectionReasonMask(AuthorityVerificationSelectionReason::Rotation),
            });
            add_cost(accumulated, target.cost);
            ++statistics.rotation_targets;
            ++end;
        }
        if (selected.empty())
            fail(ScheduleError::Code::UnschedulableRotationTarget, "complete repair verification produced an empty nonterminal batch");

        AuthorityVerificationScheduleCursor next_cursor = retry_cursor;
        incrementChecked(next_cursor.planned_batches, "complete repair verification batch sequence overflows UInt64");
        if (end == sorted_unique_targets.size())
        {
            next_cursor.resume_after.reset();
            next_cursor.completed_rotations = 1;
        }
        else
        {
            next_cursor.resume_after = sorted_unique_targets[end - 1].leaf.key;
        }
        result.emplace_back(new AuthorityVerificationBatchPlan(
            root,
            target_set_digest,
            AuthorityVerificationScheduleStatus::Scheduled,
            retry_cursor,
            next_cursor,
            std::move(selected),
            statistics));
        retry_cursor = std::move(next_cursor);
        begin = end;
    }
    if (begin != sorted_unique_targets.size() || retry_cursor.completed_rotations != 1)
        fail(ScheduleError::Code::InvalidTarget, "complete repair verification did not cover its exact target set once");
    return result;
}

AuthorityVerificationCursorDecision
finalizePeriodicAuthorityVerificationBatch(const AuthorityVerificationBatchPlan & plan, const AuthorityVerificationBatchReceipt & receipt)
{
    if (receipt.root != plan.root || receipt.target_set_digest != plan.target_set_digest || receipt.charge_abi != plan.charge_abi
        || plan.charge_abi != authority_verification_charge_abi)
        return {AuthorityVerificationCursorDecisionStatus::RetryRootChanged, plan.retry_cursor};
    if (receipt.retry_cursor != plan.retry_cursor || receipt.planned_target_count != toUInt64(plan.targets.size())
        || receipt.terminal_completions.size() != plan.targets.size())
        return {AuthorityVerificationCursorDecisionStatus::RetryIncompleteRotation, plan.retry_cursor};

    AuthorityVerificationTargetCost completed_cost;
    for (size_t index = 0; index < plan.targets.size(); ++index)
    {
        const auto & completion = receipt.terminal_completions[index];
        const auto & scheduled = plan.targets[index];
        if (completion.leaf != scheduled.leaf || completion.disposition != AuthorityVerificationTargetDisposition::Verified
            || !costFits(completion.actual_charged_cost, scheduled.cost))
            return {AuthorityVerificationCursorDecisionStatus::RetryIncompleteRotation, plan.retry_cursor};
        completed_cost = aggregateCosts(completed_cost, completion.actual_charged_cost);
    }
    if (completed_cost != receipt.actual_charged_cost)
        return {AuthorityVerificationCursorDecisionStatus::RetryIncompleteRotation, plan.retry_cursor};
    if (plan.status == AuthorityVerificationScheduleStatus::EmptySnapshot)
        return {AuthorityVerificationCursorDecisionStatus::NoWork, plan.retry_cursor};
    return {AuthorityVerificationCursorDecisionStatus::Advanced, plan.next_cursor};
}

}
