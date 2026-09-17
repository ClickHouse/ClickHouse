#pragma once

#include <DataTypes/UDT/ResourceAccounting.h>

#include <Core/Types.h>

#include <memory>

namespace DB::UDT
{

enum class DatabaseResourceQuotaState : UInt8
{
    Active,
    OverQuota,
};

/// Immutable database-local quota image. The fixed ResourceLimit identities
/// are retained one-for-one in both limits and usage; no aggregate counter can
/// mask another counter's ceiling.
class DatabaseResourceQuotaSnapshot final
{
public:
    using Ptr = std::shared_ptr<const DatabaseResourceQuotaSnapshot>;

    UInt64 getRevision() const noexcept { return revision; }
    DatabaseResourceQuotaState getState() const noexcept { return state; }
    const EffectiveResourceLimits & getLimits() const noexcept { return limits; }
    const ResourceUsage & getUsage() const noexcept { return usage; }
    static UInt64 getLogicalCharge() noexcept;

private:
    DatabaseResourceQuotaSnapshot(UInt64 revision_, EffectiveResourceLimits limits_, ResourceUsage usage_) noexcept;

    friend class DatabaseResourceQuotaTransitionBuilder;

    const UInt64 revision;
    const EffectiveResourceLimits limits;
    const ResourceUsage usage;
    const DatabaseResourceQuotaState state;
};

enum class DatabaseResourceQuotaPreparationStatus : UInt8
{
    Prepared,
    InvalidBase,
    RevisionOverflow,
    ResourceRejected,
};

/// Move-only prospective transition. It retains the exact immutable base image
/// and its checked replacement, but deliberately has no publication method.
class PreparedDatabaseResourceQuota final
{
public:
    PreparedDatabaseResourceQuota(PreparedDatabaseResourceQuota &&) noexcept = default;
    PreparedDatabaseResourceQuota & operator=(PreparedDatabaseResourceQuota &&) noexcept = default;

    PreparedDatabaseResourceQuota(const PreparedDatabaseResourceQuota &) = delete;
    PreparedDatabaseResourceQuota & operator=(const PreparedDatabaseResourceQuota &) = delete;

    DatabaseResourceQuotaPreparationStatus getStatus() const noexcept { return status; }
    bool isPrepared() const noexcept { return status == DatabaseResourceQuotaPreparationStatus::Prepared; }
    const ResourceAdmissionResult & getResourceAdmission() const noexcept { return resource_admission; }
    const DatabaseResourceQuotaSnapshot::Ptr & getBase() const noexcept { return base; }
    const DatabaseResourceQuotaSnapshot::Ptr & getReplacement() const noexcept { return replacement; }

private:
    PreparedDatabaseResourceQuota() = default;

    friend class DatabaseResourceQuotaTransitionBuilder;

    DatabaseResourceQuotaPreparationStatus status = DatabaseResourceQuotaPreparationStatus::InvalidBase;
    ResourceAdmissionResult resource_admission;
    DatabaseResourceQuotaSnapshot::Ptr base;
    DatabaseResourceQuotaSnapshot::Ptr replacement;
};

/// Feature-inert pure builder for one database quota image. It never owns a
/// mutable current pointer and intentionally exposes no standalone publication
/// API: the prepared image must be embedded in the next AuthorityRoot and
/// published by that root's existing AtomicAuthority transaction.
class DatabaseResourceQuotaTransitionBuilder final
{
public:
    [[nodiscard]] static DatabaseResourceQuotaSnapshot::Ptr
    makeInitial(EffectiveResourceLimits initial_limits, ResourceUsage initial_usage = {}, UInt64 initial_revision = 1);

    /// Applies `delta` against `next_limits` as one prospective replacement.
    /// If the base usage is over any next limit, every independently growing
    /// counter is rejected; neutral or shrinking counters remain admissible.
    [[nodiscard]] static PreparedDatabaseResourceQuota
    prepareReplacement(DatabaseResourceQuotaSnapshot::Ptr base, EffectiveResourceLimits next_limits, const ResourceDelta & delta);

private:
    DatabaseResourceQuotaTransitionBuilder() = delete;
};

}
