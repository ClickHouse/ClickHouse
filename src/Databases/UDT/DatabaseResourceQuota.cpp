#include <Databases/UDT/DatabaseResourceQuota.h>

#include <limits>
#include <utility>

namespace DB::UDT
{
namespace
{

DatabaseResourceQuotaState toDatabaseState(QuotaState state) noexcept
{
    return state == QuotaState::WithinQuota ? DatabaseResourceQuotaState::Active : DatabaseResourceQuotaState::OverQuota;
}

ResourceAdmissionResult evaluateDatabaseResourceAdmission(
    const ResourceUsage & current,
    const ResourceDelta & delta,
    const EffectiveResourceLimits & limits,
    ResourceUsage & prospective) noexcept
{
    auto result = evaluateResourceAdmission(current, delta, limits, &prospective);
    if (!result.isAccepted() || result.current_state != QuotaState::OverQuota)
        return result;

    /// Database OVER_QUOTA is stricter than a query's independently exceeded
    /// work counters: no resource category may grow until the database returns
    /// to ACTIVE. Canonical enum order selects the deterministic first failure.
    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const UInt64 current_value = current.get(limit);
        const UInt64 prospective_value = prospective.get(limit);
        if (prospective_value <= current_value)
            continue;
        result.status = ResourceAdmissionStatus::LimitExceeded;
        result.prospective_state = getQuotaState(prospective, limits);
        result.limit = limit;
        result.current = current_value;
        result.removal = delta.getRemoval(limit);
        result.addition = delta.getAddition(limit);
        result.prospective = prospective_value;
        result.maximum = limits.get(limit);
        return result;
    }
    return result;
}

}

DatabaseResourceQuotaSnapshot::DatabaseResourceQuotaSnapshot(
    UInt64 revision_, EffectiveResourceLimits limits_, ResourceUsage usage_) noexcept
    : revision(revision_)
    , limits(std::move(limits_))
    , usage(std::move(usage_))
    , state(toDatabaseState(getQuotaState(usage, limits)))
{
}

UInt64 DatabaseResourceQuotaSnapshot::getLogicalCharge() noexcept
{
    return sizeof(DatabaseResourceQuotaSnapshot) + 2 * sizeof(void *);
}

DatabaseResourceQuotaSnapshot::Ptr DatabaseResourceQuotaTransitionBuilder::makeInitial(
    EffectiveResourceLimits initial_limits, ResourceUsage initial_usage, UInt64 initial_revision)
{
    if (initial_revision == 0)
        throw ResourceLimitError(ResourceLimitError::Code::InvalidLimit, "a database resource-quota revision must be nonzero");
    return DatabaseResourceQuotaSnapshot::Ptr(
        new DatabaseResourceQuotaSnapshot(initial_revision, std::move(initial_limits), std::move(initial_usage)));
}

PreparedDatabaseResourceQuota DatabaseResourceQuotaTransitionBuilder::prepareReplacement(
    DatabaseResourceQuotaSnapshot::Ptr base, EffectiveResourceLimits next_limits, const ResourceDelta & delta)
{
    PreparedDatabaseResourceQuota result;
    if (!base)
    {
        result.status = DatabaseResourceQuotaPreparationStatus::InvalidBase;
        return result;
    }
    result.base = std::move(base);
    if (result.base->getRevision() == std::numeric_limits<UInt64>::max())
    {
        result.status = DatabaseResourceQuotaPreparationStatus::RevisionOverflow;
        return result;
    }

    ResourceUsage prospective_usage;
    result.resource_admission = evaluateDatabaseResourceAdmission(result.base->getUsage(), delta, next_limits, prospective_usage);
    if (!result.resource_admission.isAccepted())
    {
        result.status = DatabaseResourceQuotaPreparationStatus::ResourceRejected;
        return result;
    }

    /// This is the first allocation in the transition. Every counter and the
    /// revision have already been checked, and no shared state was mutated.
    result.replacement = DatabaseResourceQuotaSnapshot::Ptr(
        new DatabaseResourceQuotaSnapshot(result.base->getRevision() + 1, std::move(next_limits), std::move(prospective_usage)));
    result.status = DatabaseResourceQuotaPreparationStatus::Prepared;
    return result;
}

}
