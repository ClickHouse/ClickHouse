#include <DataTypes/UDT/ResourceAccounting.h>

#include <fmt/format.h>

#include <algorithm>
#include <exception>
#include <limits>
#include <set>
#include <tuple>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr std::size_t indexOf(ResourceLimit limit) noexcept
{
    return static_cast<std::size_t>(limit);
}

std::size_t checkedIndexOf(ResourceLimit limit) noexcept
{
    const auto index = indexOf(limit);
    if (index >= resource_limit_count)
        std::terminate();
    return index;
}

bool checkedAdd(UInt64 lhs, UInt64 rhs, UInt64 & result) noexcept
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        return false;
    result = lhs + rhs;
    return true;
}

bool checkedMultiply(UInt64 lhs, UInt64 rhs, UInt64 & result) noexcept
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        return false;
    result = lhs * rhs;
    return true;
}

bool isOverQuota(const ResourceUsage & usage, const EffectiveResourceLimits & limits) noexcept
{
    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        if (usage.get(limit) > limits.get(limit))
            return true;
    }
    return false;
}

ResourceAdmissionResult admitCurrentUsageStrictly(const ResourceUsage & usage, const EffectiveResourceLimits & limits) noexcept
{
    ResourceAdmissionResult result;
    result.current_state = getQuotaState(usage, limits);
    result.prospective_state = result.current_state;
    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const UInt64 current = usage.get(limit);
        const UInt64 maximum = limits.get(limit);
        if (current <= maximum)
            continue;
        result.status = ResourceAdmissionStatus::LimitExceeded;
        result.limit = limit;
        result.current = current;
        result.prospective = current;
        result.maximum = maximum;
        return result;
    }
    return result;
}

DeterministicCatalogChargeResult
failedCatalogCharge(DeterministicCatalogChargeStatus status, DeterministicCatalogChargeComponent component, UInt64 maximum_bytes) noexcept
{
    return {
        .status = status,
        .component = component,
        .charged_bytes = 0,
        .maximum_bytes = maximum_bytes,
    };
}

}

struct QueryResourceLedger::Impl
{
    struct DescriptorKey
    {
        DefinitionIdentity identity;
        String canonical_arguments;
    };

    struct DescriptorKeyView
    {
        const DefinitionIdentity & identity;
        std::string_view canonical_arguments;
    };

    struct DescriptorKeyLess
    {
        using is_transparent = void;

        static bool identityLess(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
        {
            return std::tie(lhs.database_uuid, lhs.type_uuid, lhs.revision) < std::tie(rhs.database_uuid, rhs.type_uuid, rhs.revision);
        }

        static bool less(
            const DefinitionIdentity & lhs_identity,
            std::string_view lhs_arguments,
            const DefinitionIdentity & rhs_identity,
            std::string_view rhs_arguments) noexcept
        {
            if (identityLess(lhs_identity, rhs_identity))
                return true;
            if (identityLess(rhs_identity, lhs_identity))
                return false;
            return lhs_arguments < rhs_arguments;
        }

        bool operator()(const DescriptorKey & lhs, const DescriptorKey & rhs) const noexcept
        {
            return less(lhs.identity, lhs.canonical_arguments, rhs.identity, rhs.canonical_arguments);
        }

        bool operator()(const DescriptorKey & lhs, DescriptorKeyView rhs) const noexcept
        {
            return less(lhs.identity, lhs.canonical_arguments, rhs.identity, rhs.canonical_arguments);
        }

        bool operator()(DescriptorKeyView lhs, const DescriptorKey & rhs) const noexcept
        {
            return less(lhs.identity, lhs.canonical_arguments, rhs.identity, rhs.canonical_arguments);
        }
    };

    ResourceUsage usage;
    std::set<DescriptorKey, DescriptorKeyLess> descriptors;
    std::optional<EffectiveResourceLimits> attached_query_limits;
    UInt64 limits_generation = 0;
};

QueryResourceLedger::QueryResourceLedger()
    : impl(std::make_unique<Impl>())
{
}

QueryResourceLedger::~QueryResourceLedger() = default;

ResourceAdmissionResult QueryResourceLedger::admitCurrentUsage(const EffectiveResourceLimits & limits)
{
    auto joined_limits = impl->attached_query_limits ? minimumEffectiveResourceLimits(*impl->attached_query_limits, limits) : limits;
    auto admission = admitCurrentUsageStrictly(impl->usage, joined_limits);
    if (admission.isAccepted())
    {
        if (!impl->attached_query_limits || *impl->attached_query_limits != joined_limits)
        {
            if (impl->limits_generation == std::numeric_limits<UInt64>::max())
            {
                admission.status = ResourceAdmissionStatus::ArithmeticOverflow;
                return admission;
            }
            impl->attached_query_limits.emplace(std::move(joined_limits));
            ++impl->limits_generation;
        }
    }
    return admission;
}

ResourceAdmissionResult QueryResourceLedger::charge(const ResourceDelta & delta, const EffectiveResourceLimits & limits) noexcept
{
    return tryApplyResourceDelta(impl->usage, delta, limits);
}

ResourceAdmissionResult QueryResourceLedger::chargeDistinctDescriptor(
    const DefinitionIdentity & identity, std::string_view canonical_arguments, const EffectiveResourceLimits & limits)
{
    static_assert(sizeof(std::size_t) <= sizeof(UInt64));
    const Impl::DescriptorKeyView view{identity, canonical_arguments};
    if (impl->descriptors.find(view) != impl->descriptors.end())
        return evaluateResourceAdmission(impl->usage, ResourceDelta{}, limits);

    ResourceDelta delta;
    delta.add(ResourceLimit::DemandedDescriptorsPerQuery, 1);
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, static_cast<UInt64>(sizeof(Impl::DescriptorKey)));
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, static_cast<UInt64>(canonical_arguments.size()));
    auto admission = charge(delta, limits);
    if (!admission.isAccepted())
        return admission;

    /// A later allocation failure deliberately does not refund a successful
    /// prospective charge: query/checker budgets are monotonic across failures.
    impl->descriptors.emplace(Impl::DescriptorKey{identity, String(canonical_arguments)});
    return admission;
}

EffectiveResourceLimits QueryResourceLedger::getApplicableLimits(const EffectiveResourceLimits & fallback) const noexcept
{
    return impl->attached_query_limits ? minimumEffectiveResourceLimits(*impl->attached_query_limits, fallback) : fallback;
}

UInt64 QueryResourceLedger::getLimitsGeneration() const noexcept
{
    return impl->limits_generation;
}

const ResourceUsage & QueryResourceLedger::getUsage() const noexcept
{
    return impl->usage;
}

UInt64 ResourceUsage::get(ResourceLimit limit) const noexcept
{
    return values[checkedIndexOf(limit)];
}

void ResourceUsage::set(ResourceLimit limit, UInt64 value) noexcept
{
    values[checkedIndexOf(limit)] = value;
}

ResourceDelta & ResourceDelta::add(ResourceLimit limit, UInt64 amount) noexcept
{
    const auto index = checkedIndexOf(limit);
    UInt64 result = 0;
    if (checkedAdd(additions[index], amount, result))
        additions[index] = result;
    else
        addition_overflow.set(index);
    return *this;
}

ResourceDelta & ResourceDelta::remove(ResourceLimit limit, UInt64 amount) noexcept
{
    const auto index = checkedIndexOf(limit);
    UInt64 result = 0;
    if (checkedAdd(removals[index], amount, result))
        removals[index] = result;
    else
        removal_overflow.set(index);
    return *this;
}

UInt64 ResourceDelta::getAddition(ResourceLimit limit) const noexcept
{
    return additions[checkedIndexOf(limit)];
}

UInt64 ResourceDelta::getRemoval(ResourceLimit limit) const noexcept
{
    return removals[checkedIndexOf(limit)];
}

bool ResourceDelta::additionOverflowed(ResourceLimit limit) const noexcept
{
    return addition_overflow.test(checkedIndexOf(limit));
}

bool ResourceDelta::removalOverflowed(ResourceLimit limit) const noexcept
{
    return removal_overflow.test(checkedIndexOf(limit));
}

QuotaState getQuotaState(const ResourceUsage & usage, const EffectiveResourceLimits & limits) noexcept
{
    return isOverQuota(usage, limits) ? QuotaState::OverQuota : QuotaState::WithinQuota;
}

ResourceAdmissionResult evaluateResourceAdmission(
    const ResourceUsage & current,
    const ResourceDelta & delta,
    const EffectiveResourceLimits & limits,
    ResourceUsage * prospective_usage) noexcept
{
    ResourceAdmissionResult result;
    result.current_state = getQuotaState(current, limits);
    result.prospective_state = result.current_state;

    ResourceUsage prospective;
    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const UInt64 current_value = current.get(limit);
        const UInt64 removal = delta.getRemoval(limit);
        const UInt64 addition = delta.getAddition(limit);
        const UInt64 maximum = limits.get(limit);

        if (delta.additionOverflowed(limit) || delta.removalOverflowed(limit))
        {
            result.status = ResourceAdmissionStatus::ArithmeticOverflow;
            result.limit = limit;
            result.current = current_value;
            result.removal = removal;
            result.addition = addition;
            result.maximum = maximum;
            return result;
        }

        if (removal > current_value)
        {
            result.status = ResourceAdmissionStatus::InvalidRemoval;
            result.limit = limit;
            result.current = current_value;
            result.removal = removal;
            result.addition = addition;
            result.maximum = maximum;
            return result;
        }

        UInt64 prospective_value = 0;
        if (!checkedAdd(current_value - removal, addition, prospective_value))
        {
            result.status = ResourceAdmissionStatus::ArithmeticOverflow;
            result.limit = limit;
            result.current = current_value;
            result.removal = removal;
            result.addition = addition;
            result.maximum = maximum;
            return result;
        }
        prospective.set(limit, prospective_value);
    }

    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const UInt64 current_value = current.get(limit);
        const UInt64 prospective_value = prospective.get(limit);
        const UInt64 maximum = limits.get(limit);
        const bool currently_over_quota = current_value > maximum;
        const bool rejected = currently_over_quota ? prospective_value > current_value : prospective_value > maximum;
        if (rejected)
        {
            result.status = ResourceAdmissionStatus::LimitExceeded;
            result.prospective_state = QuotaState::OverQuota;
            result.limit = limit;
            result.current = current_value;
            result.removal = delta.getRemoval(limit);
            result.addition = delta.getAddition(limit);
            result.prospective = prospective_value;
            result.maximum = maximum;
            return result;
        }
    }

    result.prospective_state = getQuotaState(prospective, limits);
    if (prospective_usage)
        *prospective_usage = prospective;
    return result;
}

ResourceAdmissionResult
tryApplyResourceDelta(ResourceUsage & current, const ResourceDelta & delta, const EffectiveResourceLimits & limits) noexcept
{
    ResourceUsage prospective;
    auto result = evaluateResourceAdmission(current, delta, limits, &prospective);
    if (result.isAccepted())
        current = prospective;
    return result;
}

ProspectiveResourceBudget::ProspectiveResourceBudget(EffectiveResourceLimits limits_)
    : limits(std::move(limits_))
    , query_ledger(std::make_shared<QueryResourceLedger>())
{
}

ProspectiveResourceBudget::ProspectiveResourceBudget(EffectiveResourceLimits limits_, std::shared_ptr<QueryResourceLedger> query_ledger_)
    : limits(std::move(limits_))
    , query_ledger(std::move(query_ledger_))
{
    if (!query_ledger)
        throw ResourceLimitError(ResourceLimitError::Code::InvalidLimit, "a prospective resource budget has no query ledger");
}

ResourceAdmissionResult ProspectiveResourceBudget::charge(ResourceLimit limit, UInt64 amount) noexcept
{
    ResourceDelta delta;
    delta.add(limit, amount);
    return charge(delta);
}

ResourceAdmissionResult ProspectiveResourceBudget::charge(const ResourceDelta & delta) noexcept
{
    refreshApplicableLimits();
    return query_ledger->charge(delta, limits);
}

ResourceAdmissionResult
ProspectiveResourceBudget::chargeDistinctDescriptor(const DefinitionIdentity & identity, std::string_view canonical_arguments)
{
    refreshApplicableLimits();
    return query_ledger->chargeDistinctDescriptor(identity, canonical_arguments, limits);
}

ResourceAdmissionResult ProspectiveResourceBudget::admitCurrentUsage()
{
    auto admission = query_ledger->admitCurrentUsage(limits);
    if (admission.isAccepted())
    {
        limits = query_ledger->getApplicableLimits(limits);
        cached_limits_generation = query_ledger->getLimitsGeneration();
    }
    return admission;
}

ResourceAdmissionResult ProspectiveResourceBudget::admitCurrentUsage(const EffectiveResourceLimits & additional_limits)
{
    const auto joined_limits = minimumEffectiveResourceLimits(limits, additional_limits);
    auto admission = query_ledger->admitCurrentUsage(joined_limits);
    if (admission.isAccepted())
    {
        limits = query_ledger->getApplicableLimits(joined_limits);
        cached_limits_generation = query_ledger->getLimitsGeneration();
    }
    return admission;
}

EffectiveResourceLimits ProspectiveResourceBudget::getLimits() const noexcept
{
    refreshApplicableLimits();
    return limits;
}

void ProspectiveResourceBudget::refreshApplicableLimits() const noexcept
{
    const UInt64 generation = query_ledger->getLimitsGeneration();
    if (generation == cached_limits_generation)
        return;
    limits = query_ledger->getApplicableLimits(limits);
    cached_limits_generation = generation;
}

const ResourceUsage & ProspectiveResourceBudget::getUsage() const noexcept
{
    return query_ledger->getUsage();
}

String formatResourceAdmissionFailure(const ResourceAdmissionResult & result)
{
    const auto status = [&]() -> std::string_view
    {
        switch (result.status)
        {
            case ResourceAdmissionStatus::Accepted: return "accepted";
            case ResourceAdmissionStatus::LimitExceeded: return "limit_exceeded";
            case ResourceAdmissionStatus::ArithmeticOverflow: return "arithmetic_overflow";
            case ResourceAdmissionStatus::InvalidRemoval: return "invalid_removal";
        }
        return "unknown";
    }();
    const auto limit = result.limit ? resourceLimitName(*result.limit) : std::string_view("unknown");
    return fmt::format(
        "UDT resource admission failed: status={}, limit={}, current={}, removal={}, addition={}, prospective={}, maximum={}",
        status,
        limit,
        result.current,
        result.removal,
        result.addition,
        result.prospective,
        result.maximum);
}

std::string_view deterministicCatalogChargeComponentName(DeterministicCatalogChargeComponent component) noexcept
{
    switch (component)
    {
        case DeterministicCatalogChargeComponent::CanonicalRecordBytes: return "canonical_record_bytes";
        case DeterministicCatalogChargeComponent::Definitions: return "definitions";
        case DeterministicCatalogChargeComponent::Specializations: return "specializations";
        case DeterministicCatalogChargeComponent::DependencyEdges: return "dependency_edges";
        case DeterministicCatalogChargeComponent::OccurrencePaths: return "occurrence_paths";
        case DeterministicCatalogChargeComponent::InventoryNodes: return "inventory_nodes";
        case DeterministicCatalogChargeComponent::InventoryLeaves: return "inventory_leaves";
    }
    return "unknown";
}

const DeterministicCatalogChargeRates & getDeterministicCatalogChargeRates() noexcept
{
    static constexpr DeterministicCatalogChargeRates rates;
    static_assert(
        rates.definition_bytes != 0 && rates.specialization_bytes != 0 && rates.dependency_edge_bytes != 0
        && rates.occurrence_path_bytes != 0 && rates.inventory_node_bytes != 0 && rates.inventory_leaf_bytes != 0);
    return rates;
}

DeterministicCatalogChargeResult
calculateDeterministicCatalogCharge(const DeterministicCatalogChargeInput & input, UInt64 maximum_bytes) noexcept
{
    if (maximum_bytes == 0)
    {
        return {
            .status = DeterministicCatalogChargeStatus::InvalidMaximum,
            .component = std::nullopt,
            .charged_bytes = 0,
            .maximum_bytes = 0,
        };
    }

    const auto & rates = getDeterministicCatalogChargeRates();
    UInt64 total = 0;

    const auto charge
        = [&](DeterministicCatalogChargeComponent component, UInt64 count, UInt64 rate) -> std::optional<DeterministicCatalogChargeResult>
    {
        UInt64 amount = 0;
        if (!checkedMultiply(count, rate, amount))
            return failedCatalogCharge(DeterministicCatalogChargeStatus::ArithmeticOverflow, component, maximum_bytes);
        UInt64 prospective = 0;
        if (!checkedAdd(total, amount, prospective))
            return failedCatalogCharge(DeterministicCatalogChargeStatus::ArithmeticOverflow, component, maximum_bytes);
        if (prospective > maximum_bytes)
            return failedCatalogCharge(DeterministicCatalogChargeStatus::LimitExceeded, component, maximum_bytes);
        total = prospective;
        return std::nullopt;
    };

    if (const auto failure = charge(DeterministicCatalogChargeComponent::CanonicalRecordBytes, input.canonical_record_bytes, 1))
        return *failure;
    if (const auto failure = charge(DeterministicCatalogChargeComponent::Definitions, input.definitions, rates.definition_bytes))
        return *failure;
    if (const auto failure
        = charge(DeterministicCatalogChargeComponent::Specializations, input.specializations, rates.specialization_bytes))
        return *failure;
    if (const auto failure
        = charge(DeterministicCatalogChargeComponent::DependencyEdges, input.dependency_edges, rates.dependency_edge_bytes))
        return *failure;
    if (const auto failure
        = charge(DeterministicCatalogChargeComponent::OccurrencePaths, input.occurrence_paths, rates.occurrence_path_bytes))
        return *failure;
    if (const auto failure = charge(DeterministicCatalogChargeComponent::InventoryNodes, input.inventory_nodes, rates.inventory_node_bytes))
        return *failure;
    if (const auto failure
        = charge(DeterministicCatalogChargeComponent::InventoryLeaves, input.inventory_leaves, rates.inventory_leaf_bytes))
        return *failure;

    return {
        .status = DeterministicCatalogChargeStatus::Accepted,
        .component = std::nullopt,
        .charged_bytes = total,
        .maximum_bytes = maximum_bytes,
    };
}

DeterministicCatalogChargeResult
calculateDeterministicCatalogCharge(const DeterministicCatalogChargeInput & input, const EffectiveResourceLimits & limits) noexcept
{
    return calculateDeterministicCatalogCharge(input, limits.get(ResourceLimit::DeterministicCatalogBytesPerDatabase));
}

static_assert(deterministic_catalog_charge_abi != 0);

}
