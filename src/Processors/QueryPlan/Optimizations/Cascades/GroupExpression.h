#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StepIdentity.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <base/types.h>
#include <memory>
#include <unordered_set>

namespace DB
{

class IOptimizationRule;
using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

/// Which physical property a self-referential enforcer adds; `None` for ordinary expressions. Lets
/// the input resolver stop an enforcer from satisfying its own input by over-providing on a
/// wildcard (empty sort / empty distribution columns), which would form a cycle. A sorted gather is a
/// `Distribution` enforcer even though it preserves sorting.
enum class EnforcedProperty : uint8_t
{
    None,
    Sorting,
    Distribution,
};

class GroupExpression final
{
public:
    /// Initial creation from the query plan (takes ownership via unique_ptr,
    /// then stored as shared_ptr<const> for sharing across GroupExpressions).
    explicit GroupExpression(QueryPlanStepPtr plan_step_)
        : plan_step(std::move(plan_step_))
    {}

    /// Shallow copy: shares the immutable plan_step, copies only metadata.
    /// Rules that need a different step assign a new one to `plan_step`.
    GroupExpression(const GroupExpression & other_)
        : group_id(other_.group_id)
        , plan_step(other_.plan_step)
        , strategy(other_.strategy)
        , description_suffix(other_.description_suffix)
        , inputs(other_.inputs)
        , enforced_property(other_.enforced_property)
        , cached_step_fingerprint(other_.cached_step_fingerprint)
    {}

    String getName() const;
    String getDescription() const;
    const IQueryPlanStep * getQueryPlanStep() const;
    bool isApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties) const;
    void setApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties);

    void dump(WriteBuffer & out, const CostConfig & cost_config) const;
    String dump(const CostConfig & cost_config) const;
    size_t fingerprint() const;

    /// Structural identity used to deduplicate physical expressions. Compares the same
    /// components the fingerprint hashes, so a fingerprint hash collision does not
    /// silently drop a distinct alternative.
    bool structurallyEqualTo(const GroupExpression & other) const;

    /// Content-based fingerprint of `plan_step`, or nullptr when the step type has not opted in.
    /// Recomputed when the cached entry was inherited from a copy whose step was then replaced.
    const StepFingerprint * cachedStepFingerprint() const;

    /// Cross-group identity, for deduplication across the whole memo. Stronger than the
    /// within-group `fingerprint` / `structurallyEqualTo` pair: it compares step content instead
    /// of step name and the step's display description. Fails closed - a step type that has not
    /// opted in compares by pointer, and every GroupExpression-side field that can change what
    /// the expression means is compared, including `enforced_property` and `description_suffix`.
    size_t fullFingerprint() const;
    bool fullyEqualTo(const GroupExpression & other) const;

    GroupId group_id = INVALID_GROUP_ID;
    std::shared_ptr<const IQueryPlanStep> plan_step;
    ImplementationStrategyPtr strategy;     /// Implementation strategy (nullptr = logical / default)
    String description_suffix;             /// Extra description set by rules (e.g., "(by col)" for single-key shuffle)

    struct Input
    {
        GroupId group_id = INVALID_GROUP_ID;
        ExpressionProperties required_properties;
    };

    std::vector<Input> inputs;

    ExpressionProperties properties;

    /// Non-`None` for self-referential enforcer expressions (see `EnforcedProperty`).
    EnforcedProperty enforced_property = EnforcedProperty::None;

    std::unordered_set<RulePropertiesKey, RulePropertiesKeyHash> applied_rules;

    std::optional<ExpressionCost> cost;

    /// Physical output row count when it differs from the group's logical statistics: a partial
    /// top-N emits up to L rows on each of its nodes while the group stats are trimmed to L.
    /// Set during costing (statistics are derived by then); parents price exchanges on it.
    std::optional<Float64> physical_output_rows;

private:
    /// Lazily computed by `cachedStepFingerprint`; shared with shallow copies, which is safe
    /// because it records the step it was computed from and is dropped when that step no longer
    /// matches. Mutated from a `const` getter with no synchronization - safe only because the
    /// Cascades optimizer runs single-threaded.
    mutable std::shared_ptr<const StepFingerprint> cached_step_fingerprint;
};

using GroupExpressionPtr = std::shared_ptr<GroupExpression>;

}
