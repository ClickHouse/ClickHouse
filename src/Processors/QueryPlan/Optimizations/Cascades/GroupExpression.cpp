#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <IO/Operators.h>
#include <base/defines.h>
#include <Common/SipHash.h>
#include <boost/functional/hash.hpp>

namespace DB
{

String GroupExpression::getName() const
{
    if (plan_step)
        return plan_step->getSerializationName();
    return {};
}

String GroupExpression::getDescription() const
{
    String description;
    if (plan_step)
        description = plan_step->getStepDescription();
    if (description.empty())
        return getName();
    return getName() + " " + description;
}

const IQueryPlanStep * GroupExpression::getQueryPlanStep() const
{
    return plan_step.get();
}

bool GroupExpression::isApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties) const
{
    return applied_rules.contains({&rule, required_properties});
}

void GroupExpression::setApplied(const IOptimizationRule & rule, const ExpressionProperties & required_properties)
{
    applied_rules.insert({&rule, required_properties});
}

void GroupExpression::dump(WriteBuffer & out, const CostConfig & cost_config) const
{
    properties.dump(out);
    out << " '" << getDescription() << "'";
    if (strategy)
        out << " [" << strategy->getName() << "]";
    if (enforced_property != EnforcedProperty::None)
        out << " [enforcer:" << (enforced_property == EnforcedProperty::Sorting ? "Sorting" : "Distribution") << "]";
    out << " inputs:";
    for (const auto & input : inputs)
        out << " #" << input.group_id;
    if (cost.has_value())
        out << " cost: " << cost->subtree_cost.total(cost_config);
}

String GroupExpression::dump(const CostConfig & cost_config) const
{
    WriteBufferFromOwnString out;
    dump(out, cost_config);
    return out.str();
}

const StepFingerprint * GroupExpression::cachedStepFingerprint() const
{
    /// Every step has a full digest, so the only expression without one is a stepless expression.
    if (!plan_step)
    {
        /// Drop an inherited entry so it stops pinning the step it was computed from.
        cached_step_fingerprint.reset();
        return nullptr;
    }

    /// A rule may legally replace `plan_step` on a shallow copy before insertion, so a cached
    /// fingerprint that was computed for a different step must not be trusted.
    if (!cached_step_fingerprint || cached_step_fingerprint->source_step != plan_step)
        cached_step_fingerprint = std::make_shared<const StepFingerprint>(StepFingerprint{computeStepFullFingerprint(*plan_step), plan_step});

    return cached_step_fingerprint.get();
}

/// `fullyEqualTo` and `fullFingerprint` must cover the same components in the same order:
/// properties, inputs (group and required properties), strategy, enforced property, description
/// suffix, then the step. `enforced_property` and `description_suffix` are GroupExpression state,
/// not the step's display description that the digest excludes: `Group` relies on
/// `enforced_property` for enforcer cycle avoidance, and nothing guarantees `description_suffix`
/// carries no meaning, so both are included to fail closed.
size_t GroupExpression::fullFingerprint() const
{
    size_t hash_value = ExpressionPropertiesHash()(properties);
    for (const auto & input : inputs)
    {
        boost::hash_combine(hash_value, input.group_id);
        boost::hash_combine(hash_value, ExpressionPropertiesHash()(input.required_properties));
    }
    if (strategy)
        boost::hash_combine(hash_value, std::hash<String>()(strategy->getName()));
    boost::hash_combine(hash_value, static_cast<uint8_t>(enforced_property));
    boost::hash_combine(hash_value, std::hash<String>()(description_suffix));

    if (const auto * fingerprint = cachedStepFingerprint())
    {
        boost::hash_combine(hash_value, fingerprint->value.items[0]);
        boost::hash_combine(hash_value, fingerprint->value.items[1]);
    }
    else
    {
        /// A stepless expression: nothing to digest, and `fullyEqualTo` compares it by pointer.
        boost::hash_combine(hash_value, reinterpret_cast<uintptr_t>(plan_step.get()));
    }

    return hash_value;
}

bool GroupExpression::fullyEqualTo(const GroupExpression & other) const
{
    if (!(properties == other.properties))
        return false;

    if (inputs.size() != other.inputs.size())
        return false;
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (inputs[i].group_id != other.inputs[i].group_id)
            return false;
        if (!(inputs[i].required_properties == other.inputs[i].required_properties))
            return false;
    }

    /// Strategies are per-type singletons (`strategySingleton`), so equal pointers mean the
    /// same strategy type.
    if (strategy != other.strategy)
        return false;

    if (enforced_property != other.enforced_property)
        return false;

    if (description_suffix != other.description_suffix)
        return false;

    /// Fast path, and the whole answer for a stepless expression: two of those are equal by their
    /// (null) step, and a stepless one is never equal to one that has a step.
    if (plan_step == other.plan_step)
        return true;
    if (!plan_step || !other.plan_step)
        return false;

    /// The digest is total, so both fingerprints exist. It only narrows the candidates; the bytes
    /// decide.
    if (cachedStepFingerprint()->value != other.cachedStepFingerprint()->value)
        return false;

    return stepFullDigestsEqual(*plan_step, *other.plan_step);
}

const StepFingerprint * GroupExpression::cachedStepLogicalFingerprint() const
{
    if (!plan_step || !plan_step->hasLogicalDigest())
    {
        /// Drop an inherited entry so it stops pinning the step it was computed from.
        cached_step_logical_fingerprint.reset();
        return nullptr;
    }

    /// A rule may legally replace `plan_step` on a shallow copy before insertion, so a cached
    /// fingerprint that was computed for a different step must not be trusted.
    if (!cached_step_logical_fingerprint || cached_step_logical_fingerprint->source_step != plan_step)
        cached_step_logical_fingerprint
            = std::make_shared<const StepFingerprint>(StepFingerprint{computeStepLogicalFingerprint(*plan_step), plan_step});

    return cached_step_logical_fingerprint.get();
}

/// `logicallyEqualTo` and `logicalFingerprint` must cover the same components in the same order:
/// properties, inputs (group and required properties), then the step's logical digest. The input
/// count is hashed too, so an input list cannot alias the step fingerprint that follows it.
UInt128 GroupExpression::logicalFingerprint() const
{
    chassert(strategy == nullptr && enforced_property == EnforcedProperty::None);

    SipHash hash;
    hash.update(ExpressionPropertiesHash()(properties));
    hash.update(inputs.size());
    for (const auto & input : inputs)
    {
        hash.update(input.group_id);
        hash.update(ExpressionPropertiesHash()(input.required_properties));
    }

    if (const auto * fingerprint = cachedStepLogicalFingerprint())
    {
        hash.update(fingerprint->value.items[0]);
        hash.update(fingerprint->value.items[1]);
    }
    else
    {
        /// No logical digest means the expression never merges, so hash the pointer to keep it in a
        /// bucket of its own.
        hash.update(reinterpret_cast<uintptr_t>(plan_step.get()));
    }

    return hash.get128();
}

bool GroupExpression::logicallyEqualTo(const GroupExpression & other) const
{
    chassert(strategy == nullptr && enforced_property == EnforcedProperty::None);
    chassert(other.strategy == nullptr && other.enforced_property == EnforcedProperty::None);

    if (!(properties == other.properties))
        return false;

    if (inputs.size() != other.inputs.size())
        return false;
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (inputs[i].group_id != other.inputs[i].group_id)
            return false;
        if (!(inputs[i].required_properties == other.inputs[i].required_properties))
            return false;
    }

    const auto * fingerprint = cachedStepLogicalFingerprint();
    const auto * other_fingerprint = other.cachedStepLogicalFingerprint();
    /// Fail closed: an unaudited step instance never merges, not even with itself.
    if (!fingerprint || !other_fingerprint)
        return false;

    if (plan_step == other.plan_step)
        return true;

    if (fingerprint->value != other_fingerprint->value)
        return false;

    /// The fingerprint only narrows the candidates; the bytes decide.
    return stepLogicalDigestsEqual(*plan_step, *other.plan_step);
}

}
