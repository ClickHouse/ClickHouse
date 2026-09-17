#include <Analyzer/UDT/SemanticSinkRegistry.h>

#include <array>
#include <cstddef>
#include <type_traits>

namespace DB::UDT
{
namespace
{

constexpr auto semantic_input_capabilities = semanticCapabilityBit(SemanticCapability::Input)
    | semanticCapabilityBit(SemanticCapability::ValueChecks) | semanticCapabilityBit(SemanticCapability::Default);
constexpr auto semantic_constant_capabilities
    = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks);

constexpr std::array<SemanticSinkDescriptor, static_cast<std::size_t>(SemanticSinkKind::Count)> registry{{
    {SemanticSinkKind::ExplicitUDTConstruction, semantic_input_capabilities, true, true, false},
    {SemanticSinkKind::ExplicitUDTCast, semantic_input_capabilities, true, true, false},
    {SemanticSinkKind::InsertTarget, semantic_input_capabilities, true, true, false},
    {SemanticSinkKind::AssignmentTarget, semantic_input_capabilities, true, true, false},
    {SemanticSinkKind::EqualityConstant, semantic_constant_capabilities, true, true, false},
    {SemanticSinkKind::InConstant, semantic_constant_capabilities, true, true, false},
    {SemanticSinkKind::GlobalInConstant, semantic_constant_capabilities, true, true, false},
    {SemanticSinkKind::HasConstant, semantic_constant_capabilities, true, true, false},
    {SemanticSinkKind::HasAnyConstant, semantic_constant_capabilities, true, true, false},
    {SemanticSinkKind::LogicalRendering, semanticCapabilityBit(SemanticCapability::Output), false, false, true},
    {SemanticSinkKind::LogicalIntrospection, 0, false, false, true},
    {SemanticSinkKind::SubqueryOutput, all_semantic_capabilities, false, false, false},
}};

static_assert(registry.size() == static_cast<std::size_t>(SemanticSinkKind::Count));

}

const SemanticSinkDescriptor * SemanticSinkRegistry::find(SemanticSinkKind kind) noexcept
{
    /// This is a closed admission registry, not merely a descriptor catalog.
    /// Reserved kinds retain stable vocabulary and descriptor positions, but
    /// this boundary deliberately rejects them while their capability contract
    /// is inactive. Activation must update this gate and its analyzer ownership
    /// and transfer hooks as one change.
    switch (kind)
    {
        case SemanticSinkKind::ExplicitUDTCast:
        case SemanticSinkKind::EqualityConstant:
        case SemanticSinkKind::InConstant:
        case SemanticSinkKind::GlobalInConstant:
        case SemanticSinkKind::HasConstant:
        case SemanticSinkKind::HasAnyConstant: break;
        default: return nullptr;
    }

    using Underlying = std::underlying_type_t<SemanticSinkKind>;
    const auto raw_kind = static_cast<Underlying>(kind);
    if (raw_kind >= static_cast<Underlying>(SemanticSinkKind::Count))
        return nullptr;

    const auto & descriptor = registry[static_cast<std::size_t>(raw_kind)];
    return descriptor.kind == kind ? &descriptor : nullptr;
}

bool SemanticSinkRegistry::isEligible(const SemanticSink & sink) noexcept
{
    const auto * descriptor = find(sink.kind);
    if (!descriptor || !sink.source.isValid())
        return false;

    const auto unknown_object_capabilities = static_cast<SemanticCapabilityMask>(
        sink.object_semantic_capabilities & static_cast<SemanticCapabilityMask>(~all_semantic_capabilities));
    const auto unknown_selected_capabilities = static_cast<SemanticCapabilityMask>(
        sink.selected_semantic_capabilities & static_cast<SemanticCapabilityMask>(~all_semantic_capabilities));
    if (unknown_object_capabilities || unknown_selected_capabilities
        || (sink.selected_semantic_capabilities & sink.object_semantic_capabilities) != sink.selected_semantic_capabilities)
        return false;

    if (descriptor->requires_expected_role && !sink.expected_role)
        return false;
    if (sink.expected_role && !descriptor->allows_expected_role)
        return false;
    if (sink.expected_role && !sink.expected_role->role.isValid())
        return false;
    if (sink.expected_role
        && (sink.selected_semantic_capabilities
            & static_cast<SemanticCapabilityMask>(~sink.expected_role->role.descriptor->getSemanticCapabilities()))
            != 0)
        return false;
    if (sink.expected_role && sink.expected_role->retained_descriptor
        && !sink.expected_role->retained_descriptor->getPersistedDescriptor().hasSameInstantiation(*sink.expected_role->role.descriptor))
        return false;

    if (sink.observes_identity && !descriptor->allows_identity_observation)
        return false;

    if (!descriptor->activation_capabilities)
        return sink.observes_identity;

    if ((sink.object_semantic_capabilities & descriptor->activation_capabilities) == 0)
        return false;

    return (sink.selected_semantic_capabilities & descriptor->activation_capabilities) != 0;
}

}
