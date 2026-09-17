#include <Analyzer/UDT/SemanticTransferRegistry.h>

#include <array>
#include <cstddef>
#include <limits>
#include <type_traits>

namespace DB::UDT
{
namespace
{

constexpr UInt32 many_inputs = std::numeric_limits<UInt32>::max();

constexpr std::array<SemanticTransferDescriptor, static_cast<std::size_t>(SemanticTransferKind::Count) - 1> registry{{
    {SemanticTransferKind::Identity, SemanticTransferPolicy::PreserveUnary, 1, 1, false, false, false},
    {SemanticTransferKind::Rename, SemanticTransferPolicy::PreserveUnary, 1, 1, false, false, false},
    {SemanticTransferKind::StaticChildSelection, SemanticTransferPolicy::PreserveUnary, 1, 1, false, false, false},
    {SemanticTransferKind::StaticReshape, SemanticTransferPolicy::ReshapeUnary, 1, 1, true, true, false},
    {SemanticTransferKind::NullableLift,
     SemanticTransferPolicy::ReshapeUnary,
     1,
     1,
     true,
     true,
     false,
     SemanticNullContract::OuterNullMapBypassesSemanticPrograms},
    {SemanticTransferKind::LowCardinalityReshape, SemanticTransferPolicy::ReshapeUnary, 1, 1, true, true, false},
    {SemanticTransferKind::UnanimousBranch, SemanticTransferPolicy::MeetUnanimous, 1, many_inputs, false, true, false},
    {SemanticTransferKind::UnanimousUnion, SemanticTransferPolicy::MeetUnanimous, 1, many_inputs, false, true, false},
    {SemanticTransferKind::ExactInstantiationCast, SemanticTransferPolicy::PreserveExactInstantiation, 1, 1, false, false, true},
    {SemanticTransferKind::JoinDirectNonSynthesizing, SemanticTransferPolicy::PreserveUnary, 1, 1, false, false, false},
    {SemanticTransferKind::JoinDirectNullableLift,
     SemanticTransferPolicy::ReshapeUnary,
     1,
     1,
     true,
     true,
     false,
     SemanticNullContract::OuterNullMapBypassesSemanticPrograms},
    {SemanticTransferKind::JoinUsingUnanimous, SemanticTransferPolicy::MeetUnanimous, 2, many_inputs, false, true, false},
}};

static_assert(registry.size() + 1 == static_cast<std::size_t>(SemanticTransferKind::Count));

}

const SemanticTransferDescriptor * SemanticTransferRegistry::find(SemanticTransferKind kind) noexcept
{
    using Underlying = std::underlying_type_t<SemanticTransferKind>;
    const auto raw_kind = static_cast<Underlying>(kind);
    if (raw_kind <= static_cast<Underlying>(SemanticTransferKind::Unregistered)
        || raw_kind >= static_cast<Underlying>(SemanticTransferKind::Count))
        return nullptr;

    const auto & descriptor = registry[static_cast<std::size_t>(raw_kind - 1)];
    return descriptor.kind == kind ? &descriptor : nullptr;
}

}
