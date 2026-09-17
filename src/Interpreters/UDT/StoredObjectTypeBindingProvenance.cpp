#include <Interpreters/UDT/StoredObjectTypeBindingProvenance.h>

#include <unordered_map>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = StoredObjectTypeBindingProvenanceError;

constexpr UInt64 implementation_maximum_descriptors = 65'536;
constexpr UInt64 implementation_maximum_definitions = 65'536;
constexpr UInt64 implementation_maximum_dependency_edges = 4ULL << 20;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

void validateLimits(const StoredObjectTypeBindingProvenanceLimits & limits)
{
    if (!limits.maximum_descriptors || !limits.maximum_definitions || !limits.maximum_dependency_edges)
        fail(Error::Code::InvalidConfiguration, "stored-object type-binding provenance limits contain a zero bound");
    if (limits.maximum_descriptors > implementation_maximum_descriptors || limits.maximum_definitions > implementation_maximum_definitions
        || limits.maximum_dependency_edges > implementation_maximum_dependency_edges)
        fail(Error::Code::InvalidConfiguration, "stored-object type-binding provenance limits exceed implementation maxima");
}

bool descriptorMatchesDefinition(const PersistedTypeDescriptor & descriptor, const Definition & definition) noexcept
{
    return descriptor.getDefinitionIdentity() == definition.getIdentity()
        && descriptor.getDefinitionHash() == definition.getDefinitionHash() && descriptor.getCheckerABI() == definition.getCheckerABI()
        && descriptor.getCheckerChargeABI() == definition.getCheckerChargeABI() && descriptor.getPolicyABI() == definition.getPolicyABI()
        && descriptor.getFunctionRegistryABI() == definition.getFunctionRegistryABI()
        && descriptor.getPolicySemanticHash() == definition.getPolicySemanticHash()
        && descriptor.getSemanticCapabilities() == definition.getSemanticCapabilities();
}

}

StoredObjectTypeBindingProvenanceError::StoredObjectTypeBindingProvenanceError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

void validateStoredObjectTypeBindingProvenance(
    const SchemaObjectID & object,
    std::span<const InstantiatedTypeDescriptor::Ptr> descriptors,
    std::span<const Definition::Ptr> definitions,
    const StoredObjectTypeBindingProvenanceLimits & limits)
{
    validateLimits(limits);
    if (!object.isValid() || object.kind == SchemaObjectKind::TypeDefinition || descriptors.empty() || definitions.empty())
        fail(Error::Code::InvalidInput, "stored-object type-binding provenance input is invalid");
    if (descriptors.size() > limits.maximum_descriptors)
        fail(Error::Code::LimitExceeded, "stored-object type-binding provenance exceeds its descriptor limit");
    if (definitions.size() > limits.maximum_definitions)
        fail(Error::Code::LimitExceeded, "stored-object type-binding provenance exceeds its definition limit");

    std::unordered_map<UUID, size_t> definition_by_type;
    definition_by_type.reserve(definitions.size());
    for (size_t index = 0; index < definitions.size(); ++index)
    {
        const auto & definition = definitions[index];
        if (!definition || definition->getIdentity().type_uuid == UUIDHelpers::Nil || !definition->getIdentity().revision)
            fail(Error::Code::InvalidInput, "stored-object type-binding provenance contains an invalid definition handle");
        if (definition->getIdentity().database_uuid != object.database_uuid)
            fail(Error::Code::CrossDatabaseReference, "stored-object type-binding definition belongs to another database");
        if (!definition_by_type.emplace(definition->getIdentity().type_uuid, index).second)
            fail(Error::Code::DefinitionClosureMismatch, "stored-object type-binding retains multiple revisions of one definition");
    }

    std::vector<UInt8> reachable(definitions.size(), 0);
    std::vector<size_t> pending;
    pending.reserve(definitions.size());
    for (const auto & descriptor : descriptors)
    {
        if (!descriptor || !descriptor->getDefinition())
            fail(Error::Code::DescriptorMismatch, "stored-object type binding contains a descriptor without its definition handle");
        const auto & persisted = descriptor->getPersistedDescriptor();
        if (persisted.getDefinitionIdentity().database_uuid != object.database_uuid)
            fail(Error::Code::CrossDatabaseReference, "stored-object type-binding descriptor belongs to another database");
        if (!descriptorMatchesDefinition(persisted, *descriptor->getDefinition()))
            fail(Error::Code::DescriptorMismatch, "stored-object descriptor fields disagree with its immutable definition handle");

        const auto found = definition_by_type.find(persisted.getDefinitionIdentity().type_uuid);
        if (found == definition_by_type.end())
            fail(Error::Code::DefinitionClosureMismatch, "stored-object descriptor definition is absent from its retained closure");
        const auto & retained = definitions[found->second];
        if (!descriptorMatchesDefinition(persisted, *retained) || !retained->hasSameCheckedSemantics(*descriptor->getDefinition()))
            fail(Error::Code::DescriptorMismatch, "stored-object descriptor has no exact retained checked-definition match");
        if (!reachable[found->second])
        {
            reachable[found->second] = 1;
            pending.push_back(found->second);
        }
    }

    UInt64 dependency_edges = 0;
    while (!pending.empty())
    {
        const size_t definition_index = pending.back();
        pending.pop_back();
        const auto & definition = definitions[definition_index];
        const auto & dependencies = definition->getDependencies();
        if (dependencies.size() > limits.maximum_dependency_edges - dependency_edges)
            fail(Error::Code::LimitExceeded, "stored-object type-binding provenance exceeds its dependency-edge limit");
        dependency_edges += dependencies.size();

        for (const auto & dependency : dependencies)
        {
            const auto found = definition_by_type.find(dependency.type_uuid);
            if (found == definition_by_type.end())
                fail(Error::Code::DefinitionClosureMismatch, "stored-object type-binding retained closure omits a dependency");
            const auto & target = definitions[found->second];
            if (target->getIdentity().revision != dependency.revision || target->getDefinitionHash() != dependency.target_definition_hash)
                fail(
                    Error::Code::DefinitionClosureMismatch,
                    "stored-object type-binding dependency revision or hash disagrees with its handle");
            if (!reachable[found->second])
            {
                reachable[found->second] = 1;
                pending.push_back(found->second);
            }
        }
    }

    for (const UInt8 is_reachable : reachable)
    {
        if (!is_reachable)
            fail(Error::Code::DefinitionClosureMismatch, "stored-object type-binding retains an unreachable definition handle");
    }
}

}
