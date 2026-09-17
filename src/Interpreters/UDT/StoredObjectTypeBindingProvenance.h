#pragma once

#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/SchemaObjectIdentity.h>

#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

struct StoredObjectTypeBindingProvenanceLimits
{
    UInt64 maximum_descriptors = 65'536;
    UInt64 maximum_definitions = 65'536;
    UInt64 maximum_dependency_edges = 4ULL << 20;
};

class StoredObjectTypeBindingProvenanceError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidInput,
        CrossDatabaseReference,
        DescriptorMismatch,
        DefinitionClosureMismatch,
        LimitExceeded,
    };

    StoredObjectTypeBindingProvenanceError(Code code_, std::string_view message);

    const Code code;
};

/// Validates the exact immutable-definition closure retained by a stored-object
/// binding. Every descriptor must match one retained handle by identity, hash,
/// and complete checked semantics. Every retained transitive handle must be
/// reachable from a descriptor definition, and every declared dependency must
/// resolve to an exact retained revision/hash in the object's database.
void validateStoredObjectTypeBindingProvenance(
    const SchemaObjectID & object,
    std::span<const InstantiatedTypeDescriptor::Ptr> descriptors,
    std::span<const Definition::Ptr> definitions,
    const StoredObjectTypeBindingProvenanceLimits & limits = {});

}
