#pragma once

#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Core/Types.h>

#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

class TypeCatalogRoot;

/// One trusted canonical durable image. The index always decodes and hashes
/// the sidecar itself; callers cannot submit occurrence/specialization or byte
/// counters. Metadata and installation bytes are retained only as exact sizes.
struct AuthorityDependentObjectResourceImage
{
    SchemaObjectID object;
    std::string_view canonical_metadata_bytes;
    std::string_view canonical_sidecar_bytes;
    std::string_view canonical_installation_record_bytes;
};

struct AuthorityResourceUsageIndexLimits
{
    PersistedTypeReferencesLimits persisted_references;
    UInt64 maximum_objects = 100'000;
};

struct AuthorityResourceUsageSummary
{
    UInt64 object_count = 0;
    UInt64 total_occurrence_paths = 0;
    UInt64 unique_persisted_specializations = 0;
    UInt64 maximum_occurrence_paths_per_object = 0;
    UInt64 maximum_persisted_specializations_per_template = 0;
    UInt64 maximum_canonical_argument_bytes = 0;
    UInt64 maximum_lowered_physical_type_nodes = 0;
    UInt64 maximum_sidecar_bytes_per_object = 0;
    UInt64 total_durable_dependent_object_bytes = 0;
    UInt64 maximum_durable_dependent_object_bytes_per_object = 0;

    bool operator==(const AuthorityResourceUsageSummary &) const = default;
};

class AuthorityResourceUsageIndexError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidInput,
        DatabaseMismatch,
        ExpectationMismatch,
        SidecarMismatch,
        DefinitionMismatch,
        DuplicateObject,
        MissingObject,
        LimitExceeded,
    };

    AuthorityResourceUsageIndexError(Code code_, std::string_view message);

    const Code code;
};

/// Immutable root-owned contribution index. Three fixed-depth Patricia maps
/// retain object contributions, globally unique specializations and per-
/// template unique counts. Point mutations copy only paths for the affected
/// object and its distinct descriptors; untouched subtrees remain shared.
class AuthorityResourceUsageIndex final
{
public:
    using Ptr = std::shared_ptr<const AuthorityResourceUsageIndex>;
    struct Impl;

    ~AuthorityResourceUsageIndex();

    [[nodiscard]] static Ptr build(
        UUID database_uuid,
        std::span<const SidecarExpectationRecord> expectations,
        std::span<const AuthorityDependentObjectResourceImage> dependent_objects,
        const TypeCatalogRoot & catalog,
        const AuthorityResourceUsageIndexLimits & limits = {});

    [[nodiscard]] static Ptr addObject(
        const Ptr & base,
        const SidecarExpectationRecord & expectation,
        const AuthorityDependentObjectResourceImage & dependent_object,
        const TypeCatalogRoot & catalog,
        const AuthorityResourceUsageIndexLimits & limits = {});

    [[nodiscard]] static Ptr replaceObject(
        const Ptr & base,
        const SidecarExpectationRecord & expectation,
        const AuthorityDependentObjectResourceImage & dependent_object,
        const TypeCatalogRoot & catalog,
        const AuthorityResourceUsageIndexLimits & limits = {});

    [[nodiscard]] static Ptr
    removeObjects(const Ptr & base, std::span<const SchemaObjectID> objects, const AuthorityResourceUsageIndexLimits & limits = {});

    const UUID & getDatabaseUUID() const noexcept;
    const AuthorityResourceUsageSummary & getSummary() const noexcept;
    UInt64 getAccountedBytes() const noexcept;

    /// Conservative path-copy charge for adding one exact sidecar after its
    /// descriptor dictionary has already been decoded by the publication
    /// package. The two byte counts are sums of the retained descriptor
    /// strings; allocator slack is covered by two bytes per specialization.
    static UInt64 getObjectInsertionAccountedBytesUpperBound(
        UInt64 distinct_specializations, UInt64 canonical_argument_bytes, UInt64 canonical_physical_type_bytes);

    /// Exact expectation anchor retained by the contribution for this object.
    bool containsExactObject(const SidecarExpectationRecord & expectation) const noexcept;

private:
    explicit AuthorityResourceUsageIndex(std::shared_ptr<const Impl> impl_);

    const std::shared_ptr<const Impl> impl;
};

}
