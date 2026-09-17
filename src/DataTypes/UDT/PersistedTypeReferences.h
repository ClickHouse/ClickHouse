#pragma once

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/SchemaObjectIdentity.h>

#include <Core/Types.h>

#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

/// V1 remains the default for declaration-only sidecars. Writers must opt in
/// to V2 only when they need an occurrence-site discriminator; decoding both
/// versions is permanent.
inline constexpr UInt16 persisted_type_references_format_version = 1;
inline constexpr UInt16 persisted_type_path_dictionary_version = 1;
inline constexpr UInt16 persisted_type_references_format_version_v2 = 2;
inline constexpr UInt16 persisted_type_path_dictionary_version_v2 = 2;
inline constexpr std::string_view persisted_type_references_sidecar_hash_domain = "ClickHouse UDT sidecar expectation V1";
inline constexpr std::string_view persisted_type_references_sidecar_hash_domain_v2 = "ClickHouse UDT sidecar expectation V2";

/// These wire values are permanent occurrence-path section tags. They are not
/// SchemaObjectKind values and must never be serialized from an enum ordinal.
enum class PersistedTypePathSection : UInt8
{
    ColumnType = 1,
    ViewExpression = 2,
    DictionaryAttribute = 3,
    SyntheticPayload = 254,
};

/// V2-only discriminator for endpoint namespaces within one object-kind
/// section. V1 decodes canonically as Declaration and can never encode either
/// of the other values.
enum class PersistedTypeOccurrenceSite : UInt8
{
    Declaration = 1,
    StoredExpression = 2,
    SchemaString = 3,
};

struct PersistedTypeOccurrencePath
{
    PersistedTypePathSection section = PersistedTypePathSection::ColumnType;
    PersistedTypeOccurrenceSite site = PersistedTypeOccurrenceSite::Declaration;
    UInt64 object_ordinal = 0;
    /// Distinguishes multiple logical UDT occurrences at the same physical
    /// endpoint. It precedes the normalized type-child path on the wire.
    UInt64 occurrence_ordinal = 0;
    std::vector<UInt64> type_child_ordinals;

    bool operator==(const PersistedTypeOccurrencePath &) const = default;
};

struct PersistedTypeOccurrenceUse
{
    UInt64 path_id = 0;
    UInt64 descriptor_id = 0;

    bool operator==(const PersistedTypeOccurrenceUse &) const = default;
};

/// Exact object-sidecar payload. Descriptor identities and occurrence paths
/// are independent dictionaries; uses bind every path ID to one descriptor.
struct PersistedTypeReferences
{
    UInt16 format_version = persisted_type_references_format_version;
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest physical_schema_fingerprint{};
    UInt16 path_dictionary_version = persisted_type_path_dictionary_version;
    std::vector<PersistedTypeDescriptor> descriptors;
    std::vector<PersistedTypeOccurrencePath> occurrence_paths;
    std::vector<PersistedTypeOccurrenceUse> uses;
    UInt16 semantic_extension_version = 1;
    UInt16 semantic_extension_flags = 0;

    bool operator==(const PersistedTypeReferences &) const = default;
};

/// V1 maxima are shared with TypeDescriptorLimits. Callers may lower them;
/// widening any field beyond these implementation maxima needs a new format.
struct PersistedTypeReferencesLimits
{
    UInt64 maximum_sidecar_bytes = 16ULL << 20;
    UInt64 maximum_descriptors = 65'536;
    UInt64 maximum_occurrence_paths = 65'536;
    UInt64 maximum_path_depth = 64;
    UInt64 maximum_canonical_arguments_bytes = 64ULL << 10;
    UInt64 maximum_canonical_physical_type_bytes = 64ULL << 10;
    UInt64 maximum_qualified_name_bytes = 4ULL << 10;
    /// Bounds the canonical SQL-like representation independently from the
    /// compact binary sidecar. It is intentionally not part of the V1 wire.
    UInt64 maximum_text_bytes = 64ULL << 20;
};

class PersistedTypeReferencesError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        DigestMismatch,
        TrailingData,
    };

    PersistedTypeReferencesError(Code code_, std::string_view message);

    const Code code;
};

/// Validates the complete caller-supplied configuration, including limits
/// used only by the canonical text representation. Invalid bounds throw
/// PersistedTypeReferencesError before any payload is inspected.
void validatePersistedTypeReferencesLimits(const PersistedTypeReferencesLimits & limits);

String encodePersistedTypeReferences(const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits = {});

PersistedTypeReferences decodePersistedTypeReferences(std::string_view bytes, const PersistedTypeReferencesLimits & limits = {});

/// Canonical internal metadata clause. This does not register public SQL
/// admission; callers own the surrounding stored-object syntax.
String formatPersistedTypeReferencesText(const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits = {});

PersistedTypeReferences parsePersistedTypeReferencesText(std::string_view text, const PersistedTypeReferencesLimits & limits = {});

Digest computePersistedTypeReferencesSidecarHash(
    const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits = {});

static_assert(static_cast<UInt8>(PersistedTypePathSection::ColumnType) == 1);
static_assert(static_cast<UInt8>(PersistedTypePathSection::ViewExpression) == 2);
static_assert(static_cast<UInt8>(PersistedTypePathSection::DictionaryAttribute) == 3);
static_assert(static_cast<UInt8>(PersistedTypePathSection::SyntheticPayload) == 254);
static_assert(static_cast<UInt8>(PersistedTypeOccurrenceSite::Declaration) == 1);
static_assert(static_cast<UInt8>(PersistedTypeOccurrenceSite::StoredExpression) == 2);
static_assert(static_cast<UInt8>(PersistedTypeOccurrenceSite::SchemaString) == 3);
}
