#pragma once

#include <Databases/UDT/AtomicAuthorityStartup.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>

#include <Core/Types.h>

#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

inline constexpr UInt16 synthetic_object_metadata_format_version = 1;
inline constexpr std::string_view synthetic_object_physical_fingerprint_domain = "ClickHouse UDT synthetic object physical schema V1";
inline constexpr std::string_view synthetic_object_metadata_record_hash_domain = "ClickHouse UDT synthetic object metadata record V1";

struct SyntheticObjectPhysicalOccurrence
{
    PersistedTypeOccurrencePath path;
    String canonical_physical_type;
    Digest storage_fingerprint{};
    SemanticCapabilityMask selected_semantic_capabilities = 0;

    bool operator==(const SyntheticObjectPhysicalOccurrence &) const = default;
};

struct SyntheticObjectMetadata
{
    UInt16 format_version = synthetic_object_metadata_format_version;
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    String diagnostic_name;
    std::vector<SyntheticObjectPhysicalOccurrence> occurrences;
    Digest physical_schema_fingerprint{};
    UInt16 semantic_extension_version = 1;
    UInt16 semantic_extension_flags = 0;
    Digest canonical_record_hash{};

    bool operator==(const SyntheticObjectMetadata &) const = default;
};

struct SyntheticObjectMetadataLimits
{
    UInt64 maximum_metadata_bytes = 16ULL << 20;
    UInt64 maximum_occurrences = 65'536;
    UInt64 maximum_path_depth = 64;
    UInt64 maximum_retained_path_components = 4ULL << 20;
    UInt64 maximum_diagnostic_name_bytes = 4ULL << 10;
    UInt64 maximum_canonical_physical_type_bytes = 64ULL << 10;
};

class SyntheticObjectMetadataError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        FingerprintMismatch,
        PhysicalTypeMismatch,
        TrailingData,
    };

    SyntheticObjectMetadataError(Code code_, std::string_view message);

    const Code code;
};

/// Hashes only the canonical physical occurrence schema. Object identity,
/// schema revision, diagnostic presentation and logical semantic-role bits
/// are bound by the enclosing metadata/expectation records and intentionally
/// do not perturb this value.
Digest
computeSyntheticObjectPhysicalFingerprint(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits = {});

Digest
computeSyntheticObjectMetadataRecordHash(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits = {});

SyntheticObjectMetadata makeSyntheticObjectMetadata(
    SchemaObjectID object,
    UInt64 object_schema_revision,
    String diagnostic_name,
    std::vector<SyntheticObjectPhysicalOccurrence> occurrences,
    const SyntheticObjectMetadataLimits & limits = {});

String encodeSyntheticObjectMetadata(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits = {});

SyntheticObjectMetadata decodeSyntheticObjectMetadata(std::string_view bytes, const SyntheticObjectMetadataLimits & limits = {});

AtomicAuthorityValidatedDependentObject validateSyntheticDependentObjectMetadata(
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_metadata_bytes,
    const SyntheticObjectMetadataLimits & limits = {});

/// Startup validation additionally binds every normalized physical occurrence
/// and its logical role to the expectation-addressed persisted descriptor.
AtomicAuthorityValidatedDependentObject validateSyntheticDependentObjectMetadata(
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_metadata_bytes,
    std::string_view canonical_sidecar_bytes,
    const SyntheticObjectMetadataLimits & metadata_limits = {},
    const PersistedTypeReferencesLimits & sidecar_limits = {});

BoundObjectPhysicalSchema
makeSyntheticBoundPhysicalSchema(const SyntheticObjectMetadata & metadata, const SyntheticObjectMetadataLimits & limits = {});

}
