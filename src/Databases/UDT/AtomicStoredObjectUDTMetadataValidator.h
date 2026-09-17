#pragma once

#include <Databases/UDT/StoredObjectUDTPublicationPackage.h>

#include <Interpreters/UDT/DictionaryAttributeTypeBindings.h>
#include <Interpreters/UDT/ViewOutputTypeBindings.h>

#include <Parsers/IAST_fwd.h>

#include <Storages/IStorage_fwd.h>

#include <string_view>

namespace DB::UDT
{

struct AtomicStoredObjectUDTMetadataValidatorLimits
{
    PersistedTypeReferencesLimits persisted_references;
    ViewOutputTypeBindingLimits view_outputs;
    DictionaryAttributeTypeBindingLimits dictionary_attributes;
    UInt64 maximum_metadata_bytes = 16ULL << 20;
    UInt64 maximum_parser_depth = 256;
    UInt64 maximum_parser_backtracks = 100'000;
};

struct AtomicStoredObjectUDTValidatedMetadataImage
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};
};

/// DatabaseAtomic-owned validator for physicalized View and Dictionary
/// metadata. The trusted CREATE image supplies the ordered physical schema;
/// logical identity is accepted only from the exact canonical sidecar.
class AtomicStoredObjectUDTMetadataValidator final : public IStoredObjectUDTMetadataValidator
{
public:
    AtomicStoredObjectUDTMetadataValidator(
        UUID owning_database_uuid_,
        const ASTPtr & trusted_create_query,
        UInt64 object_schema_revision_,
        AtomicStoredObjectUDTMetadataValidatorLimits limits_ = {});

    /// Reconstructs exact declaration paths against the recovered authority
    /// root, then publishes one bound runtime metadata snapshot for the
    /// already-created View/Dictionary IStorage.
    void validateAndBindStartupMetadata(
        const AuthorityRoot & recovered_root,
        const SidecarExpectationRecord & expectation,
        std::string_view canonical_metadata_bytes,
        std::string_view canonical_sidecar_bytes,
        const StoragePtr & trusted_storage) const;

    /// Pure current-image validation for periodic verification. Unlike the
    /// startup method this neither publishes bound metadata nor mutates the
    /// storage; the executor separately compares the already-live binding.
    [[nodiscard]] AtomicStoredObjectUDTValidatedMetadataImage validateCurrentMetadata(
        const SidecarExpectationRecord & expectation,
        std::string_view canonical_metadata_bytes,
        std::string_view canonical_sidecar_bytes) const;

protected:
    DecodedMetadata decodeAndCanonicalize(
        std::string_view candidate_metadata_bytes,
        std::string_view canonical_sidecar_bytes,
        const StoredObjectUDTMetadataValidationLimits & validation_limits) const override;

private:
    UUID owning_database_uuid;
    SchemaObjectID trusted_object;
    UInt64 object_schema_revision = 0;
    String trusted_object_name;
    Digest trusted_physical_schema_fingerprint{};
    Digest trusted_mixed_physical_schema_fingerprint{};
    String trusted_canonical_metadata_bytes;
    AtomicStoredObjectUDTMetadataValidatorLimits limits;
};

}
