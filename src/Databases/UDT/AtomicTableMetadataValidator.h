#pragma once

#include <Databases/UDT/DependentObjectAdmissionPlanner.h>

#include <Core/UUID.h>

#include <Parsers/IAST_fwd.h>

#include <Storages/IStorage_fwd.h>

#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

struct AtomicTableMetadataValidatorLimits
{
    PersistedTypeReferencesLimits persisted_references;
    TableColumnTypeBindingLimits table_columns;
    BoundObjectTypeReferencesLimits bound_references;
    UInt64 maximum_metadata_bytes = 16ULL << 20;
    UInt64 maximum_parser_depth = 256;
    UInt64 maximum_parser_backtracks = 100'000;
};

class AtomicTableMetadataValidationError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        InvalidSidecar,
        InvalidMetadata,
        DatabaseMismatch,
        ObjectMismatch,
        PhysicalSchemaMismatch,
        TrustedMetadataMismatch,
        AuthorityMismatch,
    };

    AtomicTableMetadataValidationError(Code code_, std::string_view message);

    const Code code;
};

/// DatabaseAtomic-owned validator for the ordinary metadata.sql image staged
/// by dependent-table admission. It accepts no logical type names: the SQL
/// must already contain the physicalized column types, while the exact logical
/// identity remains exclusively in the expectation-addressed sidecar.
class AtomicTableMetadataValidator final : public IDependentTableMetadataValidator
{
public:
    explicit AtomicTableMetadataValidator(
        UUID owning_database_uuid_,
        const ASTPtr & trusted_create_query,
        const StoragePtr & trusted_table,
        AtomicTableMetadataValidatorLimits limits_ = {});

    /// Reconstructs and publishes the immutable logical column bindings for an
    /// already-attached table during Atomic startup. Every check and allocation
    /// completes against the exact recovered root and the current storage
    /// snapshot before setInMemoryMetadata performs the sole publication step.
    void validateAndBindStartupMetadata(
        const AuthorityRoot & recovered_root,
        const SidecarExpectationRecord & expectation,
        std::string_view canonical_metadata_bytes,
        std::string_view canonical_sidecar_bytes) const;

protected:
    DecodedTableMetadata
    decodeAndCanonicalize(std::string_view candidate_metadata_bytes, std::string_view canonical_sidecar_bytes) const override;

private:
    UUID owning_database_uuid;
    SchemaObjectID trusted_object;
    String trusted_database_name;
    String trusted_table_name;
    String trusted_engine_name;
    Digest trusted_physical_schema_fingerprint{};
    String trusted_canonical_metadata_bytes;
    StoragePtr trusted_table;
    AtomicTableMetadataValidatorLimits limits;
};

}
