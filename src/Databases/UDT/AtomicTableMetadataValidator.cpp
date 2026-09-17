#include <Databases/UDT/AtomicTableMetadataValidator.h>

#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationStampPublication.h>

#include <DataTypes/UDT/TableColumnTypeBindings.h>

#include <Interpreters/InterpreterCreateQuery.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/Exception.h>

#include <new>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = AtomicTableMetadataValidationError;

constexpr UInt64 implementation_maximum_metadata_bytes = 16ULL << 20;
constexpr UInt64 implementation_maximum_parser_depth = 256;
constexpr UInt64 implementation_maximum_parser_backtracks = 1'000'000;

struct DecodedCanonicalSidecar
{
    PersistedTypeReferences references;
    Digest hash{};
};

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

void validateLimits(const AtomicTableMetadataValidatorLimits & limits)
{
    if (!limits.maximum_metadata_bytes || limits.maximum_metadata_bytes > implementation_maximum_metadata_bytes
        || !limits.maximum_parser_depth || limits.maximum_parser_depth > implementation_maximum_parser_depth
        || !limits.maximum_parser_backtracks || limits.maximum_parser_backtracks > implementation_maximum_parser_backtracks
        || !std::in_range<size_t>(limits.maximum_metadata_bytes) || !std::in_range<size_t>(limits.maximum_parser_depth)
        || !std::in_range<size_t>(limits.maximum_parser_backtracks))
    {
        fail(Error::Code::InvalidConfiguration, "Atomic table metadata validator limits are invalid");
    }
}

DecodedCanonicalSidecar decodeCanonicalSidecar(std::string_view canonical_sidecar_bytes, const AtomicTableMetadataValidatorLimits & limits)
{
    try
    {
        auto references = decodePersistedTypeReferences(canonical_sidecar_bytes, limits.persisted_references);
        const String encoded = encodePersistedTypeReferences(references, limits.persisted_references);
        if (encoded != canonical_sidecar_bytes)
            fail(Error::Code::InvalidSidecar, "Atomic table metadata sidecar is not canonical");
        const Digest hash = computePersistedTypeReferencesSidecarHash(references, limits.persisted_references);
        return {
            .references = std::move(references),
            .hash = hash,
        };
    }
    catch (const Error &)
    {
        throw;
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic table metadata sidecar exceeds its limit");
        fail(Error::Code::InvalidSidecar, "Atomic table metadata sidecar is invalid");
    }
}

class StartupRootAuthorityAdapter final : public IAuthorityAdapter
{
public:
    explicit StartupRootAuthorityAdapter(const AuthorityRoot & root_) noexcept
        : root(root_)
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return root.getDatabaseUUID(); }

    ResolutionSession beginResolutionSession() const override
    {
        return makeSnapshotResolutionSession(
            &root,
            {
                .find_by_identity = findByIdentity,
                .find_by_name = findByName,
                .get_generation = getGeneration,
                .get_effective_resource_limits = getEffectiveResourceLimits,
            });
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view) const override
    {
        if (!capabilities.containsAll(required))
            fail(Error::Code::AuthorityMismatch, "Atomic table startup root lacks required transient capabilities");
    }

private:
    static Definition::Ptr findByIdentity(const void * view, const DefinitionIdentity & identity)
    {
        return static_cast<const AuthorityRoot *>(view)->findByIdentity(identity);
    }

    static Definition::Ptr findByName(const void * view, std::string_view local_name)
    {
        return static_cast<const AuthorityRoot *>(view)->findByName(local_name);
    }

    static UInt64 getGeneration(const void * view) noexcept { return static_cast<const AuthorityRoot *>(view)->getTypeIndexGeneration(); }
    static const EffectiveResourceLimits * getEffectiveResourceLimits(const void * view) noexcept
    {
        return &static_cast<const AuthorityRoot *>(view)->getDatabaseResourceQuota().getLimits();
    }

    const AuthorityRoot & root;
    const TypeAuthorityCapabilities capabilities = atomicDatabaseAuthorityCapabilities();
};

bool isPhysicalColumn(ColumnDefaultSpecifier specifier) noexcept
{
    switch (specifier)
    {
        case ColumnDefaultSpecifier::Empty:
        case ColumnDefaultSpecifier::Default:
        case ColumnDefaultSpecifier::Materialized:
        case ColumnDefaultSpecifier::AutoIncrement: return true;
        case ColumnDefaultSpecifier::Alias:
        case ColumnDefaultSpecifier::Ephemeral: return false;
    }
    return false;
}

NamesAndTypesList decodePhysicalColumns(const ASTCreateQuery & create, const AtomicTableMetadataValidatorLimits & limits)
{
    if (!create.columns_list || !create.columns_list->columns)
        fail(Error::Code::InvalidMetadata, "Atomic table metadata has no explicit column list");

    NamesAndTypesList physical_columns;
    try
    {
        for (const auto & child : create.columns_list->columns->children)
        {
            const auto * column = child ? child->as<ASTColumnDeclaration>() : nullptr;
            if (!column || !column->getType())
                fail(Error::Code::InvalidMetadata, "Atomic table metadata contains a column without an explicit physical type");
            const auto column_type = InterpreterCreateQuery::getColumnType(*column, LoadingStrictnessLevel::ATTACH, false);
            if (!isPhysicalColumn(column->default_specifier))
                continue;
            physical_columns.emplace_back(column->name, column_type);
        }
    }
    catch (const Error &)
    {
        throw;
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidMetadata, "Atomic table metadata contains an invalid physical column type");
    }
    if (physical_columns.empty())
        fail(Error::Code::InvalidMetadata, "Atomic table metadata has no physical columns");
    if (physical_columns.size() > limits.table_columns.maximum_columns)
        fail(Error::Code::LimitExceeded, "Atomic table metadata physical columns exceed their limit");
    return physical_columns;
}

Digest computePhysicalSchemaFingerprint(const NamesAndTypesList & physical_columns, const AtomicTableMetadataValidatorLimits & limits)
{
    try
    {
        return computeTableColumnPhysicalSchemaFingerprint(physical_columns, limits.table_columns);
    }
    catch (const TableColumnTypeBindingError & error)
    {
        if (error.code == TableColumnTypeBindingError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "Atomic table metadata table-column limits are invalid");
        if (error.code == TableColumnTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic table metadata physical columns exceed their limit");
        fail(Error::Code::InvalidMetadata, "Atomic table metadata physical columns are invalid");
    }
}

}

AtomicTableMetadataValidationError::AtomicTableMetadataValidationError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AtomicTableMetadataValidator::AtomicTableMetadataValidator(
    UUID owning_database_uuid_,
    const ASTPtr & trusted_create_query,
    const StoragePtr & trusted_table_,
    AtomicTableMetadataValidatorLimits limits_)
    : owning_database_uuid(owning_database_uuid_)
    , trusted_table(trusted_table_)
    , limits(std::move(limits_))
{
    validateLimits(limits);
    if (owning_database_uuid == UUIDHelpers::Nil)
        fail(Error::Code::InvalidConfiguration, "Atomic table metadata validator has a nil owning database UUID");
    if (!trusted_create_query || !trusted_table_)
        fail(Error::Code::InvalidConfiguration, "Atomic table metadata validator has no trusted CREATE query or table");

    const auto * trusted_create = trusted_create_query->as<ASTCreateQuery>();
    if (!trusted_create || trusted_create->isTemporary() || trusted_create->isView() || trusted_create->is_dictionary
        || trusted_create->getTable().empty() || trusted_create->uuid == UUIDHelpers::Nil || !trusted_create->storage
        || !trusted_create->storage->engine)
    {
        fail(Error::Code::InvalidConfiguration, "Atomic table metadata validator trusted query is not an ordinary UUID table");
    }

    const StorageID storage_id = trusted_table_->getStorageID();
    if (storage_id.uuid == UUIDHelpers::Nil || storage_id.uuid != trusted_create->uuid
        || storage_id.table_name != trusted_create->getTable()
        || (!trusted_create->getDatabase().empty() && storage_id.database_name != trusted_create->getDatabase())
        || trusted_table_->getName() != trusted_create->storage->engine->name)
    {
        fail(Error::Code::InvalidConfiguration, "Atomic table metadata validator trusted query and table identity differ");
    }

    trusted_object = {
        .kind = SchemaObjectKind::Table,
        .database_uuid = owning_database_uuid,
        .object_uuid = storage_id.uuid,
    };
    trusted_database_name = storage_id.database_name;
    trusted_table_name = storage_id.table_name;
    trusted_engine_name = trusted_create->storage->engine->name;

    const auto query_physical_columns = decodePhysicalColumns(*trusted_create, limits);
    trusted_physical_schema_fingerprint = computePhysicalSchemaFingerprint(query_physical_columns, limits);
    const auto storage_metadata = this->trusted_table->getInMemoryMetadataPtr(nullptr, false);
    if (!storage_metadata)
        fail(Error::Code::InvalidConfiguration, "Atomic table metadata validator trusted table has no metadata snapshot");
    const Digest storage_physical_schema_fingerprint
        = computePhysicalSchemaFingerprint(storage_metadata->getColumns().getAllPhysical(), limits);
    if (storage_physical_schema_fingerprint != trusted_physical_schema_fingerprint)
    {
        fail(Error::Code::InvalidConfiguration, "Atomic table metadata validator trusted query and table schema differ");
    }

    try
    {
        trusted_canonical_metadata_bytes = getObjectDefinitionFromCreateQuery(trusted_create_query);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidConfiguration, "Atomic trusted table metadata cannot be canonicalized");
    }
    if (trusted_canonical_metadata_bytes.empty() || trusted_canonical_metadata_bytes.size() > limits.maximum_metadata_bytes)
        fail(Error::Code::InvalidConfiguration, "Atomic trusted table metadata exceeds its byte limit");
}

IDependentTableMetadataValidator::DecodedTableMetadata AtomicTableMetadataValidator::decodeAndCanonicalize(
    std::string_view candidate_metadata_bytes, std::string_view canonical_sidecar_bytes) const
{
    if (candidate_metadata_bytes.empty())
        fail(Error::Code::InvalidMetadata, "Atomic table metadata is empty");
    if (candidate_metadata_bytes.size() > limits.maximum_metadata_bytes)
        fail(Error::Code::LimitExceeded, "Atomic table metadata exceeds its byte limit");

    auto decoded_sidecar = decodeCanonicalSidecar(canonical_sidecar_bytes, limits);
    const auto & references = decoded_sidecar.references;

    if (!references.object.isValid() || references.object.kind != SchemaObjectKind::Table || !references.object_schema_revision)
        fail(Error::Code::InvalidSidecar, "Atomic table metadata sidecar has an invalid table identity or revision");
    if (references.object.database_uuid != owning_database_uuid)
        fail(Error::Code::DatabaseMismatch, "Atomic table metadata sidecar belongs to another database");
    if (references.object != trusted_object)
        fail(Error::Code::ObjectMismatch, "Atomic table metadata sidecar differs from the trusted table identity");

    ASTPtr ast;
    try
    {
        ParserCreateQuery parser;
        ast = parseQuery(
            parser,
            candidate_metadata_bytes.data(),
            candidate_metadata_bytes.data() + candidate_metadata_bytes.size(),
            "Atomic user-defined type table metadata",
            static_cast<size_t>(limits.maximum_metadata_bytes),
            static_cast<size_t>(limits.maximum_parser_depth),
            static_cast<size_t>(limits.maximum_parser_backtracks));
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidMetadata, "Atomic table metadata is not one complete CREATE-family query");
    }

    auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    if (!create || !create->attach || create->attach_short_syntax || create->if_not_exists || create->isTemporary() || create->isView()
        || create->is_time_series_table || create->is_dictionary || create->getTable().empty() || create->uuid == UUIDHelpers::Nil
        || !create->has_uuid_clause || !create->has_uuid || !create->cluster.empty() || !create->storage || !create->storage->engine
        || create->aliases_list || create->as_table_function || create->select
        || create->targets || create->sql_security || create->table_overrides || create->dictionary_attributes_list || create->dictionary
        || create->refresh_strategy || !create->as_database.empty() || !create->as_table.empty() || create->has_inner_uuid_clause
        || create->has_attach_from_path || !create->attach_from_path.empty() || create->attach_as_replicated.has_value()
        || create->is_clone_as || create->replace_view || create->replace_table || create->create_or_replace || create->is_populate
        || create->is_create_empty)
    {
        fail(Error::Code::InvalidMetadata, "Atomic table metadata is not a full ordinary ATTACH TABLE definition");
    }
    if (create->uuid != references.object.object_uuid)
        fail(Error::Code::ObjectMismatch, "Atomic table metadata UUID differs from its sidecar");

    const String & candidate_database_name = create->getDatabase();
    const String & candidate_table_name = create->getTable();
    const bool stored_placeholder = candidate_database_name.empty() && candidate_table_name == TABLE_WITH_UUID_NAME_PLACEHOLDER;
    const bool trusted_name = candidate_table_name == trusted_table_name
        && (candidate_database_name.empty() || candidate_database_name == trusted_database_name);
    if (!stored_placeholder && !trusted_name)
        fail(Error::Code::ObjectMismatch, "Atomic table metadata name differs from the trusted table identity");

    const auto physical_columns = decodePhysicalColumns(*create, limits);
    const Digest physical_schema_fingerprint = computePhysicalSchemaFingerprint(physical_columns, limits);
    if (physical_schema_fingerprint != references.physical_schema_fingerprint)
        fail(Error::Code::PhysicalSchemaMismatch, "Atomic table metadata physical schema differs from its sidecar");
    if (physical_schema_fingerprint != trusted_physical_schema_fingerprint)
        fail(Error::Code::TrustedMetadataMismatch, "Atomic table metadata physical schema differs from the trusted table");

    String canonical_metadata_bytes;
    try
    {
        canonical_metadata_bytes = getObjectDefinitionFromCreateQuery(ast);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidMetadata, "Atomic table metadata cannot be canonicalized");
    }
    if (canonical_metadata_bytes.empty() || canonical_metadata_bytes.size() > limits.maximum_metadata_bytes)
        fail(Error::Code::LimitExceeded, "Atomic canonical table metadata exceeds its byte limit");
    if (canonical_metadata_bytes != trusted_canonical_metadata_bytes)
        fail(Error::Code::TrustedMetadataMismatch, "Atomic table metadata semantics differ from the already-created trusted table");

    return {
        .object = references.object,
        .object_schema_revision = references.object_schema_revision,
        .object_name = trusted_table_name,
        .sidecar_hash = decoded_sidecar.hash,
        .physical_schema_fingerprint = physical_schema_fingerprint,
        .canonical_metadata_bytes = std::move(canonical_metadata_bytes),
    };
}

void AtomicTableMetadataValidator::validateAndBindStartupMetadata(
    const AuthorityRoot & recovered_root,
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_metadata_bytes,
    std::string_view canonical_sidecar_bytes) const
{
    if (recovered_root.getDatabaseUUID() != owning_database_uuid
        || recovered_root.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
    {
        fail(Error::Code::AuthorityMismatch, "Atomic table startup root is not the owning dependent-object-capable database authority");
    }
    const auto * recovered_expectation = recovered_root.findExpectationRecord(expectation.object);
    if (!recovered_expectation || *recovered_expectation != expectation)
        fail(Error::Code::AuthorityMismatch, "Atomic table startup expectation differs from the exact recovered authority root");

    const auto storage_id = trusted_table->getStorageID();
    if (storage_id.uuid != trusted_object.object_uuid || storage_id.database_name != trusted_database_name
        || storage_id.table_name != trusted_table_name || trusted_table->getName() != trusted_engine_name)
    {
        fail(Error::Code::TrustedMetadataMismatch, "Atomic trusted table identity changed before startup binding");
    }

    auto validated_metadata = validateAndCanonicalize(expectation, canonical_metadata_bytes, canonical_sidecar_bytes);
    const String validated_canonical_metadata = validated_metadata.releaseCanonicalMetadataBytes();
    if (validated_canonical_metadata != canonical_metadata_bytes)
        fail(Error::Code::InvalidMetadata, "Atomic table startup metadata is not canonical");

    auto decoded_sidecar = decodeCanonicalSidecar(canonical_sidecar_bytes, limits);
    auto metadata_snapshot = trusted_table->getInMemoryMetadataPtr(nullptr, false);
    if (!metadata_snapshot)
        fail(Error::Code::TrustedMetadataMismatch, "Atomic trusted table has no startup metadata snapshot");

    BoundObjectPhysicalSchema physical_schema;
    try
    {
        physical_schema = reconstructTableColumnPhysicalSchema(
            expectation.object,
            expectation.object_schema_revision,
            metadata_snapshot->getColumns().getAllPhysical(),
            decoded_sidecar.references,
            limits.table_columns);
    }
    catch (const TableColumnTypeBindingError & error)
    {
        if (error.code == TableColumnTypeBindingError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "Atomic table startup binding limits are invalid");
        if (error.code == TableColumnTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic table startup physical schema exceeds its binding limit");
        if (error.code == TableColumnTypeBindingError::Code::PhysicalSchemaMismatch)
            fail(Error::Code::PhysicalSchemaMismatch, "Atomic table startup physical schema differs from its sidecar");
        fail(Error::Code::InvalidSidecar, "Atomic table startup sidecar cannot be reconstructed over its physical schema");
    }

    BoundObjectTypeReferences::Ptr bound_references;
    try
    {
        StartupRootAuthorityAdapter authority(recovered_root);
        bound_references
            = BoundObjectTypeReferences::bind(decoded_sidecar.references, std::move(physical_schema), authority, limits.bound_references);
    }
    catch (const BoundObjectTypeReferencesError & error)
    {
        if (error.code == BoundObjectTypeReferencesError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "Atomic table startup bound-reference limits are invalid");
        if (error.code == BoundObjectTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic table startup bound references exceed their limit");
        if (error.code == BoundObjectTypeReferencesError::Code::AuthorityFailure)
            fail(Error::Code::AuthorityMismatch, "Atomic table startup sidecar cannot bind against the exact recovered root");
        if (error.code == BoundObjectTypeReferencesError::Code::PhysicalSchemaMismatch)
            fail(Error::Code::PhysicalSchemaMismatch, "Atomic table startup binding differs from its physical schema");
        fail(Error::Code::InvalidSidecar, "Atomic table startup sidecar cannot be bound");
    }

    if (!bound_references || bound_references->getObject() != expectation.object
        || bound_references->getObjectSchemaRevision() != expectation.object_schema_revision
        || bound_references->getSidecarHash() != expectation.sidecar_hash
        || bound_references->getPhysicalSchemaFingerprint() != expectation.physical_schema_fingerprint)
    {
        fail(Error::Code::InvalidSidecar, "Atomic table startup binding differs from its durable expectation");
    }

    const auto verification_stamp
        = verifyAndCreateAuthorityVerificationStamp(recovered_root, expectation, canonical_sidecar_bytes, *bound_references);
    StorageInMemoryMetadata replacement(*metadata_snapshot);
    replacement.setColumnsAndBoundUDTReferences(metadata_snapshot->getColumns(), std::move(bound_references), expectation);
    replacement.setBoundUDTVerificationStamp(verification_stamp);

    /// Sole externally visible step: every parse, root lookup, reconstruction,
    /// binding, copy, and invariant check above has already succeeded.
    trusted_table->setInMemoryMetadata(replacement);
}

}
