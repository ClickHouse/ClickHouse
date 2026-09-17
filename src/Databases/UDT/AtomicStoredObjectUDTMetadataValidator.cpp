#include <Databases/UDT/AtomicStoredObjectUDTMetadataValidator.h>

#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationStampPublication.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>

#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/UDT/StoredObjectTypeBindingPreparation.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/Exception.h>

#include <algorithm>
#include <new>
#include <span>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = StoredObjectUDTPublicationPackageError;

constexpr UInt64 implementation_maximum_metadata_bytes = 16ULL << 20;
constexpr UInt64 implementation_maximum_parser_depth = 256;
constexpr UInt64 implementation_maximum_parser_backtracks = 1'000'000;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

void validateLimits(const AtomicStoredObjectUDTMetadataValidatorLimits & limits)
{
    if (!limits.maximum_metadata_bytes || limits.maximum_metadata_bytes > implementation_maximum_metadata_bytes
        || !limits.maximum_parser_depth || limits.maximum_parser_depth > implementation_maximum_parser_depth
        || !limits.maximum_parser_backtracks || limits.maximum_parser_backtracks > implementation_maximum_parser_backtracks
        || !std::in_range<size_t>(limits.maximum_metadata_bytes) || !std::in_range<size_t>(limits.maximum_parser_depth)
        || !std::in_range<size_t>(limits.maximum_parser_backtracks))
    {
        fail(Error::Code::InvalidConfiguration, "Atomic stored-object metadata validator limits are invalid");
    }
}

PersistedTypeReferences decodeCanonicalSidecar(std::string_view canonical_sidecar_bytes, const PersistedTypeReferencesLimits & limits)
{
    try
    {
        auto references = decodePersistedTypeReferences(canonical_sidecar_bytes, limits);
        if (encodePersistedTypeReferences(references, limits) != canonical_sidecar_bytes)
            fail(Error::Code::InvalidSidecar, "Atomic stored-object metadata sidecar is not canonical");
        return references;
    }
    catch (const Error &)
    {
        throw;
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic stored-object metadata sidecar exceeds its limit");
        fail(Error::Code::InvalidSidecar, "Atomic stored-object metadata sidecar is invalid");
    }
}

NamesAndTypesList decodeViewPhysicalOutputs(const ASTCreateQuery & create)
{
    if (!create.columns_list || !create.columns_list->columns)
        fail(Error::Code::InvalidMetadata, "Atomic View metadata has no explicit output declarations");

    NamesAndTypesList outputs;
    try
    {
        for (const auto & child : create.columns_list->columns->children)
        {
            const auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
            if (!declaration || !declaration->getType())
                fail(Error::Code::InvalidMetadata, "Atomic View metadata contains an output without a physical type");
            outputs.emplace_back(
                declaration->name, InterpreterCreateQuery::getColumnType(*declaration, LoadingStrictnessLevel::ATTACH, false));
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
        fail(Error::Code::InvalidMetadata, "Atomic View metadata contains an invalid physical output type");
    }
    return outputs;
}

NamesAndTypesList decodeDictionaryPhysicalAttributes(const ASTCreateQuery & create)
{
    if (!create.dictionary_attributes_list)
        fail(Error::Code::InvalidMetadata, "Atomic Dictionary metadata has no attribute declarations");

    NamesAndTypesList attributes;
    try
    {
        for (const auto & child : create.dictionary_attributes_list->children)
        {
            const auto * declaration = child ? child->as<ASTDictionaryAttributeDeclaration>() : nullptr;
            if (!declaration || !declaration->type)
                fail(Error::Code::InvalidMetadata, "Atomic Dictionary metadata contains an attribute without a physical type");
            attributes.emplace_back(declaration->name, DataTypeFactory::instance().get(declaration->type));
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
        fail(Error::Code::InvalidMetadata, "Atomic Dictionary metadata contains an invalid physical attribute type");
    }
    return attributes;
}

Digest computeTrustedPhysicalSchemaFingerprint(const ASTCreateQuery & create, const AtomicStoredObjectUDTMetadataValidatorLimits & limits)
{
    try
    {
        if (create.is_dictionary)
            return computeDictionaryAttributePhysicalSchemaFingerprint(
                decodeDictionaryPhysicalAttributes(create), limits.dictionary_attributes);
        return computeViewOutputPhysicalSchemaFingerprint(decodeViewPhysicalOutputs(create), limits.view_outputs);
    }
    catch (const Error &)
    {
        throw;
    }
    catch (const ViewOutputTypeBindingError & error)
    {
        if (error.code == ViewOutputTypeBindingError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "Atomic View metadata output-binding limits are invalid");
        if (error.code == ViewOutputTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic View metadata physical outputs exceed their limit");
        fail(Error::Code::InvalidMetadata, "Atomic View metadata physical outputs are invalid");
    }
    catch (const DictionaryAttributeTypeBindingError & error)
    {
        if (error.code == DictionaryAttributeTypeBindingError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "Atomic Dictionary metadata attribute-binding limits are invalid");
        if (error.code == DictionaryAttributeTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic Dictionary metadata physical attributes exceed their limit");
        fail(Error::Code::InvalidMetadata, "Atomic Dictionary metadata physical attributes are invalid");
    }
}

Digest
computeTrustedMixedPhysicalSchemaFingerprint(const ASTCreateQuery & create, const AtomicStoredObjectUDTMetadataValidatorLimits & limits)
{
    if (create.is_dictionary)
        return computeTrustedPhysicalSchemaFingerprint(create, limits);
    try
    {
        const auto auxiliary = collectViewAuxiliaryPhysicalTypeBindings(create, limits.view_outputs);
        return computeViewMixedPhysicalSchemaFingerprint(decodeViewPhysicalOutputs(create), auxiliary, limits.view_outputs);
    }
    catch (const Error &)
    {
        throw;
    }
    catch (const StoredObjectTypeBindingPreparationError & error)
    {
        if (error.code == StoredObjectTypeBindingPreparationError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic View auxiliary endpoints exceed their limit");
        fail(Error::Code::InvalidMetadata, "Atomic View auxiliary endpoints are not exact physical slots");
    }
    catch (const ViewOutputTypeBindingError & error)
    {
        if (error.code == ViewOutputTypeBindingError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "Atomic View mixed-binding limits are invalid");
        if (error.code == ViewOutputTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic View mixed physical schema exceeds its limit");
        fail(Error::Code::InvalidMetadata, "Atomic View mixed physical schema is invalid");
    }
}

bool usesMixedViewPhysicalSchema(const PersistedTypeReferences & references) noexcept
{
    return references.format_version == persisted_type_references_format_version_v2
        || references.path_dictionary_version == persisted_type_path_dictionary_version_v2;
}

class StartupStoredObjectRootAuthorityAdapter final : public IAuthorityAdapter
{
public:
    explicit StartupStoredObjectRootAuthorityAdapter(const AuthorityRoot & root_) noexcept
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
            fail(Error::Code::InvalidBase, "Atomic stored-object startup root lacks required capabilities");
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

bool storageMatchesObjectKind(const IStorage & storage, SchemaObjectKind kind) noexcept
{
    if (kind == SchemaObjectKind::Dictionary)
        return storage.isDictionary();
    if (kind == SchemaObjectKind::View)
        return storage.isView();
    return false;
}

bool auxiliaryEndpointTablesEqual(
    std::span<const ViewAuxiliaryPhysicalTypeBindingInput> lhs, std::span<const ViewAuxiliaryPhysicalTypeBindingInput> rhs) noexcept
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t index = 0; index < lhs.size(); ++index)
    {
        if (lhs[index].site != rhs[index].site || lhs[index].object_ordinal != rhs[index].object_ordinal
            || lhs[index].runtime_owner_key != rhs[index].runtime_owner_key || !lhs[index].physical_type || !rhs[index].physical_type
            || !lhs[index].physical_type->equals(*rhs[index].physical_type)
            || lhs[index].physical_type->getName() != rhs[index].physical_type->getName())
            return false;
    }
    return true;
}

SchemaObjectKind getSchemaObjectKind(const ASTCreateQuery & create)
{
    if (create.is_dictionary)
        return SchemaObjectKind::Dictionary;
    if (create.isView())
        return SchemaObjectKind::View;
    fail(Error::Code::InvalidMetadata, "Atomic stored-object metadata is neither a View nor a Dictionary");
}

String normalizeTrustedCanonicalMetadataImage(
    std::string_view metadata_bytes,
    const SchemaObjectID & trusted_object,
    const Digest & trusted_physical_schema_fingerprint,
    const Digest & trusted_mixed_physical_schema_fingerprint,
    const AtomicStoredObjectUDTMetadataValidatorLimits & limits)
{
    ASTPtr ast;
    try
    {
        ParserCreateQuery parser;
        ast = parseQuery(
            parser,
            metadata_bytes.data(),
            metadata_bytes.data() + metadata_bytes.size(),
            "Atomic trusted stored-object canonical metadata",
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
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object metadata has no complete canonical round-trip");
    }

    const auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    const bool kind_matches = create
        && ((trusted_object.kind == SchemaObjectKind::Dictionary && create->is_dictionary)
            || (trusted_object.kind == SchemaObjectKind::View && !create->is_dictionary && create->isView()));
    if (!kind_matches || create->isTemporary() || create->uuid != trusted_object.object_uuid || !create->has_uuid
        || !create->has_uuid_clause || !create->getDatabase().empty() || create->getTable() != TABLE_WITH_UUID_NAME_PLACEHOLDER
        || !create->cluster.empty())
    {
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object canonical round-trip changes its object identity");
    }

    try
    {
        if (computeTrustedPhysicalSchemaFingerprint(*create, limits) != trusted_physical_schema_fingerprint
            || computeTrustedMixedPhysicalSchemaFingerprint(*create, limits) != trusted_mixed_physical_schema_fingerprint)
        {
            fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object canonical round-trip changes its physical schema");
        }
    }
    catch (const Error & error)
    {
        if (error.code == Error::Code::InvalidConfiguration)
            throw;
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object canonical round-trip has invalid physical schema");
    }

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
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object metadata cannot complete its canonical round-trip");
    }
    if (canonical_metadata_bytes.empty() || canonical_metadata_bytes.size() > limits.maximum_metadata_bytes)
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object canonical round-trip exceeds its byte limit");
    return canonical_metadata_bytes;
}
}

AtomicStoredObjectUDTMetadataValidator::AtomicStoredObjectUDTMetadataValidator(
    UUID owning_database_uuid_,
    const ASTPtr & trusted_create_query,
    UInt64 object_schema_revision_,
    AtomicStoredObjectUDTMetadataValidatorLimits limits_)
    : owning_database_uuid(owning_database_uuid_)
    , object_schema_revision(object_schema_revision_)
    , limits(std::move(limits_))
{
    validateLimits(limits);
    if (owning_database_uuid == UUIDHelpers::Nil || !object_schema_revision)
        fail(Error::Code::InvalidConfiguration, "Atomic stored-object metadata validator has an invalid owner identity or revision");

    const auto * trusted_create = trusted_create_query ? trusted_create_query->as<ASTCreateQuery>() : nullptr;
    if (!trusted_create || trusted_create->isTemporary() || trusted_create->uuid == UUIDHelpers::Nil || trusted_create->getTable().empty()
        || !trusted_create->cluster.empty())
    {
        fail(Error::Code::InvalidConfiguration, "Atomic stored-object metadata validator has no complete trusted CREATE query");
    }

    trusted_object = {
        .kind = getSchemaObjectKind(*trusted_create),
        .database_uuid = owning_database_uuid,
        .object_uuid = trusted_create->uuid,
    };
    trusted_object_name = trusted_create->getTable();
    String serialized_trusted_metadata;
    try
    {
        serialized_trusted_metadata = getObjectDefinitionFromCreateQuery(trusted_create_query);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object metadata cannot be canonicalized");
    }
    if (serialized_trusted_metadata.empty() || serialized_trusted_metadata.size() > limits.maximum_metadata_bytes)
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object metadata exceeds its byte limit");
    trusted_physical_schema_fingerprint = computeTrustedPhysicalSchemaFingerprint(*trusted_create, limits);
    trusted_mixed_physical_schema_fingerprint = computeTrustedMixedPhysicalSchemaFingerprint(*trusted_create, limits);
    trusted_canonical_metadata_bytes = normalizeTrustedCanonicalMetadataImage(
        serialized_trusted_metadata,
        trusted_object,
        trusted_physical_schema_fingerprint,
        trusted_mixed_physical_schema_fingerprint,
        limits);
    const String stable_canonical_metadata_bytes = normalizeTrustedCanonicalMetadataImage(
        trusted_canonical_metadata_bytes,
        trusted_object,
        trusted_physical_schema_fingerprint,
        trusted_mixed_physical_schema_fingerprint,
        limits);
    if (stable_canonical_metadata_bytes != trusted_canonical_metadata_bytes)
        fail(Error::Code::InvalidConfiguration, "Atomic trusted stored-object metadata canonical round-trip is not stable");
}

IStoredObjectUDTMetadataValidator::DecodedMetadata AtomicStoredObjectUDTMetadataValidator::decodeAndCanonicalize(
    std::string_view candidate_metadata_bytes,
    std::string_view canonical_sidecar_bytes,
    const StoredObjectUDTMetadataValidationLimits & validation_limits) const
{
    const UInt64 maximum_metadata_bytes = std::min(limits.maximum_metadata_bytes, validation_limits.maximum_candidate_metadata_bytes);
    if (candidate_metadata_bytes.empty())
        fail(Error::Code::InvalidMetadata, "Atomic stored-object metadata is empty");
    if (candidate_metadata_bytes.size() > maximum_metadata_bytes)
        fail(Error::Code::LimitExceeded, "Atomic stored-object metadata exceeds its byte limit");

    auto references = decodeCanonicalSidecar(canonical_sidecar_bytes, limits.persisted_references);
    if (references.object != trusted_object || references.object_schema_revision != object_schema_revision)
        fail(Error::Code::InvalidSidecar, "Atomic stored-object metadata sidecar differs from its trusted object identity");
    const Digest trusted_fingerprint = trusted_object.kind == SchemaObjectKind::View && usesMixedViewPhysicalSchema(references)
        ? trusted_mixed_physical_schema_fingerprint
        : trusted_physical_schema_fingerprint;
    if (references.physical_schema_fingerprint != trusted_fingerprint)
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object metadata sidecar differs from its trusted physical schema");

    ASTPtr candidate_ast;
    try
    {
        ParserCreateQuery parser;
        candidate_ast = parseQuery(
            parser,
            candidate_metadata_bytes.data(),
            candidate_metadata_bytes.data() + candidate_metadata_bytes.size(),
            "Atomic user-defined type stored-object metadata",
            static_cast<size_t>(maximum_metadata_bytes),
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
        fail(Error::Code::InvalidMetadata, "Atomic stored-object metadata is not one complete CREATE-family query");
    }

    const auto * candidate_create = candidate_ast ? candidate_ast->as<ASTCreateQuery>() : nullptr;
    if (!candidate_create || candidate_create->uuid != trusted_object.object_uuid
        || getSchemaObjectKind(*candidate_create) != trusted_object.kind)
    {
        fail(Error::Code::InvalidMetadata, "Atomic stored-object metadata describes another object");
    }

    String canonical_metadata_bytes;
    try
    {
        canonical_metadata_bytes = getObjectDefinitionFromCreateQuery(candidate_ast);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidMetadata, "Atomic stored-object metadata cannot be canonicalized");
    }
    if (canonical_metadata_bytes.empty() || canonical_metadata_bytes.size() > validation_limits.maximum_canonical_metadata_bytes)
        fail(Error::Code::LimitExceeded, "Atomic canonical stored-object metadata exceeds its byte limit");
    if (canonical_metadata_bytes != trusted_canonical_metadata_bytes)
        fail(Error::Code::InvalidMetadata, "Atomic stored-object metadata semantics differ from its trusted CREATE image");

    try
    {
        BoundObjectPhysicalSchema reconstructed;
        if (trusted_object.kind == SchemaObjectKind::Dictionary)
        {
            reconstructed = reconstructDictionaryAttributePhysicalSchema(
                trusted_object,
                object_schema_revision,
                decodeDictionaryPhysicalAttributes(*candidate_create),
                references,
                limits.dictionary_attributes);
        }
        else if (usesMixedViewPhysicalSchema(references))
        {
            const auto auxiliary = collectViewAuxiliaryPhysicalTypeBindings(*candidate_create, limits.view_outputs);
            reconstructed = reconstructViewMixedPhysicalSchema(
                trusted_object,
                object_schema_revision,
                decodeViewPhysicalOutputs(*candidate_create),
                auxiliary,
                references,
                limits.view_outputs);
        }
        else
        {
            reconstructed = reconstructViewOutputPhysicalSchema(
                trusted_object, object_schema_revision, decodeViewPhysicalOutputs(*candidate_create), references, limits.view_outputs);
        }
        if (reconstructed.physical_schema_fingerprint != trusted_fingerprint)
            fail(Error::Code::IntegrityMismatch, "Atomic stored-object reconstructed schema fingerprint differs from its sidecar");
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
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object occurrence paths cannot be reconstructed");
    }

    Digest sidecar_hash{};
    try
    {
        sidecar_hash = computePersistedTypeReferencesSidecarHash(references, limits.persisted_references);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "Atomic stored-object sidecar hash input exceeds its limit");
        fail(Error::Code::InvalidSidecar, "Atomic stored-object sidecar cannot be hashed canonically");
    }

    return {
        .object = trusted_object,
        .object_schema_revision = object_schema_revision,
        .object_name = trusted_object_name,
        .sidecar_hash = sidecar_hash,
        .physical_schema_fingerprint = trusted_fingerprint,
        .canonical_metadata_bytes = std::move(canonical_metadata_bytes),
    };
}

AtomicStoredObjectUDTValidatedMetadataImage AtomicStoredObjectUDTMetadataValidator::validateCurrentMetadata(
    const SidecarExpectationRecord & expectation, std::string_view canonical_metadata_bytes, std::string_view canonical_sidecar_bytes) const
{
    if (expectation.object != trusted_object || expectation.object_schema_revision != object_schema_revision)
        fail(Error::Code::InvalidBase, "Atomic stored-object verification expectation differs from its trusted metadata identity");
    const auto validated = decodeAndCanonicalize(canonical_metadata_bytes, canonical_sidecar_bytes, {});
    if (validated.object != expectation.object || validated.object_schema_revision != expectation.object_schema_revision
        || validated.sidecar_hash != expectation.sidecar_hash
        || validated.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || validated.canonical_metadata_bytes != canonical_metadata_bytes)
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object verification metadata differs from its durable expectation");
    return {
        .object = validated.object,
        .object_schema_revision = validated.object_schema_revision,
        .sidecar_hash = validated.sidecar_hash,
        .physical_schema_fingerprint = validated.physical_schema_fingerprint,
    };
}

void AtomicStoredObjectUDTMetadataValidator::validateAndBindStartupMetadata(
    const AuthorityRoot & recovered_root,
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_metadata_bytes,
    std::string_view canonical_sidecar_bytes,
    const StoragePtr & trusted_storage) const
{
    if (!trusted_storage || recovered_root.getDatabaseUUID() != owning_database_uuid
        || recovered_root.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(Error::Code::InvalidBase, "Atomic stored-object startup has no matching dependent-object authority or storage");
    const auto * recovered_expectation = recovered_root.findExpectationRecord(expectation.object);
    if (!recovered_expectation || *recovered_expectation != expectation || expectation.object != trusted_object
        || expectation.object_schema_revision != object_schema_revision)
        fail(Error::Code::InvalidBase, "Atomic stored-object startup expectation differs from the recovered authority root");

    const auto storage_id = trusted_storage->getStorageID();
    if (storage_id.uuid != trusted_object.object_uuid || storage_id.table_name != trusted_object_name
        || !storageMatchesObjectKind(*trusted_storage, trusted_object.kind))
        fail(Error::Code::InvalidMetadata, "Atomic stored-object startup storage identity differs from its trusted metadata");

    const auto validated = decodeAndCanonicalize(canonical_metadata_bytes, canonical_sidecar_bytes, {});
    if (validated.object != expectation.object || validated.object_schema_revision != expectation.object_schema_revision
        || validated.sidecar_hash != expectation.sidecar_hash
        || validated.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || validated.canonical_metadata_bytes != canonical_metadata_bytes)
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object startup metadata differs from its durable expectation");

    auto references = decodeCanonicalSidecar(canonical_sidecar_bytes, limits.persisted_references);
    ASTPtr metadata_ast;
    try
    {
        ParserCreateQuery parser;
        metadata_ast = parseQuery(
            parser,
            canonical_metadata_bytes.data(),
            canonical_metadata_bytes.data() + canonical_metadata_bytes.size(),
            "Atomic user-defined type stored-object startup metadata",
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
        fail(Error::Code::InvalidMetadata, "Atomic stored-object startup metadata cannot be parsed for binding");
    }
    const auto * create = metadata_ast ? metadata_ast->as<ASTCreateQuery>() : nullptr;
    if (!create || create->uuid != trusted_object.object_uuid || getSchemaObjectKind(*create) != trusted_object.kind)
        fail(Error::Code::InvalidMetadata, "Atomic stored-object startup metadata describes another object");

    BoundObjectPhysicalSchema physical_schema;
    NamesAndTypesList canonical_physical_columns;
    std::vector<ViewAuxiliaryPhysicalTypeBindingInput> canonical_auxiliary_endpoints;
    try
    {
        if (trusted_object.kind == SchemaObjectKind::Dictionary)
        {
            canonical_physical_columns = decodeDictionaryPhysicalAttributes(*create);
            physical_schema = reconstructDictionaryAttributePhysicalSchema(
                trusted_object, object_schema_revision, canonical_physical_columns, references, limits.dictionary_attributes);
        }
        else
        {
            canonical_physical_columns = decodeViewPhysicalOutputs(*create);
            canonical_auxiliary_endpoints = collectViewAuxiliaryPhysicalTypeBindings(*create, limits.view_outputs);
            if (usesMixedViewPhysicalSchema(references))
            {
                physical_schema = reconstructViewMixedPhysicalSchema(
                    trusted_object,
                    object_schema_revision,
                    canonical_physical_columns,
                    canonical_auxiliary_endpoints,
                    references,
                    limits.view_outputs);
            }
            else
                physical_schema = reconstructViewOutputPhysicalSchema(
                    trusted_object, object_schema_revision, canonical_physical_columns, references, limits.view_outputs);
        }
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object startup declaration paths cannot be reconstructed");
    }

    BoundObjectTypeReferences::Ptr bound_references;
    try
    {
        StartupStoredObjectRootAuthorityAdapter authority(recovered_root);
        bound_references = BoundObjectTypeReferences::bind(references, std::move(physical_schema), authority);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::DefinitionMismatch, "Atomic stored-object startup sidecar cannot bind against the recovered root");
    }
    if (!bound_references || bound_references->getObject() != expectation.object
        || bound_references->getObjectSchemaRevision() != expectation.object_schema_revision
        || bound_references->getSidecarHash() != expectation.sidecar_hash
        || bound_references->getPhysicalSchemaFingerprint() != expectation.physical_schema_fingerprint)
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object startup binding differs from its durable expectation");

    AuthorityVerificationStamp::Ptr verification_stamp;
    try
    {
        verification_stamp
            = verifyAndCreateAuthorityVerificationStamp(recovered_root, expectation, canonical_sidecar_bytes, *bound_references);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object startup binding cannot be stamped against its exact root");
    }

    /// Stored-object UDT bindings and physical columns belong to the outer
    /// storage metadata. `StorageMaterializedView::getInMemoryMetadataPtr`
    /// resolves its target through the catalog, which is not allowed while
    /// Atomic startup holds the schema-mutation mutex and target startup may
    /// need that mutex.
    auto metadata_snapshot = trusted_storage->IStorage::getInMemoryMetadataPtr(nullptr, true);
    if (!metadata_snapshot)
        fail(Error::Code::InvalidMetadata, "Atomic stored-object startup storage has no runtime metadata snapshot");
    if (metadata_snapshot->getColumns().getAllPhysical() != canonical_physical_columns)
        fail(Error::Code::IntegrityMismatch, "Atomic stored-object runtime columns differ from canonical physical declarations");
    StorageInMemoryMetadata replacement(*metadata_snapshot);
    if (trusted_object.kind == SchemaObjectKind::View)
    {
        try
        {
            const auto & select = replacement.getSelectQuery();
            if (!select.select_query && !select.inner_query)
                fail(Error::Code::IntegrityMismatch, "Atomic View runtime metadata has no stored SELECT clone");

            if (select.select_query)
            {
                const auto runtime_auxiliary_endpoints
                    = collectViewAuxiliaryPhysicalTypeBindingsFromStoredSelect(select.select_query, limits.view_outputs);
                if (!auxiliaryEndpointTablesEqual(canonical_auxiliary_endpoints, runtime_auxiliary_endpoints))
                {
                    fail(
                        Error::Code::IntegrityMismatch,
                        "Atomic View runtime full SELECT differs from its canonical auxiliary endpoint table");
                }
            }

            if (select.inner_query)
            {
                const std::vector<ViewAuxiliaryPhysicalTypeBindingInput> * expected_endpoints = &canonical_auxiliary_endpoints;
                std::vector<ViewAuxiliaryPhysicalTypeBindingInput> canonical_first_arm_endpoints;
                if (select.select_query)
                {
                    const auto * canonical_union = create->select ? create->select->as<ASTSelectWithUnionQuery>() : nullptr;
                    if (!canonical_union || !canonical_union->list_of_selects || canonical_union->list_of_selects->children.empty())
                    {
                        fail(Error::Code::IntegrityMismatch, "Atomic MaterializedView canonical SELECT has no first execution arm");
                    }
                    canonical_first_arm_endpoints = collectViewAuxiliaryPhysicalTypeBindingsFromStoredSelect(
                        canonical_union->list_of_selects->children.front(), limits.view_outputs);
                    expected_endpoints = &canonical_first_arm_endpoints;
                }

                const auto runtime_inner_endpoints
                    = collectViewAuxiliaryPhysicalTypeBindingsFromStoredSelect(select.inner_query, limits.view_outputs);
                if (!auxiliaryEndpointTablesEqual(*expected_endpoints, runtime_inner_endpoints))
                {
                    fail(
                        Error::Code::IntegrityMismatch,
                        "Atomic View runtime execution SELECT differs from its canonical auxiliary endpoint table");
                }
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
            fail(Error::Code::IntegrityMismatch, "Atomic View runtime SELECT cannot retain exact stored-expression owners");
        }
    }
    replacement.setColumnsAndBoundStoredObjectUDTReferences(metadata_snapshot->getColumns(), std::move(bound_references), expectation);
    replacement.setBoundUDTVerificationStamp(std::move(verification_stamp));
    trusted_storage->setInMemoryMetadata(replacement);
}
}
