#include <Databases/UDT/AuthorityStorageOperationGate.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>

#include <Interpreters/DatabaseCatalog.h>

#include <Storages/IStorage.h>

#include <Common/Exception.h>

#include <string_view>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int ABORTED;
}

namespace UDT
{
namespace
{

void validateMappedStorageIdentity(
    const IStorage & storage, const StorageMetadataPtr & metadata, const DatabaseAtomic & database, std::string_view boundary)
{
    const auto & bound = metadata->getBoundUDTReferences();
    if (!bound)
        return;

    const auto storage_id = storage.getStorageID();
    const auto & object = bound->getObject();
    const bool ambiguous_kind = storage.isView() && storage.isDictionary();
    const auto expected_kind = storage.isDictionary() ? SchemaObjectKind::Dictionary
        : storage.isView()                            ? SchemaObjectKind::View
                                                      : SchemaObjectKind::Table;
    if (ambiguous_kind || !storage_id.hasUUID() || object.database_uuid != database.getUUID() || object.object_uuid != storage_id.uuid
        || object.kind != expected_kind)
    {
        throw Exception(
            ErrorCodes::ABORTED,
            "Mapped UDT storage metadata does not belong to storage {} at its {} boundary",
            storage_id.getNameForLogs(),
            boundary);
    }
}

}

AuthorityStorageNewOperationCommitGuard::AuthorityStorageNewOperationCommitGuard(
    std::shared_ptr<const IDatabase> database_owner_, const AuthorityVerificationRuntimeState * runtime_)
    : database_owner(std::move(database_owner_))
    , runtime(runtime_)
{
    if (!database_owner || !runtime)
        throw Exception(ErrorCodes::ABORTED, "Mapped UDT storage final commit has no active runtime owner");
    runtime->acquireNewOperationCommitFence();
}

AuthorityStorageNewOperationCommitGuard::AuthorityStorageNewOperationCommitGuard(AuthorityStorageNewOperationCommitGuard && other) noexcept
    : database_owner(std::move(other.database_owner))
    , runtime(std::exchange(other.runtime, nullptr))
{
}

AuthorityStorageNewOperationCommitGuard::~AuthorityStorageNewOperationCommitGuard()
{
    if (runtime)
        runtime->releaseNewOperationCommitFence();
}

AuthorityStorageReadContinuationEvidence::AuthorityStorageReadContinuationEvidence(
    AuthorityRootGraphIdentity pinned_root_,
    AuthorityObjectImageIdentity object_image_,
    AuthorityVerificationStamp::Ptr verification_stamp_) noexcept
    : pinned_root(std::move(pinned_root_))
    , object_image(std::move(object_image_))
    , verification_stamp(std::move(verification_stamp_))
{
}

AuthorityStorageReadContinuationEvidence::Ptr
acquireAuthorityStorageReadContinuationEvidence(const IStorage & storage, const StorageMetadataPtr & metadata)
{
    if (!metadata || !metadata->getBoundUDTReferences())
        return {};

    metadata->validateBoundUDTReferences();
    const auto storage_id = storage.getStorageID();
    if (storage_id.database_name.empty())
        throw Exception(ErrorCodes::ABORTED, "Mapped UDT storage has no owning database at its read boundary");
    const auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.database_name);
    const auto atomic = std::dynamic_pointer_cast<DatabaseAtomic>(database);
    if (!atomic)
        throw Exception(ErrorCodes::ABORTED, "Mapped UDT storage has no active Atomic database at its read boundary");
    validateMappedStorageIdentity(storage, metadata, *atomic, "read");
    return atomic->acquireUDTStorageReadContinuationEvidence(metadata);
}

void assertAuthorityStorageNewOperationAllowed(
    const IStorage & storage, const StorageMetadataPtr & metadata, AuthorityQuarantineOperationKind kind)
{
    if (!metadata || !metadata->getBoundUDTReferences())
        return;

    metadata->validateBoundUDTReferences();
    const auto storage_id = storage.getStorageID();
    if (storage_id.database_name.empty())
        throw Exception(ErrorCodes::ABORTED, "Mapped UDT storage has no owning database at its operation boundary");
    const auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.database_name);
    const auto atomic = std::dynamic_pointer_cast<DatabaseAtomic>(database);
    if (!atomic)
        throw Exception(ErrorCodes::ABORTED, "Mapped UDT storage has no active Atomic database at its operation boundary");
    validateMappedStorageIdentity(storage, metadata, *atomic, "operation");
    atomic->assertUDTNewStorageOperationAllowed(metadata, kind);
}

AuthorityStorageNewOperationCommitGuard acquireAuthorityStorageNewOperationCommitGuard(
    const IStorage & storage, const StorageMetadataPtr & metadata, AuthorityQuarantineOperationKind kind)
{
    if (!metadata || !metadata->getBoundUDTReferences())
        return {};

    metadata->validateBoundUDTReferences();
    const auto storage_id = storage.getStorageID();
    if (storage_id.database_name.empty())
        throw Exception(ErrorCodes::ABORTED, "Mapped UDT storage has no owning database at its final commit boundary");
    const auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.database_name);
    const auto atomic = std::dynamic_pointer_cast<DatabaseAtomic>(database);
    if (!atomic)
        throw Exception(ErrorCodes::ABORTED, "Mapped UDT storage has no active Atomic database at its final commit boundary");
    validateMappedStorageIdentity(storage, metadata, *atomic, "final commit");
    return atomic->acquireUDTNewStorageOperationCommitGuard(metadata, kind);
}

void assertAuthorityOwnedInnerStorageOperationAllowed(const std::shared_ptr<IStorage> & storage, std::string_view operation)
{
    if (!storage)
        return;

    const auto storage_id = storage->getStorageID();
    if ((!storage_id.table_name.starts_with(".inner_id.") && !storage_id.table_name.starts_with(".tmp.inner_id."))
        || storage_id.database_name.empty())
        return;

    const auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.database_name);
    if (const auto atomic = std::dynamic_pointer_cast<DatabaseAtomic>(database))
        atomic->assertUDTPhysicalInnerTableOperationAllowed(storage, operation);
}

}
}
