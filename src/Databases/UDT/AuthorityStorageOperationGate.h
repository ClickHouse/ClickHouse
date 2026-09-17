#pragma once

#include <Databases/UDT/AuthorityQuarantineAdmission.h>

#include <Storages/StorageInMemoryMetadata.h>

#include <memory>
#include <string_view>
#include <utility>

namespace DB
{

class DatabaseAtomic;
class IDatabase;
class IStorage;

namespace UDT
{

class AuthorityVerificationRuntimeState;

/// Compact query-lifetime evidence captured when a mapped storage snapshot is
/// first admitted. It retains no authority/root handle: ordinary reads keep
/// only their immutable metadata stamp and exact root identities.
class AuthorityStorageReadContinuationEvidence final
{
public:
    using Ptr = std::shared_ptr<const AuthorityStorageReadContinuationEvidence>;

    const AuthorityRootGraphIdentity & getPinnedRoot() const noexcept { return pinned_root; }
    const AuthorityObjectImageIdentity & getObjectImage() const noexcept { return object_image; }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }

private:
    friend class DB::DatabaseAtomic;

    AuthorityStorageReadContinuationEvidence(
        AuthorityRootGraphIdentity pinned_root_,
        AuthorityObjectImageIdentity object_image_,
        AuthorityVerificationStamp::Ptr verification_stamp_) noexcept;

    const AuthorityRootGraphIdentity pinned_root;
    const AuthorityObjectImageIdentity object_image;
    const AuthorityVerificationStamp::Ptr verification_stamp;
};

/// Short-lived final-publication fence for mapped non-read storage work. It
/// retains the owning database and serializes the exact engine commit point
/// with quarantine/fail-closed publication. An empty guard is the zero-cost
/// physical-only result.
class AuthorityStorageNewOperationCommitGuard final
{
public:
    AuthorityStorageNewOperationCommitGuard() = default;
    AuthorityStorageNewOperationCommitGuard(const AuthorityStorageNewOperationCommitGuard &) = delete;
    AuthorityStorageNewOperationCommitGuard & operator=(const AuthorityStorageNewOperationCommitGuard &) = delete;
    AuthorityStorageNewOperationCommitGuard(AuthorityStorageNewOperationCommitGuard && other) noexcept;
    AuthorityStorageNewOperationCommitGuard & operator=(AuthorityStorageNewOperationCommitGuard &&) noexcept = delete;
    ~AuthorityStorageNewOperationCommitGuard();

    explicit operator bool() const noexcept { return runtime != nullptr; }

private:
    friend class DB::DatabaseAtomic;

    AuthorityStorageNewOperationCommitGuard(
        std::shared_ptr<const IDatabase> database_owner_, const AuthorityVerificationRuntimeState * runtime_);

    /// Destruction explicitly releases the runtime fence before this owner.
    std::shared_ptr<const IDatabase> database_owner;
    const AuthorityVerificationRuntimeState * runtime = nullptr;
};

/// StorageSnapshot boundary for new mapped reads. Physical-only metadata is a
/// no-op. A mapped image without an exact current verification stamp fails
/// closed before a query can retain it.
[[nodiscard]] AuthorityStorageReadContinuationEvidence::Ptr
acquireAuthorityStorageReadContinuationEvidence(const IStorage & storage, const StorageMetadataPtr & metadata);

/// Authoritative initiation boundary for a new mapped write, mutation, DDL,
/// or ATTACH. Physical-only metadata is a no-op; mapped metadata resolves and
/// gates against its owning Atomic database.
void assertAuthorityStorageNewOperationAllowed(
    const IStorage & storage, const StorageMetadataPtr & metadata, AuthorityQuarantineOperationKind kind);

/// Final commit boundary for mapped writes/mutations/DDL/ATTACH. The exact
/// new-operation gate is evaluated while retaining the runtime publication
/// fence; engine-visible state must be published before the guard is released.
[[nodiscard]] AuthorityStorageNewOperationCommitGuard acquireAuthorityStorageNewOperationCommitGuard(
    const IStorage & storage, const StorageMetadataPtr & metadata, AuthorityQuarantineOperationKind kind);

/// Rejects an independently addressed generated physical inner table while a
/// mapped MaterializedView still owns it. Ordinary names and non-Atomic
/// databases take a cheap no-op path.
void assertAuthorityOwnedInnerStorageOperationAllowed(const std::shared_ptr<IStorage> & storage, std::string_view operation);

}
}
