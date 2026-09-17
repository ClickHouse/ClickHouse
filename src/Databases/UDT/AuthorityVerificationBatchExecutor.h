#pragma once

#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityIntegrityVerifier.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>

#include <Core/Types.h>

#include <chrono>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string_view>

namespace DB
{

class DatabaseAtomic;

namespace UDT
{

class AtomicDatabaseSchemaMutationStorage;
class AuthorityRepairCoordinator;

struct AuthorityVerificationBatchExecutorLimits
{
    AuthorityIntegrityVerifierLimits object_verifier;
    /// Process-local cooperative cap on newly terminal targets in this
    /// invocation. A sealed prefix does not consume it again.
    UInt64 maximum_terminal_targets = 1'024;
    std::stop_token cancellation;
    /// Absolute cooperative deadlines checked at durable target boundaries.
    /// A target is never interrupted halfway through its integrity invariants.
    std::optional<std::chrono::steady_clock::time_point> monotonic_deadline;
    std::optional<UInt64> thread_cpu_deadline_nanoseconds;
};

/// Storage-owned immutable byte view used only while constructing one cached
/// exact-root schedule snapshot. It is not verification evidence: execution
/// independently rereads and accounts the selected durable files.
struct AuthorityVerificationTargetArtifactView
{
    std::optional<std::string_view> authority_record;
    std::optional<std::string_view> installation_record;
    std::optional<std::string_view> persisted_references;
    std::optional<std::string_view> metadata;
    UInt64 retained_bytes = 0;
    AuthorityVerificationTargetCost source_cost;
};

class AuthorityVerificationBatchExecutorError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidRoot,
        InvalidPlan,
        InvalidTrustedBatch,
        ArithmeticOverflow,
    };

    AuthorityVerificationBatchExecutorError(Code code_, std::string_view message);

    const Code code;
};

/// Move-only process-local capability minted only by DatabaseAtomic while it
/// owns the UDT schema-mutation mutex. It contains no caller-provided bytes or
/// success assertions: the executor uses the bound concrete database/storage
/// pair to pull and release one admitted durable target at a time.
class AuthorityVerificationTrustedBatch final
{
public:
    AuthorityVerificationTrustedBatch(const AuthorityVerificationTrustedBatch &) = delete;
    AuthorityVerificationTrustedBatch & operator=(const AuthorityVerificationTrustedBatch &) = delete;
    AuthorityVerificationTrustedBatch(AuthorityVerificationTrustedBatch &&) noexcept = default;
    AuthorityVerificationTrustedBatch & operator=(AuthorityVerificationTrustedBatch &&) noexcept = default;

private:
    friend class DB::DatabaseAtomic;
    friend class AuthorityVerificationBatchExecutor;
    friend class AuthorityRepairCoordinator;

    AuthorityVerificationTrustedBatch(
        DB::DatabaseAtomic & database_, AtomicDatabaseSchemaMutationStorage & storage_, std::unique_lock<std::mutex> schema_lock_) noexcept;

    DB::DatabaseAtomic * database;
    AtomicDatabaseSchemaMutationStorage * storage;
    std::unique_lock<std::mutex> schema_lock;
};

/// Concrete sealed executor for periodic verification. DatabaseAtomic retains
/// the move-only exact-root hazard pin and schema capability through receipt
/// consumption; the executor independently derives the complete rooted
/// inventory digest, pulls ordered admitted artifacts from the concrete Atomic
/// database storage, independently derives current object images, and is the
/// only class able to mint a receipt.
class AuthorityVerificationBatchExecutor final
{
public:
    AuthorityVerificationBatchExecutor() = delete;

    [[nodiscard]] static AuthorityVerificationBatchReceipt::Ptr execute(
        DB::DatabaseAtomic & database,
        const AuthorityVerificationBatchPlan & plan,
        const AuthorityVerificationBatchExecutorLimits & limits = {},
        const AuthorityVerificationBatchReceipt * verified_prefix = nullptr);

    /// Computes the prospective charge for the exact storage-owned artifact
    /// snapshot. A later file change cannot turn this into success: execution
    /// stops before exceeding the declaration and the scheduler rebuilds the
    /// snapshot. This method performs no publication and mints no receipt.
    [[nodiscard]] static AuthorityVerificationTargetCost estimateTrustedTargetCost(
        const AuthorityRoot & authority,
        const AuthorityInventoryLeaf & leaf,
        const AuthorityVerificationTargetArtifactView & input,
        const AuthorityVerificationBatchExecutorLimits & limits = {});

private:
    friend class DB::DatabaseAtomic;
    friend class AuthorityRepairCoordinator;

    [[nodiscard]] static AuthorityVerificationBatchReceipt::Ptr executeTrusted(
        AtomicAuthority::RootSnapshot & pinned_root,
        const AuthorityVerificationBatchPlan & plan,
        AuthorityVerificationTrustedBatch & trusted_batch,
        const AuthorityVerificationBatchExecutorLimits & limits,
        const AuthorityVerificationBatchReceipt * verified_prefix = nullptr);
};

}
}
