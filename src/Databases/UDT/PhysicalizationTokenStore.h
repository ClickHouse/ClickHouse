#pragma once

#include <Databases/UDT/PhysicalizationPlan.h>

#include <Core/Types.h>

#include <array>
#include <functional>
#include <map>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt16 physicalization_token_format_version = 1;

/// Invoked while the token-store mutex is held, immediately before the expiry
/// comparison which authorizes point-of-no-return consumption. Production
/// callbacks must be bounded and must not re-enter the same token store.
using PhysicalizationMonotonicClock = std::function<UInt64()>;

struct PhysicalizationTokenStoreLimits
{
    UInt64 maximum_outstanding_tokens = 1'024;
    UInt64 maximum_aggregate_record_bytes = 1ULL << 20;
    UInt64 maximum_tokens_per_principal = 64;
    UInt64 maximum_record_bytes_per_principal = 256ULL << 10;
    UInt64 maximum_record_bytes = 4ULL << 10;
    UInt64 maximum_ttl_microseconds = 15ULL * 60 * 1'000'000;
    UInt64 maximum_entropy_attempts = 8;
};

class IPhysicalizationEntropySource
{
public:
    virtual ~IPhysicalizationEntropySource() = default;
    virtual void fill(std::span<CanonicalByte> bytes) = 0;
};

IPhysicalizationEntropySource & defaultPhysicalizationEntropySource();

class PhysicalizationTokenStoreError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidPlan,
        InvalidPrincipal,
        LimitExceeded,
        EntropyFailure,
        TokenRejected,
    };

    PhysicalizationTokenStoreError(Code code_, std::string_view message);

    const Code code;
};

/// Compact binding returned to the apply path. The store never returns or
/// retains the expanded scope, manifest, authority root, or a resolution
/// lease. Apply must inspect, rebuild and authorize a fresh plan under the
/// schema-mutation guard, demand matches(), then atomically consume the exact
/// operation identity before its first durable side effect.
class PhysicalizationApplyBinding final
{
public:
    const UUID & getOperationID() const noexcept { return operation_id; }
    const UUID & getPrincipalUUID() const noexcept { return principal_uuid; }
    const PhysicalizationSelector & getSelector() const noexcept { return selector; }
    const UUID & getDatabaseUUID() const noexcept { return database_uuid; }
    UInt64 getDatabaseCatalogEpoch() const noexcept { return database_catalog_epoch; }
    const Digest & getInventoryRoot() const noexcept { return inventory_root; }
    const Digest & getScopeDigest() const noexcept { return scope_digest; }
    UInt64 getScopeCount() const noexcept { return scope_count; }
    UInt64 getScopeBytes() const noexcept { return scope_bytes; }
    const Digest & getManifestDigest() const noexcept { return manifest_digest; }
    UInt64 getManifestCount() const noexcept { return manifest_count; }
    UInt64 getManifestBytes() const noexcept { return manifest_bytes; }
    UInt64 getExpiresAtMicroseconds() const noexcept { return expires_at_microseconds; }

    bool matches(const PhysicalizationPlan & plan) const noexcept;

private:
    friend class PhysicalizationTokenStore;

    UUID operation_id = UUIDHelpers::Nil;
    UUID principal_uuid = UUIDHelpers::Nil;
    PhysicalizationSelector selector;
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 database_catalog_epoch = 0;
    Digest inventory_root{};
    Digest scope_digest{};
    UInt64 scope_count = 0;
    UInt64 scope_bytes = 0;
    Digest manifest_digest{};
    UInt64 manifest_count = 0;
    UInt64 manifest_bytes = 0;
    UInt64 expires_at_microseconds = 0;
};

/// Database-owned, in-memory, restart-invalidated token registry. A successful
/// consume removes the exact inspected operation atomically, preventing
/// concurrent replay. Inspection and wrong-principal probes do not consume or
/// disclose another principal's token.
class PhysicalizationTokenStore final
{
public:
    explicit PhysicalizationTokenStore(
        UUID database_uuid_,
        PhysicalizationTokenStoreLimits limits_ = {},
        IPhysicalizationEntropySource & entropy_source_ = defaultPhysicalizationEntropySource());

    String
    issue(const PhysicalizationPlan & plan, UUID principal_uuid, UInt64 now_microseconds, UInt64 ttl_microseconds = 15ULL * 60 * 1'000'000);

    PhysicalizationApplyBinding inspectForApply(std::string_view opaque_token, UUID authenticated_principal_uuid, UInt64 now_microseconds);

    PhysicalizationApplyBinding
    consumeForApply(std::string_view opaque_token, UUID authenticated_principal_uuid, UUID expected_operation_id, UInt64 now_microseconds);

    /// Production point-of-no-return form. The clock is sampled after acquiring
    /// the store mutex, so rebuilding the plan or waiting for consume
    /// serialization cannot extend a token's lifetime.
    PhysicalizationApplyBinding consumeForApply(
        std::string_view opaque_token,
        UUID authenticated_principal_uuid,
        UUID expected_operation_id,
        const PhysicalizationMonotonicClock & monotonic_clock);

    /// Removes an issued token whose production routing registration failed.
    /// A wrong principal or unknown token is deliberately a no-op.
    void discard(std::string_view opaque_token, UUID authenticated_principal_uuid) noexcept;

    /// Server restart invalidates every token. This operation is noexcept and
    /// performs no durable write.
    void invalidateAllForRestart();

    UInt64 getOutstandingTokenCount() const;
    UInt64 getOutstandingRecordBytes() const;

private:
    struct PrincipalUsage
    {
        UInt64 token_count = 0;
        UInt64 record_bytes = 0;
    };

    struct TokenRecord
    {
        PhysicalizationApplyBinding binding;
        UInt64 accounted_bytes = 0;
    };

    void removeRecord(std::map<String, TokenRecord>::iterator record) noexcept;
    void pruneExpired(UInt64 now_microseconds) noexcept;
    PhysicalizationApplyBinding consumeForApplyLocked(
        std::string_view opaque_token, UUID authenticated_principal_uuid, UUID expected_operation_id, UInt64 now_microseconds);

    const UUID database_uuid;
    const PhysicalizationTokenStoreLimits limits;
    IPhysicalizationEntropySource & entropy_source;
    mutable std::mutex mutex;
    std::map<String, TokenRecord> records;
    std::map<UUID, PrincipalUsage> principal_usage;
    UInt64 aggregate_record_bytes = 0;
};

/// Process-local routing index for the SQL APPLY form, whose token is
/// intentionally opaque and carries no database name. The authoritative
/// binding and every replay/principal/root check remain in the database-owned
/// token store; this bounded index reveals only which live database should
/// inspect the token. Restart invalidates it together with all stores.
class PhysicalizationTokenRouter final
{
public:
    static void registerToken(
        std::string_view opaque_token, UUID database_uuid, UUID principal_uuid, UInt64 now_microseconds, UInt64 expires_at_microseconds);
    static UUID resolveDatabase(std::string_view opaque_token, UUID authenticated_principal_uuid, UInt64 now_microseconds);
    static void unregisterToken(std::string_view opaque_token, UUID database_uuid) noexcept;
    static void unregisterDatabase(UUID database_uuid) noexcept;

private:
    PhysicalizationTokenRouter() = delete;
};

}
