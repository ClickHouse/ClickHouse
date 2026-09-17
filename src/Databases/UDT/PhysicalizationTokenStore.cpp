#include <Databases/UDT/PhysicalizationTokenStore.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = PhysicalizationTokenStoreError;

constexpr std::string_view token_prefix = "udtpt1_";
constexpr size_t token_entropy_bytes = 32;
constexpr size_t operation_id_entropy_bytes = 16;
constexpr size_t total_entropy_bytes = token_entropy_bytes + operation_id_entropy_bytes;
constexpr size_t maximum_routed_tokens = 1ULL << 20;
constexpr size_t maximum_routed_tokens_per_principal = 4'096;
constexpr UInt64 maximum_routed_token_bytes = 128ULL << 20;
constexpr UInt64 routed_token_record_overhead = 128;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedSize(size_t value)
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

void checkedCharge(UInt64 & value, UInt64 additional, UInt64 maximum, Error::Code code, std::string_view message)
{
    if (value > maximum || additional > maximum - value)
        fail(code, message);
    value += additional;
}

bool isZero(std::span<const CanonicalByte> bytes) noexcept
{
    return std::all_of(bytes.begin(), bytes.end(), [](CanonicalByte byte) { return byte == 0; });
}

String toOpaqueToken(std::span<const CanonicalByte> entropy)
{
    static constexpr std::string_view digits = "0123456789abcdef";
    String result;
    result.reserve(token_prefix.size() + entropy.size() * 2);
    result.append(token_prefix);
    for (const CanonicalByte byte : entropy)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

bool hasCanonicalTokenSyntax(std::string_view token) noexcept
{
    if (!token.starts_with(token_prefix) || token.size() != token_prefix.size() + token_entropy_bytes * 2)
        return false;
    return std::all_of(
        token.begin() + static_cast<ptrdiff_t>(token_prefix.size()),
        token.end(),
        [](char character) { return (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'); });
}

UUID operationID(std::span<const CanonicalByte, operation_id_entropy_bytes> bytes) noexcept
{
    CanonicalUUID canonical{};
    std::copy(bytes.begin(), bytes.end(), canonical.begin());
    return uuidFromCanonicalBytes(canonical);
}

void validateLimits(const PhysicalizationTokenStoreLimits & limits)
{
    const PhysicalizationTokenStoreLimits maximum;
    if (!limits.maximum_outstanding_tokens || limits.maximum_outstanding_tokens > maximum.maximum_outstanding_tokens
        || !limits.maximum_aggregate_record_bytes || limits.maximum_aggregate_record_bytes > maximum.maximum_aggregate_record_bytes
        || !limits.maximum_tokens_per_principal || limits.maximum_tokens_per_principal > maximum.maximum_tokens_per_principal
        || !limits.maximum_record_bytes_per_principal
        || limits.maximum_record_bytes_per_principal > maximum.maximum_record_bytes_per_principal || !limits.maximum_record_bytes
        || limits.maximum_record_bytes > maximum.maximum_record_bytes || !limits.maximum_ttl_microseconds
        || limits.maximum_ttl_microseconds > maximum.maximum_ttl_microseconds || !limits.maximum_entropy_attempts
        || limits.maximum_entropy_attempts > maximum.maximum_entropy_attempts
        || limits.maximum_record_bytes > limits.maximum_record_bytes_per_principal
        || limits.maximum_record_bytes_per_principal > limits.maximum_aggregate_record_bytes
        || limits.maximum_tokens_per_principal > limits.maximum_outstanding_tokens)
        fail(Error::Code::InvalidConfiguration, "physicalization token-store limits are invalid");
}

class Encoder final
{
public:
    explicit Encoder(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    void byte(UInt8 value)
    {
        reserve(1);
        output.push_back(static_cast<char>(value));
    }

    void uint16(UInt16 value)
    {
        reserve(sizeof(value));
        for (size_t index = 0; index < sizeof(value); ++index)
            output.push_back(static_cast<char>(value >> (8 * index)));
    }

    void uint64(UInt64 value)
    {
        reserve(sizeof(value));
        for (size_t index = 0; index < sizeof(value); ++index)
            output.push_back(static_cast<char>(value >> (8 * index)));
    }

    void uuid(const UUID & value)
    {
        const auto bytes = uuidToCanonicalBytes(value);
        raw(bytes);
    }

    void digest(const Digest & value) { raw(value); }

    void object(const std::optional<SchemaObjectID> & value)
    {
        byte(value ? 1 : 0);
        if (!value)
            return;
        byte(static_cast<UInt8>(value->kind));
        uuid(value->database_uuid);
        uuid(value->object_uuid);
    }

    UInt64 size() const noexcept { return checkedSize(output.size()); }

private:
    template <size_t size>
    void raw(const std::array<CanonicalByte, size> & value)
    {
        reserve(size);
        output.append(reinterpret_cast<const char *>(value.data()), value.size());
    }

    void reserve(UInt64 additional)
    {
        const UInt64 current = checkedSize(output.size());
        if (current > maximum_bytes || additional > maximum_bytes - current)
            fail(Error::Code::LimitExceeded, "physicalization token record exceeds its byte limit");
    }

    const UInt64 maximum_bytes;
    String output;
};

UInt64 encodedRecordBytes(const PhysicalizationApplyBinding & binding, std::string_view opaque_token, UInt64 maximum_record_bytes)
{
    Encoder encoder(maximum_record_bytes);
    encoder.uint16(physicalization_token_format_version);
    encoder.uuid(binding.getOperationID());
    encoder.uuid(binding.getPrincipalUUID());
    encoder.byte(static_cast<UInt8>(binding.getSelector().scope));
    encoder.object(binding.getSelector().object);
    encoder.byte(binding.getSelector().drop_unused_types ? 1 : 0);
    encoder.uuid(binding.getDatabaseUUID());
    encoder.uint64(binding.getDatabaseCatalogEpoch());
    encoder.digest(binding.getInventoryRoot());
    encoder.digest(binding.getScopeDigest());
    encoder.uint64(binding.getScopeCount());
    encoder.uint64(binding.getScopeBytes());
    encoder.digest(binding.getManifestDigest());
    encoder.uint64(binding.getManifestCount());
    encoder.uint64(binding.getManifestBytes());
    encoder.uint64(binding.getExpiresAtMicroseconds());
    UInt64 result = encoder.size();
    checkedCharge(
        result,
        checkedSize(opaque_token.size()),
        maximum_record_bytes,
        Error::Code::LimitExceeded,
        "physicalization token record exceeds its byte limit");
    checkedCharge(result, 128, maximum_record_bytes, Error::Code::LimitExceeded, "physicalization token record exceeds its byte limit");
    return result;
}

class OpenSSLEntropySource final : public IPhysicalizationEntropySource
{
public:
    void fill(std::span<CanonicalByte> bytes) override
    {
        if (bytes.size() > static_cast<size_t>(std::numeric_limits<int>::max())
            || RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1)
            throw std::runtime_error("cryptographic entropy source failed");
    }
};

struct RoutedTokenRecord
{
    UUID database_uuid = UUIDHelpers::Nil;
    UUID principal_uuid = UUIDHelpers::Nil;
    UInt64 expires_at_microseconds = 0;

    bool operator==(const RoutedTokenRecord &) const = default;
};

using RoutedTokenExpiration = std::pair<UInt64, String>;
using RoutedTokenExpirationView = std::pair<UInt64, std::string_view>;

struct RoutedTokenExpirationLess
{
    using is_transparent = void;

    bool operator()(const RoutedTokenExpiration & lhs, const RoutedTokenExpiration & rhs) const noexcept
    {
        return lhs.first != rhs.first ? lhs.first < rhs.first : lhs.second < rhs.second;
    }

    bool operator()(const RoutedTokenExpiration & lhs, const RoutedTokenExpirationView & rhs) const noexcept
    {
        return lhs.first != rhs.first ? lhs.first < rhs.first : std::string_view(lhs.second) < rhs.second;
    }

    bool operator()(const RoutedTokenExpirationView & lhs, const RoutedTokenExpiration & rhs) const noexcept
    {
        return lhs.first != rhs.first ? lhs.first < rhs.first : lhs.second < std::string_view(rhs.second);
    }
};

struct TokenRouterState
{
    std::mutex mutex;
    std::map<String, RoutedTokenRecord, std::less<>> records;
    std::set<RoutedTokenExpiration, RoutedTokenExpirationLess> expiration_index;
    std::map<UUID, size_t> principal_token_counts;
    UInt64 aggregate_record_bytes = 0;
};

UInt64 routedTokenRecordBytes(std::string_view opaque_token)
{
    UInt64 result = checkedSize(opaque_token.size());
    checkedCharge(
        result,
        routed_token_record_overhead,
        maximum_routed_token_bytes,
        Error::Code::LimitExceeded,
        "physicalization routed-token bytes exceed their process limit");
    return result;
}

void eraseRoutedToken(TokenRouterState & state, std::map<String, RoutedTokenRecord, std::less<>>::iterator record) noexcept
{
    const auto expiration = state.expiration_index.find(RoutedTokenExpirationView{record->second.expires_at_microseconds, record->first});
    const auto usage = state.principal_token_counts.find(record->second.principal_uuid);
    const UInt64 accounted_bytes = checkedSize(record->first.size()) + routed_token_record_overhead;
    if (expiration == state.expiration_index.end() || usage == state.principal_token_counts.end() || usage->second == 0
        || state.aggregate_record_bytes < accounted_bytes)
        std::terminate();

    state.expiration_index.erase(expiration);
    --usage->second;
    if (usage->second == 0)
        state.principal_token_counts.erase(usage);
    state.aggregate_record_bytes -= accounted_bytes;
    state.records.erase(record);
}

void pruneExpiredRoutedTokens(TokenRouterState & state, UInt64 now_microseconds) noexcept
{
    while (!state.expiration_index.empty() && state.expiration_index.begin()->first <= now_microseconds)
    {
        const auto record = state.records.find(state.expiration_index.begin()->second);
        if (record == state.records.end() || record->second.expires_at_microseconds != state.expiration_index.begin()->first)
            std::terminate();
        eraseRoutedToken(state, record);
    }
}

TokenRouterState & tokenRouterState()
{
    static TokenRouterState state;
    return state;
}

}

IPhysicalizationEntropySource & defaultPhysicalizationEntropySource()
{
    static OpenSSLEntropySource source;
    return source;
}

PhysicalizationTokenStoreError::PhysicalizationTokenStoreError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

bool PhysicalizationApplyBinding::matches(const PhysicalizationPlan & plan) const noexcept
{
    return selector == plan.getSelector() && database_uuid == plan.getDatabaseUUID()
        && database_catalog_epoch == plan.getDatabaseCatalogEpoch() && inventory_root == plan.getInventoryRoot()
        && scope_digest == plan.getScopeDigest() && scope_count == plan.getScopeCount() && scope_bytes == plan.getScopeBytes()
        && manifest_digest == plan.getManifestDigest() && manifest_count == plan.getManifestCount()
        && manifest_bytes == plan.getManifestBytes();
}

PhysicalizationTokenStore::PhysicalizationTokenStore(
    UUID database_uuid_, PhysicalizationTokenStoreLimits limits_, IPhysicalizationEntropySource & entropy_source_)
    : database_uuid(database_uuid_)
    , limits(limits_)
    , entropy_source(entropy_source_)
{
    if (database_uuid == UUIDHelpers::Nil)
        fail(Error::Code::InvalidConfiguration, "physicalization token store has a nil database identity");
    validateLimits(limits);
}

String
PhysicalizationTokenStore::issue(const PhysicalizationPlan & plan, UUID principal_uuid, UInt64 now_microseconds, UInt64 ttl_microseconds)
{
    if (principal_uuid == UUIDHelpers::Nil)
        fail(Error::Code::InvalidPrincipal, "physicalization token principal identity is nil");
    const auto definitions = plan.getDefinitions();
    const bool definition_only_database_drop = plan.getObjects().empty() && plan.getScopeCount() == 0
        && plan.getSelector().scope == PhysicalizationScope::Database && plan.getSelector().drop_unused_types
        && std::any_of(definitions.begin(), definitions.end(), [](const auto & definition) { return definition.selected_for_drop; });
    if (plan.getDatabaseUUID() != database_uuid || (plan.getScopeCount() == 0 && !definition_only_database_drop)
        || plan.getManifestCount() == 0 || plan.getScopeBytes() == 0 || plan.getManifestBytes() == 0)
        fail(Error::Code::InvalidPlan, "physicalization token plan is empty or belongs to another database");
    if (!ttl_microseconds || ttl_microseconds > limits.maximum_ttl_microseconds
        || ttl_microseconds > std::numeric_limits<UInt64>::max() - now_microseconds)
        fail(Error::Code::InvalidPlan, "physicalization token lifetime is invalid");

    std::lock_guard lock(mutex);
    pruneExpired(now_microseconds);
    if (records.size() >= limits.maximum_outstanding_tokens)
        fail(Error::Code::LimitExceeded, "physicalization token count exceeds its database limit");

    const auto usage = principal_usage.find(principal_uuid);
    if (usage != principal_usage.end() && usage->second.token_count >= limits.maximum_tokens_per_principal)
        fail(Error::Code::LimitExceeded, "physicalization token count exceeds its principal limit");

    std::array<CanonicalByte, total_entropy_bytes> entropy{};
    String opaque_token;
    UUID operation_id = UUIDHelpers::Nil;
    bool unique = false;
    for (UInt64 attempt = 0; attempt < limits.maximum_entropy_attempts; ++attempt)
    {
        try
        {
            entropy_source.fill(entropy);
        }
        catch (const std::exception &)
        {
            fail(Error::Code::EntropyFailure, "physicalization token entropy source failed");
        }
        const std::span<const CanonicalByte> token_entropy(entropy.data(), token_entropy_bytes);
        const std::span<const CanonicalByte, operation_id_entropy_bytes> operation_entropy(
            entropy.data() + token_entropy_bytes, operation_id_entropy_bytes);
        if (isZero(token_entropy))
            continue;
        operation_id = operationID(operation_entropy);
        if (operation_id == UUIDHelpers::Nil)
            continue;
        opaque_token = toOpaqueToken(token_entropy);
        if (!records.contains(opaque_token))
        {
            unique = true;
            break;
        }
    }
    if (!unique)
        fail(Error::Code::EntropyFailure, "physicalization token entropy did not produce a unique identity");

    PhysicalizationApplyBinding binding;
    binding.operation_id = operation_id;
    binding.principal_uuid = principal_uuid;
    binding.selector = plan.getSelector();
    binding.database_uuid = plan.getDatabaseUUID();
    binding.database_catalog_epoch = plan.getDatabaseCatalogEpoch();
    binding.inventory_root = plan.getInventoryRoot();
    binding.scope_digest = plan.getScopeDigest();
    binding.scope_count = plan.getScopeCount();
    binding.scope_bytes = plan.getScopeBytes();
    binding.manifest_digest = plan.getManifestDigest();
    binding.manifest_count = plan.getManifestCount();
    binding.manifest_bytes = plan.getManifestBytes();
    binding.expires_at_microseconds = now_microseconds + ttl_microseconds;

    const UInt64 accounted_bytes = encodedRecordBytes(binding, opaque_token, limits.maximum_record_bytes);
    const PrincipalUsage current_usage = usage == principal_usage.end() ? PrincipalUsage{} : usage->second;
    if (current_usage.record_bytes > limits.maximum_record_bytes_per_principal
        || accounted_bytes > limits.maximum_record_bytes_per_principal - current_usage.record_bytes)
        fail(Error::Code::LimitExceeded, "physicalization token bytes exceed their principal limit");
    if (aggregate_record_bytes > limits.maximum_aggregate_record_bytes
        || accounted_bytes > limits.maximum_aggregate_record_bytes - aggregate_record_bytes)
        fail(Error::Code::LimitExceeded, "physicalization token bytes exceed their database limit");

    auto [updated_usage, inserted_usage] = principal_usage.try_emplace(principal_uuid);
    bool inserted_record = false;
    try
    {
        inserted_record = records
                              .emplace(
                                  opaque_token,
                                  TokenRecord{
                                      .binding = std::move(binding),
                                      .accounted_bytes = accounted_bytes,
                                  })
                              .second;
    }
    catch (...)
    {
        if (inserted_usage)
            principal_usage.erase(updated_usage);
        throw;
    }
    if (!inserted_record)
    {
        if (inserted_usage)
            principal_usage.erase(updated_usage);
        fail(Error::Code::EntropyFailure, "physicalization token identity was reused during insertion");
    }
    ++updated_usage->second.token_count;
    updated_usage->second.record_bytes += accounted_bytes;
    aggregate_record_bytes += accounted_bytes;
    return opaque_token;
}

PhysicalizationApplyBinding
PhysicalizationTokenStore::inspectForApply(std::string_view opaque_token, UUID authenticated_principal_uuid, UInt64 now_microseconds)
{
    if (authenticated_principal_uuid == UUIDHelpers::Nil || !hasCanonicalTokenSyntax(opaque_token))
        fail(Error::Code::TokenRejected, "physicalization token was rejected");

    std::lock_guard lock(mutex);
    auto found = records.find(String(opaque_token));
    if (found == records.end() || found->second.binding.principal_uuid != authenticated_principal_uuid)
        fail(Error::Code::TokenRejected, "physicalization token was rejected");
    if (found->second.binding.expires_at_microseconds <= now_microseconds)
    {
        removeRecord(found);
        fail(Error::Code::TokenRejected, "physicalization token was rejected");
    }

    return found->second.binding;
}

PhysicalizationApplyBinding PhysicalizationTokenStore::consumeForApply(
    std::string_view opaque_token, UUID authenticated_principal_uuid, UUID expected_operation_id, UInt64 now_microseconds)
{
    if (authenticated_principal_uuid == UUIDHelpers::Nil || expected_operation_id == UUIDHelpers::Nil
        || !hasCanonicalTokenSyntax(opaque_token))
        fail(Error::Code::TokenRejected, "physicalization token was rejected");

    std::lock_guard lock(mutex);
    return consumeForApplyLocked(opaque_token, authenticated_principal_uuid, expected_operation_id, now_microseconds);
}

PhysicalizationApplyBinding PhysicalizationTokenStore::consumeForApply(
    std::string_view opaque_token,
    UUID authenticated_principal_uuid,
    UUID expected_operation_id,
    const PhysicalizationMonotonicClock & monotonic_clock)
{
    if (authenticated_principal_uuid == UUIDHelpers::Nil || expected_operation_id == UUIDHelpers::Nil
        || !hasCanonicalTokenSyntax(opaque_token) || !monotonic_clock)
        fail(Error::Code::TokenRejected, "physicalization token was rejected");

    std::lock_guard lock(mutex);
    return consumeForApplyLocked(opaque_token, authenticated_principal_uuid, expected_operation_id, monotonic_clock());
}

PhysicalizationApplyBinding PhysicalizationTokenStore::consumeForApplyLocked(
    std::string_view opaque_token, UUID authenticated_principal_uuid, UUID expected_operation_id, UInt64 now_microseconds)
{
    auto found = records.find(String(opaque_token));
    if (found == records.end() || found->second.binding.principal_uuid != authenticated_principal_uuid
        || found->second.binding.operation_id != expected_operation_id)
        fail(Error::Code::TokenRejected, "physicalization token was rejected");
    if (found->second.binding.expires_at_microseconds <= now_microseconds)
    {
        removeRecord(found);
        fail(Error::Code::TokenRejected, "physicalization token was rejected");
    }

    auto binding = std::move(found->second.binding);
    removeRecord(found);
    return binding;
}

void PhysicalizationTokenStore::discard(std::string_view opaque_token, UUID authenticated_principal_uuid) noexcept
{
    if (authenticated_principal_uuid == UUIDHelpers::Nil || !hasCanonicalTokenSyntax(opaque_token))
        return;
    std::lock_guard lock(mutex);
    auto found = records.find(String(opaque_token));
    if (found != records.end() && found->second.binding.principal_uuid == authenticated_principal_uuid)
        removeRecord(found);
}

void PhysicalizationTokenStore::removeRecord(std::map<String, TokenRecord>::iterator record) noexcept
{
    const UUID principal_uuid = record->second.binding.principal_uuid;
    const UInt64 accounted_bytes = record->second.accounted_bytes;
    const auto usage = principal_usage.find(principal_uuid);
    if (usage == principal_usage.end() || usage->second.token_count == 0 || usage->second.record_bytes < accounted_bytes
        || aggregate_record_bytes < accounted_bytes)
        std::terminate();
    --usage->second.token_count;
    usage->second.record_bytes -= accounted_bytes;
    aggregate_record_bytes -= accounted_bytes;
    if (usage->second.token_count == 0)
        principal_usage.erase(usage);
    records.erase(record);
}

void PhysicalizationTokenStore::pruneExpired(UInt64 now_microseconds) noexcept
{
    for (auto record = records.begin(); record != records.end();)
    {
        if (record->second.binding.expires_at_microseconds > now_microseconds)
        {
            ++record;
            continue;
        }
        auto expired = record++;
        removeRecord(expired);
    }
}

void PhysicalizationTokenStore::invalidateAllForRestart()
{
    std::lock_guard lock(mutex);
    records.clear();
    principal_usage.clear();
    aggregate_record_bytes = 0;
}

UInt64 PhysicalizationTokenStore::getOutstandingTokenCount() const
{
    std::lock_guard lock(mutex);
    return checkedSize(records.size());
}

UInt64 PhysicalizationTokenStore::getOutstandingRecordBytes() const
{
    std::lock_guard lock(mutex);
    return aggregate_record_bytes;
}

void PhysicalizationTokenRouter::registerToken(
    std::string_view opaque_token, UUID database_uuid, UUID principal_uuid, UInt64 now_microseconds, UInt64 expires_at_microseconds)
{
    if (!hasCanonicalTokenSyntax(opaque_token) || database_uuid == UUIDHelpers::Nil || principal_uuid == UUIDHelpers::Nil
        || expires_at_microseconds <= now_microseconds)
        fail(Error::Code::TokenRejected, "physicalization token routing identity is invalid");

    auto & state = tokenRouterState();
    std::lock_guard lock(state.mutex);
    pruneExpiredRoutedTokens(state, now_microseconds);
    if (state.records.size() >= maximum_routed_tokens)
        fail(Error::Code::LimitExceeded, "physicalization routed-token count exceeds its process limit");

    const auto existing = state.records.find(opaque_token);
    if (existing != state.records.end())
    {
        if (existing->second != RoutedTokenRecord{database_uuid, principal_uuid, expires_at_microseconds})
            fail(Error::Code::EntropyFailure, "physicalization routed-token identity was reused");
        return;
    }

    const auto current_usage = state.principal_token_counts.find(principal_uuid);
    if (current_usage != state.principal_token_counts.end() && current_usage->second >= maximum_routed_tokens_per_principal)
        fail(Error::Code::LimitExceeded, "physicalization routed-token count exceeds its principal limit");
    const UInt64 accounted_bytes = routedTokenRecordBytes(opaque_token);
    if (state.aggregate_record_bytes > maximum_routed_token_bytes
        || accounted_bytes > maximum_routed_token_bytes - state.aggregate_record_bytes)
        fail(Error::Code::LimitExceeded, "physicalization routed-token bytes exceed their process limit");

    const auto [usage, inserted_usage] = state.principal_token_counts.try_emplace(principal_uuid, 0);
    auto record = state.records.end();
    try
    {
        const auto [inserted_record, inserted] = state.records.emplace(
            String(opaque_token),
            RoutedTokenRecord{
                .database_uuid = database_uuid,
                .principal_uuid = principal_uuid,
                .expires_at_microseconds = expires_at_microseconds,
            });
        record = inserted_record;
        if (!inserted)
            fail(Error::Code::EntropyFailure, "physicalization routed-token identity was reused");
        if (!state.expiration_index.emplace(expires_at_microseconds, record->first).second)
            fail(Error::Code::EntropyFailure, "physicalization routed-token expiry identity was reused");
    }
    catch (...)
    {
        if (record != state.records.end())
            state.records.erase(record);
        if (inserted_usage)
            state.principal_token_counts.erase(usage);
        throw;
    }

    ++usage->second;
    state.aggregate_record_bytes += accounted_bytes;
}

UUID PhysicalizationTokenRouter::resolveDatabase(std::string_view opaque_token, UUID authenticated_principal_uuid, UInt64 now_microseconds)
{
    if (!hasCanonicalTokenSyntax(opaque_token) || authenticated_principal_uuid == UUIDHelpers::Nil)
        fail(Error::Code::TokenRejected, "physicalization token was rejected");
    auto & state = tokenRouterState();
    std::lock_guard lock(state.mutex);
    auto found = state.records.find(opaque_token);
    if (found == state.records.end() || found->second.principal_uuid != authenticated_principal_uuid)
        fail(Error::Code::TokenRejected, "physicalization token was rejected");
    if (found->second.expires_at_microseconds <= now_microseconds)
    {
        eraseRoutedToken(state, found);
        fail(Error::Code::TokenRejected, "physicalization token was rejected");
    }
    return found->second.database_uuid;
}

void PhysicalizationTokenRouter::unregisterToken(std::string_view opaque_token, UUID database_uuid) noexcept
{
    auto & state = tokenRouterState();
    std::lock_guard lock(state.mutex);
    auto found = state.records.find(opaque_token);
    if (found != state.records.end() && found->second.database_uuid == database_uuid)
        eraseRoutedToken(state, found);
}

void PhysicalizationTokenRouter::unregisterDatabase(UUID database_uuid) noexcept
{
    auto & state = tokenRouterState();
    std::lock_guard lock(state.mutex);
    for (auto record = state.records.begin(); record != state.records.end();)
    {
        if (record->second.database_uuid == database_uuid)
        {
            auto removed = record++;
            eraseRoutedToken(state, removed);
        }
        else
            ++record;
    }
}

}
