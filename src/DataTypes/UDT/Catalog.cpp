#include <DataTypes/UDT/Catalog.h>

#include <Core/UUID.h>
#include <Common/CacheLine.h>
#include <Common/SipHash.h>

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <atomic>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace DB::UDT
{

namespace
{

[[noreturn]] void catalogError(CatalogError::Code code, std::string_view message)
{
    throw CatalogError(code, message);
}

UInt64 toUInt64(std::size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        catalogError(CatalogError::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        catalogError(CatalogError::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        catalogError(CatalogError::Code::LimitExceeded, message);
    return lhs * rhs;
}

UInt64 checkedSubtract(UInt64 lhs, UInt64 rhs) noexcept
{
    if (rhs > lhs)
        std::terminate();
    return lhs - rhs;
}

void charge(UInt64 & total, UInt64 amount, UInt64 maximum, std::string_view message)
{
    if (amount > maximum || total > maximum - amount)
        catalogError(CatalogError::Code::LimitExceeded, message);
    total += amount;
}

UInt64 stringOwnedBytes(const String & value)
{
    return checkedAdd(toUInt64(value.capacity(), "catalog String capacity does not fit UInt64"), 1, "catalog String byte count overflow");
}

template <typename T>
UInt64 vectorOwnedBytes(const std::vector<T> & value)
{
    return checkedMultiply(
        toUInt64(value.capacity(), "catalog vector capacity does not fit UInt64"),
        static_cast<UInt64>(sizeof(T)),
        "catalog vector byte count overflow");
}

UInt64 estimateDefinitionBytes(const Definition & definition)
{
    UInt64 result = sizeof(Definition) + 2 * sizeof(void *);
    result = checkedAdd(result, stringOwnedBytes(definition.getNormalizedName()), "catalog definition byte count overflow");
    result = checkedAdd(result, stringOwnedBytes(definition.getNormalizedLocalName()), "catalog definition byte count overflow");

    const auto & parameters = definition.getParameters();
    result = checkedAdd(result, vectorOwnedBytes(parameters), "catalog definition byte count overflow");
    for (const auto & parameter : parameters)
        result = checkedAdd(result, stringOwnedBytes(parameter.normalized_name), "catalog definition byte count overflow");

    const auto & nodes = definition.getNodes();
    result = checkedAdd(result, vectorOwnedBytes(nodes), "catalog definition byte count overflow");
    for (const auto & node : nodes)
    {
        result = checkedAdd(result, stringOwnedBytes(node.atom), "catalog definition byte count overflow");
        result = checkedAdd(result, stringOwnedBytes(node.text), "catalog definition byte count overflow");
        result = checkedAdd(result, stringOwnedBytes(node.field_value.payload), "catalog definition byte count overflow");
        result = checkedAdd(result, stringOwnedBytes(node.field_value.name), "catalog definition byte count overflow");
        result = checkedAdd(result, vectorOwnedBytes(node.enum_entries), "catalog definition byte count overflow");
        for (const auto & entry : node.enum_entries)
            result = checkedAdd(result, stringOwnedBytes(entry.name), "catalog definition byte count overflow");
        result = checkedAdd(result, vectorOwnedBytes(node.children), "catalog definition byte count overflow");
        for (const auto & child : node.children)
            result = checkedAdd(result, stringOwnedBytes(child.label), "catalog definition byte count overflow");
    }

    result = checkedAdd(result, vectorOwnedBytes(definition.getDependencies()), "catalog definition byte count overflow");
    const auto & certificate = definition.getCertificate();
    result = checkedAdd(result, stringOwnedBytes(certificate.canonical_template_ir), "catalog definition byte count overflow");
    result = checkedAdd(result, stringOwnedBytes(certificate.encoded_certificate), "catalog definition byte count overflow");
    return result;
}

int compareUUID(const UUID & lhs, const UUID & rhs)
{
    const UInt64 lhs_high = UUIDHelpers::getHighBytes(lhs);
    const UInt64 rhs_high = UUIDHelpers::getHighBytes(rhs);
    if (lhs_high != rhs_high)
        return lhs_high < rhs_high ? -1 : 1;
    const UInt64 lhs_low = UUIDHelpers::getLowBytes(lhs);
    const UInt64 rhs_low = UUIDHelpers::getLowBytes(rhs);
    if (lhs_low != rhs_low)
        return lhs_low < rhs_low ? -1 : 1;
    return 0;
}

int compareIdentity(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs)
{
    if (const int result = compareUUID(lhs.database_uuid, rhs.database_uuid))
        return result;
    if (const int result = compareUUID(lhs.type_uuid, rhs.type_uuid))
        return result;
    if (lhs.revision != rhs.revision)
        return lhs.revision < rhs.revision ? -1 : 1;
    return 0;
}

struct IdentityHash
{
    UInt64 key0 = 0;
    UInt64 key1 = 0;

    std::size_t operator()(const DefinitionIdentity & identity) const noexcept
    {
        SipHash hash(key0 ^ 0x6964656e74697479ULL, key1 ^ 0x2d696e6465782d31ULL);
        hash.update(UUIDHelpers::getHighBytes(identity.database_uuid));
        hash.update(UUIDHelpers::getLowBytes(identity.database_uuid));
        hash.update(UUIDHelpers::getHighBytes(identity.type_uuid));
        hash.update(UUIDHelpers::getLowBytes(identity.type_uuid));
        hash.update(identity.revision);
        return static_cast<std::size_t>(hash.get64());
    }
};

struct NameHash
{
    using is_transparent = void;

    UInt64 key0 = 0;
    UInt64 key1 = 0;

    std::size_t operator()(std::string_view value) const noexcept
    {
        SipHash hash(key0 ^ 0x6e616d652d696e64ULL, key1 ^ 0x65782d76312d2d2dULL);
        hash.update(value);
        return static_cast<std::size_t>(hash.get64());
    }

    std::size_t operator()(const String & value) const noexcept { return operator()(std::string_view(value)); }
};

struct NameEqual
{
    using is_transparent = void;

    bool operator()(const String & lhs, const String & rhs) const noexcept { return lhs == rhs; }
    bool operator()(const String & lhs, std::string_view rhs) const noexcept { return std::string_view(lhs) == rhs; }
};

bool isPowerOfTwo(std::size_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

constexpr std::size_t maximum_supported_shards = 4'096;
constexpr std::size_t maximum_supported_hazard_slots = 65'536;
constexpr UInt64 maximum_supported_retired_roots = 1ULL << 20;
constexpr UInt64 maximum_supported_definitions = 100'000;
constexpr UInt64 maximum_supported_normalized_name_length = 4ULL << 10;
constexpr UInt64 maximum_supported_normalized_name_bytes = 64ULL << 20;
constexpr UInt64 maximum_supported_root_accounted_bytes = 512ULL << 20;
constexpr UInt64 maximum_supported_shard_entries = 4'096;
constexpr UInt64 maximum_supported_shard_accounted_bytes = 32ULL << 20;

void validateBuildLimits(const TypeCatalogBuildLimits & limits)
{
    if (!isPowerOfTwo(limits.shard_count) || limits.shard_count > maximum_supported_shards)
        catalogError(CatalogError::Code::InvalidConfiguration, "catalog shard count must be a supported power of two");
    if (limits.maximum_definitions > maximum_supported_definitions
        || limits.maximum_normalized_name_length > maximum_supported_normalized_name_length
        || limits.maximum_normalized_name_bytes > maximum_supported_normalized_name_bytes
        || limits.maximum_root_accounted_bytes > maximum_supported_root_accounted_bytes
        || limits.maximum_identity_shard_entries > maximum_supported_shard_entries
        || limits.maximum_name_shard_entries > maximum_supported_shard_entries
        || limits.maximum_shard_accounted_bytes > maximum_supported_shard_accounted_bytes)
        catalogError(CatalogError::Code::InvalidConfiguration, "catalog limits exceed the implementation domain");
}

template <typename Map>
UInt64 estimateFlatMapBytes(UInt64 entries)
{
    if (entries == 0)
        return 0;
    /// Abseil's maximum load factor is below one. Charging two slots per entry
    /// plus two control bytes per slot is deliberately conservative and stable
    /// across Abseil growth-policy revisions.
    const UInt64 slots = checkedMultiply(entries, 2, "catalog flat-map slot count overflow");
    return checkedMultiply(slots, static_cast<UInt64>(sizeof(typename Map::value_type) + 2), "catalog flat-map byte count overflow");
}

}

CatalogError::CatalogError(Code code_, std::string_view message)
    : std::runtime_error(std::string(message))
    , code(code_)
{
}

class TypeCatalogRoot::Impl final
{
public:
    using IdentityMap = absl::flat_hash_map<DefinitionIdentity, Definition::Ptr, IdentityHash>;
    using NameMap = absl::flat_hash_map<String, Definition::Ptr, NameHash, NameEqual>;

    struct IdentityShard final
    {
        IdentityShard(IdentityHash hash, TypeCatalogShardAccounting accounting_)
            : values(0, hash)
            , accounting(accounting_)
        {
        }

        IdentityMap values;
        TypeCatalogShardAccounting accounting;
    };

    struct NameShard final
    {
        NameShard(NameHash hash, TypeCatalogShardAccounting accounting_, UInt64 key_bytes_)
            : values(0, hash, NameEqual{})
            , accounting(accounting_)
            , key_bytes(key_bytes_)
        {
        }

        NameMap values;
        TypeCatalogShardAccounting accounting;
        UInt64 key_bytes;
    };

    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 generation = 0;
    UInt64 definition_count = 0;
    UInt64 normalized_name_bytes = 0;
    UInt64 retained_definition_bytes = 0;
    UInt64 accounted_bytes = 0;
    std::size_t shard_count = 0;
    UInt64 hash_key0 = 0;
    UInt64 hash_key1 = 0;
    UInt64 maximum_identity_shard_entries = 0;
    UInt64 maximum_name_shard_entries = 0;
    UInt64 maximum_shard_accounted_bytes = 0;
    UInt64 maximum_normalized_name_length = 0;
    std::vector<std::shared_ptr<const IdentityShard>> identity_shards;
    std::vector<std::shared_ptr<const NameShard>> name_shards;
};

TypeCatalogRoot::TypeCatalogRoot(std::unique_ptr<const Impl> impl_)
    : impl(std::move(impl_))
{
}

TypeCatalogRoot::~TypeCatalogRoot() = default;

const UUID & TypeCatalogRoot::getDatabaseUUID() const noexcept
{
    return impl->database_uuid;
}

UInt64 TypeCatalogRoot::getGeneration() const noexcept
{
    return impl->generation;
}

UInt64 TypeCatalogRoot::getDefinitionCount() const noexcept
{
    return impl->definition_count;
}

UInt64 TypeCatalogRoot::getAccountedBytes() const noexcept
{
    return impl->accounted_bytes;
}

std::size_t TypeCatalogRoot::getShardCount() const noexcept
{
    return impl->shard_count;
}

UInt64 TypeCatalogRoot::getShardHashKey0() const noexcept
{
    return impl->hash_key0;
}

UInt64 TypeCatalogRoot::getShardHashKey1() const noexcept
{
    return impl->hash_key1;
}

UInt64 TypeCatalogRoot::getMaximumIdentityShardEntries() const noexcept
{
    return impl->maximum_identity_shard_entries;
}

UInt64 TypeCatalogRoot::getMaximumNameShardEntries() const noexcept
{
    return impl->maximum_name_shard_entries;
}

UInt64 TypeCatalogRoot::getMaximumShardAccountedBytes() const noexcept
{
    return impl->maximum_shard_accounted_bytes;
}

UInt64 TypeCatalogRoot::getMaximumNormalizedNameLength() const noexcept
{
    return impl->maximum_normalized_name_length;
}

TypeCatalogShardAccounting TypeCatalogRoot::getIdentityShardAccounting(std::size_t shard) const
{
    if (shard >= impl->identity_shards.size())
        throw std::out_of_range("user-defined type identity shard is out of range");
    return impl->identity_shards[shard]->accounting;
}

TypeCatalogShardAccounting TypeCatalogRoot::getNameShardAccounting(std::size_t shard) const
{
    if (shard >= impl->name_shards.size())
        throw std::out_of_range("user-defined type name shard is out of range");
    return impl->name_shards[shard]->accounting;
}

bool TypeCatalogRoot::sharesIdentityShardWith(const TypeCatalogRoot & other, std::size_t shard) const
{
    if (shard >= impl->identity_shards.size() || shard >= other.impl->identity_shards.size())
        throw std::out_of_range("user-defined type identity shard is out of range");
    return impl->identity_shards[shard] == other.impl->identity_shards[shard];
}

bool TypeCatalogRoot::sharesNameShardWith(const TypeCatalogRoot & other, std::size_t shard) const
{
    if (shard >= impl->name_shards.size() || shard >= other.impl->name_shards.size())
        throw std::out_of_range("user-defined type name shard is out of range");
    return impl->name_shards[shard] == other.impl->name_shards[shard];
}

Definition::Ptr TypeCatalogRoot::findByIdentity(const DefinitionIdentity & identity) const
{
    const IdentityHash hash{impl->hash_key0, impl->hash_key1};
    const std::size_t shard = hash(identity) & (impl->shard_count - 1);
    const auto & values = impl->identity_shards[shard]->values;
    const auto it = values.find(identity);
    return it == values.end() ? Definition::Ptr{} : it->second;
}

Definition::Ptr TypeCatalogRoot::findByName(std::string_view normalized_local_name) const
{
    if (normalized_local_name.size() > impl->maximum_normalized_name_length)
        catalogError(CatalogError::Code::LimitExceeded, "catalog lookup name exceeds its length limit");
    const NameHash hash{impl->hash_key0, impl->hash_key1};
    const std::size_t shard = hash(normalized_local_name) & (impl->shard_count - 1);
    const auto & values = impl->name_shards[shard]->values;
    const auto it = values.find(normalized_local_name);
    return it == values.end() ? Definition::Ptr{} : it->second;
}

TypeCatalogRoot::Ptr TypeCatalogBuilder::build(
    UInt64 generation, std::span<const Definition::Ptr> definitions, const TypeCatalogBuildLimits & limits)
{
    if (definitions.empty())
        catalogError(
            CatalogError::Code::InvalidDefinition, "empty catalog build requires an explicit database authority UUID");
    if (!definitions.front())
        catalogError(CatalogError::Code::InvalidDefinition, "catalog definition is null");
    return build(definitions.front()->getIdentity().database_uuid, generation, definitions, limits);
}

TypeCatalogRoot::Ptr TypeCatalogBuilder::build(
    UUID database_uuid,
    UInt64 generation,
    std::span<const Definition::Ptr> definitions,
    const TypeCatalogBuildLimits & limits)
{
    validateBuildLimits(limits);
    if (database_uuid == UUIDHelpers::Nil)
        catalogError(CatalogError::Code::InvalidDefinition, "catalog database authority UUID is nil");
    if (definitions.size() > limits.maximum_definitions)
        catalogError(CatalogError::Code::LimitExceeded, "catalog definition count exceeds its limit");

    const IdentityHash identity_hash{limits.shard_hash_key0, limits.shard_hash_key1};
    const NameHash name_hash{limits.shard_hash_key0, limits.shard_hash_key1};
    std::vector<UInt64> identity_counts(limits.shard_count, 0);
    std::vector<UInt64> name_counts(limits.shard_count, 0);
    std::vector<UInt64> name_key_bytes(limits.shard_count, 0);
    std::vector<Definition::Ptr> by_identity;
    by_identity.reserve(definitions.size());

    UInt64 total_name_bytes = 0;
    UInt64 retained_definition_bytes = 0;
    for (const auto & definition : definitions)
    {
        if (!definition)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog definition is null");
        if (definition->getNormalizedName().empty() || definition->getNormalizedLocalName().empty())
            catalogError(CatalogError::Code::InvalidDefinition, "catalog definition name is empty");
        const auto & identity = definition->getIdentity();
        if (identity.database_uuid == UUIDHelpers::Nil || identity.type_uuid == UUIDHelpers::Nil || identity.revision == 0)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog definition identity is invalid");
        if (identity.database_uuid != database_uuid)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog definition belongs to another database authority");

        const UInt64 name_bytes = toUInt64(definition->getNormalizedLocalName().size(), "catalog name length does not fit UInt64");
        if (name_bytes > limits.maximum_normalized_name_length)
            catalogError(CatalogError::Code::LimitExceeded, "catalog normalized name exceeds its length limit");
        charge(total_name_bytes, name_bytes, limits.maximum_normalized_name_bytes, "catalog normalized-name bytes exceed their limit");
        const UInt64 definition_bytes = estimateDefinitionBytes(*definition);
        charge(
            retained_definition_bytes,
            definition_bytes,
            limits.maximum_root_accounted_bytes,
            "catalog retained-definition bytes exceed the root limit");

        const std::size_t identity_shard = identity_hash(identity) & (limits.shard_count - 1);
        const std::size_t name_shard = name_hash(definition->getNormalizedLocalName()) & (limits.shard_count - 1);
        ++identity_counts[identity_shard];
        ++name_counts[name_shard];
        name_key_bytes[name_shard] = checkedAdd(
            name_key_bytes[name_shard], stringOwnedBytes(definition->getNormalizedLocalName()), "catalog name-index byte count overflow");
        by_identity.push_back(definition);
    }

    std::sort(
        by_identity.begin(),
        by_identity.end(),
        [](const auto & lhs, const auto & rhs)
        {
            if (const int result = compareIdentity(lhs->getIdentity(), rhs->getIdentity()))
                return result < 0;
            return lhs->getNormalizedName() < rhs->getNormalizedName();
        });
    for (std::size_t index = 1; index < by_identity.size(); ++index)
    {
        if (compareIdentity(by_identity[index - 1]->getIdentity(), by_identity[index]->getIdentity()) == 0)
            catalogError(CatalogError::Code::DuplicateIdentity, "catalog contains a duplicate immutable identity");
    }

    std::vector<Definition::Ptr> by_name = by_identity;
    std::sort(
        by_name.begin(),
        by_name.end(),
        [](const auto & lhs, const auto & rhs)
        {
            if (lhs->getNormalizedLocalName() != rhs->getNormalizedLocalName())
                return lhs->getNormalizedLocalName() < rhs->getNormalizedLocalName();
            return compareIdentity(lhs->getIdentity(), rhs->getIdentity()) < 0;
        });
    for (std::size_t index = 1; index < by_name.size(); ++index)
    {
        if (by_name[index - 1]->getNormalizedLocalName() == by_name[index]->getNormalizedLocalName())
            catalogError(CatalogError::Code::DuplicateName, "catalog contains a duplicate normalized name");
    }

    using IdentityMap = TypeCatalogRoot::Impl::IdentityMap;
    using NameMap = TypeCatalogRoot::Impl::NameMap;
    UInt64 accounted_bytes = sizeof(TypeCatalogRoot) + sizeof(TypeCatalogRoot::Impl);
    accounted_bytes = checkedAdd(
        accounted_bytes,
        checkedMultiply(
            static_cast<UInt64>(limits.shard_count),
            2 * static_cast<UInt64>(sizeof(std::shared_ptr<const void>)),
            "catalog shard-vector byte count overflow"),
        "catalog root byte count overflow");
    accounted_bytes = checkedAdd(accounted_bytes, retained_definition_bytes, "catalog root byte count overflow");

    std::vector<TypeCatalogShardAccounting> identity_accounting(limits.shard_count);
    std::vector<TypeCatalogShardAccounting> name_accounting(limits.shard_count);
    for (std::size_t shard = 0; shard < limits.shard_count; ++shard)
    {
        if (identity_counts[shard] > limits.maximum_identity_shard_entries || name_counts[shard] > limits.maximum_name_shard_entries)
            catalogError(CatalogError::Code::LimitExceeded, "catalog shard entry count exceeds its limit");
        identity_accounting[shard] = {
            .entries = identity_counts[shard],
            .accounted_bytes = checkedAdd(
                sizeof(TypeCatalogRoot::Impl::IdentityShard) + 2 * sizeof(void *),
                estimateFlatMapBytes<IdentityMap>(identity_counts[shard]),
                "catalog identity-shard byte count overflow"),
        };
        name_accounting[shard] = {
            .entries = name_counts[shard],
            .accounted_bytes = checkedAdd(
                checkedAdd(
                    sizeof(TypeCatalogRoot::Impl::NameShard) + 2 * sizeof(void *),
                    estimateFlatMapBytes<NameMap>(name_counts[shard]),
                    "catalog name-shard byte count overflow"),
                name_key_bytes[shard],
                "catalog name-shard byte count overflow"),
        };
        if (identity_accounting[shard].accounted_bytes > limits.maximum_shard_accounted_bytes
            || name_accounting[shard].accounted_bytes > limits.maximum_shard_accounted_bytes)
            catalogError(CatalogError::Code::LimitExceeded, "catalog shard bytes exceed their limit");
        charge(
            accounted_bytes,
            identity_accounting[shard].accounted_bytes,
            limits.maximum_root_accounted_bytes,
            "catalog root bytes exceed their limit");
        charge(
            accounted_bytes,
            name_accounting[shard].accounted_bytes,
            limits.maximum_root_accounted_bytes,
            "catalog root bytes exceed their limit");
    }
    if (accounted_bytes > limits.maximum_root_accounted_bytes)
        catalogError(CatalogError::Code::LimitExceeded, "catalog root bytes exceed their limit");

    auto mutable_impl = std::make_unique<TypeCatalogRoot::Impl>();
    mutable_impl->database_uuid = database_uuid;
    mutable_impl->generation = generation;
    mutable_impl->definition_count = toUInt64(definitions.size(), "catalog definition count does not fit UInt64");
    mutable_impl->normalized_name_bytes = total_name_bytes;
    mutable_impl->retained_definition_bytes = retained_definition_bytes;
    mutable_impl->accounted_bytes = accounted_bytes;
    mutable_impl->shard_count = limits.shard_count;
    mutable_impl->hash_key0 = limits.shard_hash_key0;
    mutable_impl->hash_key1 = limits.shard_hash_key1;
    mutable_impl->maximum_identity_shard_entries = limits.maximum_identity_shard_entries;
    mutable_impl->maximum_name_shard_entries = limits.maximum_name_shard_entries;
    mutable_impl->maximum_shard_accounted_bytes = limits.maximum_shard_accounted_bytes;
    mutable_impl->maximum_normalized_name_length = limits.maximum_normalized_name_length;
    mutable_impl->identity_shards.reserve(limits.shard_count);
    mutable_impl->name_shards.reserve(limits.shard_count);

    std::vector<std::shared_ptr<TypeCatalogRoot::Impl::IdentityShard>> mutable_identity_shards;
    std::vector<std::shared_ptr<TypeCatalogRoot::Impl::NameShard>> mutable_name_shards;
    mutable_identity_shards.reserve(limits.shard_count);
    mutable_name_shards.reserve(limits.shard_count);
    for (std::size_t shard = 0; shard < limits.shard_count; ++shard)
    {
        auto identity_shard = std::make_shared<TypeCatalogRoot::Impl::IdentityShard>(identity_hash, identity_accounting[shard]);
        auto name_shard = std::make_shared<TypeCatalogRoot::Impl::NameShard>(name_hash, name_accounting[shard], name_key_bytes[shard]);
        identity_shard->values.reserve(static_cast<std::size_t>(identity_counts[shard]));
        name_shard->values.reserve(static_cast<std::size_t>(name_counts[shard]));
        mutable_identity_shards.push_back(std::move(identity_shard));
        mutable_name_shards.push_back(std::move(name_shard));
    }

    for (const auto & definition : by_identity)
    {
        const std::size_t shard = identity_hash(definition->getIdentity()) & (limits.shard_count - 1);
        const bool inserted = mutable_identity_shards[shard]->values.emplace(definition->getIdentity(), definition).second;
        if (!inserted)
            std::terminate();
    }
    for (const auto & definition : by_name)
    {
        const std::size_t shard = name_hash(definition->getNormalizedLocalName()) & (limits.shard_count - 1);
        const bool inserted = mutable_name_shards[shard]->values.emplace(definition->getNormalizedLocalName(), definition).second;
        if (!inserted)
            std::terminate();
    }
    for (std::size_t shard = 0; shard < limits.shard_count; ++shard)
    {
        mutable_impl->identity_shards.push_back(std::move(mutable_identity_shards[shard]));
        mutable_impl->name_shards.push_back(std::move(mutable_name_shards[shard]));
    }

    return TypeCatalogRoot::Ptr(new TypeCatalogRoot(std::move(mutable_impl)));
}

TypeCatalogMutation::TypeCatalogMutation(Kind kind_, DefinitionIdentity old_identity_, Definition::Ptr definition_)
    : kind(kind_)
    , old_identity(old_identity_)
    , definition(std::move(definition_))
{
}

TypeCatalogMutation TypeCatalogMutation::add(Definition::Ptr definition)
{
    return TypeCatalogMutation(Kind::Add, {}, std::move(definition));
}

TypeCatalogMutation TypeCatalogMutation::replace(DefinitionIdentity old_identity, Definition::Ptr definition)
{
    return TypeCatalogMutation(Kind::Replace, old_identity, std::move(definition));
}

TypeCatalogMutation TypeCatalogMutation::remove(DefinitionIdentity identity)
{
    return TypeCatalogMutation(Kind::Remove, identity, {});
}

TypeCatalogRoot::Ptr TypeCatalogBuilder::applyMutation(
    const TypeCatalogRoot & base, UInt64 generation, const TypeCatalogMutation & mutation, const TypeCatalogBuildLimits & limits)
{
    validateBuildLimits(limits);
    if (base.impl->shard_count != limits.shard_count || base.impl->hash_key0 != limits.shard_hash_key0
        || base.impl->hash_key1 != limits.shard_hash_key1)
        catalogError(CatalogError::Code::InvalidConfiguration, "catalog mutation changes fixed shard configuration");
    if (limits.maximum_identity_shard_entries > base.impl->maximum_identity_shard_entries
        || limits.maximum_name_shard_entries > base.impl->maximum_name_shard_entries
        || limits.maximum_shard_accounted_bytes > base.impl->maximum_shard_accounted_bytes
        || limits.maximum_normalized_name_length > base.impl->maximum_normalized_name_length)
        catalogError(CatalogError::Code::InvalidConfiguration, "catalog mutation widens fixed shard-copy limits");
    if (generation <= base.impl->generation)
        catalogError(CatalogError::Code::GenerationMismatch, "catalog mutation generation is not increasing");

    const bool removes_old = mutation.kind != TypeCatalogMutation::Kind::Add;
    const bool adds_new = mutation.kind != TypeCatalogMutation::Kind::Remove;
    Definition::Ptr old_definition;
    if (removes_old)
    {
        if (mutation.old_identity.database_uuid == UUIDHelpers::Nil || mutation.old_identity.type_uuid == UUIDHelpers::Nil
            || mutation.old_identity.revision == 0)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog mutation identity is invalid");
        if (mutation.old_identity.database_uuid != base.impl->database_uuid)
            catalogError(
                CatalogError::Code::InvalidDefinition, "catalog mutation identity belongs to another database authority");
        old_definition = base.findByIdentity(mutation.old_identity);
        if (!old_definition)
            catalogError(CatalogError::Code::MissingIdentity, "catalog mutation identity does not exist");
    }

    const auto & new_definition = mutation.definition;
    if (adds_new)
    {
        if (!new_definition)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog mutation definition is null");
        if (new_definition->getNormalizedName().empty() || new_definition->getNormalizedLocalName().empty())
            catalogError(CatalogError::Code::InvalidDefinition, "catalog mutation definition name is empty");
        if (new_definition->getNormalizedLocalName().size() > limits.maximum_normalized_name_length)
            catalogError(CatalogError::Code::LimitExceeded, "catalog mutation normalized name exceeds its length limit");
        const auto & identity = new_definition->getIdentity();
        if (identity.database_uuid == UUIDHelpers::Nil || identity.type_uuid == UUIDHelpers::Nil || identity.revision == 0)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog mutation definition identity is invalid");
        if (identity.database_uuid != base.impl->database_uuid)
            catalogError(
                CatalogError::Code::InvalidDefinition, "catalog mutation definition belongs to another database authority");

        const auto identity_collision = base.findByIdentity(identity);
        if (identity_collision && (!old_definition || identity_collision->getIdentity() != old_definition->getIdentity()))
            catalogError(CatalogError::Code::DuplicateIdentity, "catalog mutation collides with an immutable identity");
        if (old_definition && identity == old_definition->getIdentity() && !old_definition->hasSameCheckedSemantics(*new_definition))
            catalogError(
                CatalogError::Code::InvalidDefinition,
                "catalog mutation changes checked semantics under an immutable identity");
        const auto name_collision = base.findByName(new_definition->getNormalizedLocalName());
        if (name_collision && (!old_definition || name_collision->getIdentity() != old_definition->getIdentity()))
            catalogError(CatalogError::Code::DuplicateName, "catalog mutation collides with a normalized name");
    }

    UInt64 definition_count = base.impl->definition_count;
    if (removes_old)
        definition_count = checkedSubtract(definition_count, 1);
    if (adds_new)
        definition_count = checkedAdd(definition_count, 1, "catalog mutation definition count overflow");
    if (definition_count > limits.maximum_definitions)
        catalogError(CatalogError::Code::LimitExceeded, "catalog mutation definition count exceeds its limit");

    UInt64 normalized_name_bytes = base.impl->normalized_name_bytes;
    UInt64 retained_definition_bytes = base.impl->retained_definition_bytes;
    if (removes_old)
    {
        normalized_name_bytes = checkedSubtract(
            normalized_name_bytes,
            toUInt64(old_definition->getNormalizedLocalName().size(), "catalog mutation old-name length does not fit UInt64"));
        retained_definition_bytes = checkedSubtract(retained_definition_bytes, estimateDefinitionBytes(*old_definition));
    }
    if (adds_new)
    {
        normalized_name_bytes = checkedAdd(
            normalized_name_bytes,
            toUInt64(new_definition->getNormalizedLocalName().size(), "catalog mutation new-name length does not fit UInt64"),
            "catalog mutation normalized-name byte count overflow");
        retained_definition_bytes = checkedAdd(
            retained_definition_bytes, estimateDefinitionBytes(*new_definition), "catalog mutation definition byte count overflow");
    }
    if (normalized_name_bytes > limits.maximum_normalized_name_bytes)
        catalogError(CatalogError::Code::LimitExceeded, "catalog mutation normalized-name bytes exceed their limit");
    if (retained_definition_bytes > limits.maximum_root_accounted_bytes)
        catalogError(CatalogError::Code::LimitExceeded, "catalog mutation retained-definition bytes exceed the root limit");

    const IdentityHash identity_hash{limits.shard_hash_key0, limits.shard_hash_key1};
    const NameHash name_hash{limits.shard_hash_key0, limits.shard_hash_key1};
    const std::optional<std::size_t> old_identity_shard
        = removes_old ? std::optional(identity_hash(old_definition->getIdentity()) & (limits.shard_count - 1)) : std::nullopt;
    const std::optional<std::size_t> new_identity_shard
        = adds_new ? std::optional(identity_hash(new_definition->getIdentity()) & (limits.shard_count - 1)) : std::nullopt;
    const std::optional<std::size_t> old_name_shard
        = removes_old ? std::optional(name_hash(old_definition->getNormalizedLocalName()) & (limits.shard_count - 1)) : std::nullopt;
    const std::optional<std::size_t> new_name_shard
        = adds_new ? std::optional(name_hash(new_definition->getNormalizedLocalName()) & (limits.shard_count - 1)) : std::nullopt;

    using IdentityMap = TypeCatalogRoot::Impl::IdentityMap;
    using NameMap = TypeCatalogRoot::Impl::NameMap;
    UInt64 accounted_bytes = checkedSubtract(base.impl->accounted_bytes, base.impl->retained_definition_bytes);
    accounted_bytes = checkedAdd(accounted_bytes, retained_definition_bytes, "catalog mutation root byte count overflow");
    for (std::size_t shard = 0; shard < limits.shard_count; ++shard)
    {
        if (old_identity_shard == shard || new_identity_shard == shard)
        {
            const auto old_accounting = base.impl->identity_shards[shard]->accounting;
            UInt64 entries = old_accounting.entries;
            if (old_identity_shard == shard)
                entries = checkedSubtract(entries, 1);
            if (new_identity_shard == shard)
                entries = checkedAdd(entries, 1, "catalog mutation identity-shard entry count overflow");
            if (entries > limits.maximum_identity_shard_entries)
                catalogError(
                    CatalogError::Code::LimitExceeded, "catalog mutation identity-shard entries exceed their limit");
            const UInt64 replacement_bytes = checkedAdd(
                sizeof(TypeCatalogRoot::Impl::IdentityShard) + 2 * sizeof(void *),
                estimateFlatMapBytes<IdentityMap>(entries),
                "catalog mutation identity-shard byte count overflow");
            if (replacement_bytes > limits.maximum_shard_accounted_bytes)
                catalogError(CatalogError::Code::LimitExceeded, "catalog mutation identity-shard bytes exceed their limit");
            accounted_bytes = checkedSubtract(accounted_bytes, old_accounting.accounted_bytes);
            accounted_bytes = checkedAdd(accounted_bytes, replacement_bytes, "catalog mutation root byte count overflow");
        }

        if (old_name_shard == shard || new_name_shard == shard)
        {
            const auto & old_shard = *base.impl->name_shards[shard];
            UInt64 entries = old_shard.accounting.entries;
            UInt64 key_bytes = old_shard.key_bytes;
            if (old_name_shard == shard)
            {
                entries = checkedSubtract(entries, 1);
                key_bytes = checkedSubtract(key_bytes, stringOwnedBytes(old_definition->getNormalizedLocalName()));
            }
            if (new_name_shard == shard)
            {
                entries = checkedAdd(entries, 1, "catalog mutation name-shard entry count overflow");
                key_bytes = checkedAdd(
                    key_bytes,
                    stringOwnedBytes(new_definition->getNormalizedLocalName()),
                    "catalog mutation name-index byte count overflow");
            }
            if (entries > limits.maximum_name_shard_entries)
                catalogError(CatalogError::Code::LimitExceeded, "catalog mutation name-shard entries exceed their limit");
            const UInt64 replacement_bytes = checkedAdd(
                checkedAdd(
                    sizeof(TypeCatalogRoot::Impl::NameShard) + 2 * sizeof(void *),
                    estimateFlatMapBytes<NameMap>(entries),
                    "catalog mutation name-shard byte count overflow"),
                key_bytes,
                "catalog mutation name-shard byte count overflow");
            if (replacement_bytes > limits.maximum_shard_accounted_bytes)
                catalogError(CatalogError::Code::LimitExceeded, "catalog mutation name-shard bytes exceed their limit");
            accounted_bytes = checkedSubtract(accounted_bytes, old_shard.accounting.accounted_bytes);
            accounted_bytes = checkedAdd(accounted_bytes, replacement_bytes, "catalog mutation root byte count overflow");
        }
    }
    if (accounted_bytes > limits.maximum_root_accounted_bytes)
        catalogError(CatalogError::Code::LimitExceeded, "catalog mutation root bytes exceed their limit");

    /// Every validation and quota check above is allocation-free. Only now do
    /// we allocate the replacement root vectors and the affected shards.
    auto mutable_impl = std::make_unique<TypeCatalogRoot::Impl>();
    mutable_impl->database_uuid = base.impl->database_uuid;
    mutable_impl->generation = generation;
    mutable_impl->definition_count = definition_count;
    mutable_impl->normalized_name_bytes = normalized_name_bytes;
    mutable_impl->retained_definition_bytes = retained_definition_bytes;
    mutable_impl->accounted_bytes = accounted_bytes;
    mutable_impl->shard_count = base.impl->shard_count;
    mutable_impl->hash_key0 = base.impl->hash_key0;
    mutable_impl->hash_key1 = base.impl->hash_key1;
    mutable_impl->maximum_identity_shard_entries = base.impl->maximum_identity_shard_entries;
    mutable_impl->maximum_name_shard_entries = base.impl->maximum_name_shard_entries;
    mutable_impl->maximum_shard_accounted_bytes = base.impl->maximum_shard_accounted_bytes;
    mutable_impl->maximum_normalized_name_length = base.impl->maximum_normalized_name_length;
    mutable_impl->identity_shards = base.impl->identity_shards;
    mutable_impl->name_shards = base.impl->name_shards;

    for (std::size_t shard = 0; shard < limits.shard_count; ++shard)
    {
        if (old_identity_shard == shard || new_identity_shard == shard)
        {
            const auto & source = *base.impl->identity_shards[shard];
            UInt64 entries = source.accounting.entries;
            if (old_identity_shard == shard)
                entries = checkedSubtract(entries, 1);
            if (new_identity_shard == shard)
                entries = checkedAdd(entries, 1, "catalog mutation identity-shard entry count overflow");
            const TypeCatalogShardAccounting accounting{
                .entries = entries,
                .accounted_bytes = checkedAdd(
                    sizeof(TypeCatalogRoot::Impl::IdentityShard) + 2 * sizeof(void *),
                    estimateFlatMapBytes<IdentityMap>(entries),
                    "catalog mutation identity-shard byte count overflow"),
            };
            auto replacement = std::make_shared<TypeCatalogRoot::Impl::IdentityShard>(identity_hash, accounting);
            replacement->values.reserve(static_cast<std::size_t>(entries));
            for (const auto & [identity, definition] : source.values)
            {
                if (old_identity_shard == shard && identity == old_definition->getIdentity())
                    continue;
                if (!replacement->values.emplace(identity, definition).second)
                    std::terminate();
            }
            if (new_identity_shard == shard && !replacement->values.emplace(new_definition->getIdentity(), new_definition).second)
                std::terminate();
            if (replacement->values.size() != entries)
                std::terminate();
            mutable_impl->identity_shards[shard] = std::move(replacement);
        }

        if (old_name_shard == shard || new_name_shard == shard)
        {
            const auto & source = *base.impl->name_shards[shard];
            UInt64 entries = source.accounting.entries;
            UInt64 key_bytes = source.key_bytes;
            if (old_name_shard == shard)
            {
                entries = checkedSubtract(entries, 1);
                key_bytes = checkedSubtract(key_bytes, stringOwnedBytes(old_definition->getNormalizedLocalName()));
            }
            if (new_name_shard == shard)
            {
                entries = checkedAdd(entries, 1, "catalog mutation name-shard entry count overflow");
                key_bytes = checkedAdd(
                    key_bytes,
                    stringOwnedBytes(new_definition->getNormalizedLocalName()),
                    "catalog mutation name-index byte count overflow");
            }
            const TypeCatalogShardAccounting accounting{
                .entries = entries,
                .accounted_bytes = checkedAdd(
                    checkedAdd(
                        sizeof(TypeCatalogRoot::Impl::NameShard) + 2 * sizeof(void *),
                        estimateFlatMapBytes<NameMap>(entries),
                        "catalog mutation name-shard byte count overflow"),
                    key_bytes,
                    "catalog mutation name-shard byte count overflow"),
            };
            auto replacement = std::make_shared<TypeCatalogRoot::Impl::NameShard>(name_hash, accounting, key_bytes);
            replacement->values.reserve(static_cast<std::size_t>(entries));
            for (const auto & [name, definition] : source.values)
            {
                if (old_name_shard == shard && name == old_definition->getNormalizedLocalName())
                    continue;
                if (!replacement->values.emplace(name, definition).second)
                    std::terminate();
            }
            if (new_name_shard == shard && !replacement->values.emplace(new_definition->getNormalizedLocalName(), new_definition).second)
                std::terminate();
            if (replacement->values.size() != entries)
                std::terminate();
            mutable_impl->name_shards[shard] = std::move(replacement);
        }
    }

    return TypeCatalogRoot::Ptr(new TypeCatalogRoot(std::move(mutable_impl)));
}

TypeCatalogRoot::Ptr TypeCatalogBuilder::removeDefinitions(
    const TypeCatalogRoot & base,
    UInt64 generation,
    std::span<const DefinitionIdentity> sorted_identities,
    const TypeCatalogBuildLimits & limits)
{
    validateBuildLimits(limits);
    if (sorted_identities.empty())
        catalogError(CatalogError::Code::InvalidDefinition, "catalog batch removal is empty");
    if (generation <= base.impl->generation)
        catalogError(CatalogError::Code::GenerationMismatch, "catalog batch-removal generation is not increasing");
    if (base.impl->shard_count != limits.shard_count || base.impl->hash_key0 != limits.shard_hash_key0
        || base.impl->hash_key1 != limits.shard_hash_key1)
        catalogError(CatalogError::Code::InvalidConfiguration, "catalog batch removal changes fixed shard configuration");
    if (limits.maximum_identity_shard_entries > base.impl->maximum_identity_shard_entries
        || limits.maximum_name_shard_entries > base.impl->maximum_name_shard_entries
        || limits.maximum_shard_accounted_bytes > base.impl->maximum_shard_accounted_bytes
        || limits.maximum_normalized_name_length > base.impl->maximum_normalized_name_length)
        catalogError(CatalogError::Code::InvalidConfiguration, "catalog batch removal widens fixed shard-copy limits");
    if (sorted_identities.size() > base.impl->definition_count)
        catalogError(CatalogError::Code::MissingIdentity, "catalog batch removal exceeds the definition count");

    const IdentityHash identity_hash{limits.shard_hash_key0, limits.shard_hash_key1};
    const NameHash name_hash{limits.shard_hash_key0, limits.shard_hash_key1};
    std::vector<bool> touched_identity(limits.shard_count, false);
    std::vector<bool> touched_name(limits.shard_count, false);
    UInt64 removed_name_bytes = 0;
    UInt64 removed_definition_bytes = 0;
    for (size_t index = 0; index < sorted_identities.size(); ++index)
    {
        const auto & identity = sorted_identities[index];
        if (identity.database_uuid == UUIDHelpers::Nil || identity.type_uuid == UUIDHelpers::Nil || identity.revision == 0
            || identity.database_uuid != base.impl->database_uuid)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog batch-removal identity is invalid");
        if (index && compareIdentity(sorted_identities[index - 1], identity) >= 0)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog batch-removal identities are not strictly sorted");
        auto definition = base.findByIdentity(identity);
        if (!definition)
            catalogError(CatalogError::Code::MissingIdentity, "catalog batch-removal identity does not exist");
        removed_name_bytes = checkedAdd(
            removed_name_bytes,
            toUInt64(definition->getNormalizedLocalName().size(), "catalog batch-removal name length does not fit UInt64"),
            "catalog batch-removal name bytes overflow");
        removed_definition_bytes
            = checkedAdd(removed_definition_bytes, estimateDefinitionBytes(*definition), "catalog batch-removal definition bytes overflow");
        touched_identity[identity_hash(identity) & (limits.shard_count - 1)] = true;
        touched_name[name_hash(definition->getNormalizedLocalName()) & (limits.shard_count - 1)] = true;
    }

    auto is_removed = [&](const DefinitionIdentity & identity)
    {
        return std::binary_search(
            sorted_identities.begin(),
            sorted_identities.end(),
            identity,
            [](const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) { return compareIdentity(lhs, rhs) < 0; });
    };

    auto mutable_impl = std::make_unique<TypeCatalogRoot::Impl>(*base.impl);
    mutable_impl->generation = generation;
    mutable_impl->definition_count = checkedSubtract(base.impl->definition_count, sorted_identities.size());
    mutable_impl->normalized_name_bytes = checkedSubtract(base.impl->normalized_name_bytes, removed_name_bytes);
    mutable_impl->retained_definition_bytes = checkedSubtract(base.impl->retained_definition_bytes, removed_definition_bytes);
    mutable_impl->accounted_bytes = checkedSubtract(base.impl->accounted_bytes, removed_definition_bytes);

    using IdentityMap = TypeCatalogRoot::Impl::IdentityMap;
    using NameMap = TypeCatalogRoot::Impl::NameMap;
    for (std::size_t shard = 0; shard < limits.shard_count; ++shard)
    {
        if (touched_identity[shard])
        {
            const auto & source = *base.impl->identity_shards[shard];
            UInt64 removed_entries = 0;
            for (const auto & [identity, definition] : source.values)
            {
                static_cast<void>(definition);
                if (is_removed(identity))
                    ++removed_entries;
            }
            const UInt64 entries = checkedSubtract(source.accounting.entries, removed_entries);
            const TypeCatalogShardAccounting accounting{
                .entries = entries,
                .accounted_bytes = checkedAdd(
                    sizeof(TypeCatalogRoot::Impl::IdentityShard) + 2 * sizeof(void *),
                    estimateFlatMapBytes<IdentityMap>(entries),
                    "catalog batch-removal identity-shard byte count overflow"),
            };
            mutable_impl->accounted_bytes = checkedSubtract(mutable_impl->accounted_bytes, source.accounting.accounted_bytes);
            mutable_impl->accounted_bytes
                = checkedAdd(mutable_impl->accounted_bytes, accounting.accounted_bytes, "catalog batch-removal root byte count overflow");
            auto replacement = std::make_shared<TypeCatalogRoot::Impl::IdentityShard>(identity_hash, accounting);
            replacement->values.reserve(static_cast<size_t>(entries));
            for (const auto & [identity, definition] : source.values)
                if (!is_removed(identity) && !replacement->values.emplace(identity, definition).second)
                    std::terminate();
            if (replacement->values.size() != entries)
                std::terminate();
            mutable_impl->identity_shards[shard] = std::move(replacement);
        }

        if (touched_name[shard])
        {
            const auto & source = *base.impl->name_shards[shard];
            UInt64 entries = source.accounting.entries;
            UInt64 key_bytes = source.key_bytes;
            for (const auto & [name, definition] : source.values)
            {
                if (!is_removed(definition->getIdentity()))
                    continue;
                entries = checkedSubtract(entries, 1);
                key_bytes = checkedSubtract(key_bytes, stringOwnedBytes(name));
            }
            const TypeCatalogShardAccounting accounting{
                .entries = entries,
                .accounted_bytes = checkedAdd(
                    checkedAdd(
                        sizeof(TypeCatalogRoot::Impl::NameShard) + 2 * sizeof(void *),
                        estimateFlatMapBytes<NameMap>(entries),
                        "catalog batch-removal name-shard byte count overflow"),
                    key_bytes,
                    "catalog batch-removal name-shard byte count overflow"),
            };
            mutable_impl->accounted_bytes = checkedSubtract(mutable_impl->accounted_bytes, source.accounting.accounted_bytes);
            mutable_impl->accounted_bytes
                = checkedAdd(mutable_impl->accounted_bytes, accounting.accounted_bytes, "catalog batch-removal root byte count overflow");
            auto replacement = std::make_shared<TypeCatalogRoot::Impl::NameShard>(name_hash, accounting, key_bytes);
            replacement->values.reserve(static_cast<size_t>(entries));
            for (const auto & [name, definition] : source.values)
                if (!is_removed(definition->getIdentity()) && !replacement->values.emplace(name, definition).second)
                    std::terminate();
            if (replacement->values.size() != entries)
                std::terminate();
            mutable_impl->name_shards[shard] = std::move(replacement);
        }
    }
    if (mutable_impl->accounted_bytes > limits.maximum_root_accounted_bytes)
        catalogError(CatalogError::Code::LimitExceeded, "catalog batch-removal root bytes exceed their limit");
    return TypeCatalogRoot::Ptr(new TypeCatalogRoot(std::move(mutable_impl)));
}

namespace
{
class CatalogHazardDomain final
{
public:
    explicit CatalogHazardDomain(std::size_t slot_count_)
        : slot_count(slot_count_)
        , slots(std::make_unique<Slot[]>(slot_count_))
    {
        for (std::size_t index = 0; index < slot_count; ++index)
        {
            const UInt32 next = index + 1 == slot_count ? 0 : static_cast<UInt32>(index + 2);
            slots[index].next_free.store(next, std::memory_order_relaxed);
        }
        free_head.store(slot_count == 0 ? 0 : 1, std::memory_order_relaxed);
    }

    std::optional<std::size_t> tryClaim() noexcept
    {
        if (!enterClaim())
            return std::nullopt;

        UInt64 observed = free_head.load(std::memory_order_acquire);
        for (;;)
        {
            const UInt32 code = static_cast<UInt32>(observed & free_index_mask);
            if (code == 0)
            {
                leaveClaim();
                return std::nullopt;
            }
            const std::size_t index = static_cast<std::size_t>(code - 1);
            const UInt32 next = slots[index].next_free.load(std::memory_order_relaxed);
            const UInt64 replacement = advanceFreeTag(observed, next);
            if (free_head.compare_exchange_weak(observed, replacement, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                active_count.fetch_add(1, std::memory_order_relaxed);
                leaveClaim();
                return index;
            }
        }
    }

    void publish(std::size_t slot, const TypeCatalogRoot * root) noexcept { slots[slot].hazard.store(root, std::memory_order_seq_cst); }

    void clear(std::size_t slot) noexcept { slots[slot].hazard.store(nullptr, std::memory_order_seq_cst); }

    void release(std::size_t slot) noexcept
    {
        clear(slot);
        UInt64 observed = free_head.load(std::memory_order_relaxed);
        for (;;)
        {
            slots[slot].next_free.store(static_cast<UInt32>(observed & free_index_mask), std::memory_order_relaxed);
            const UInt64 replacement = advanceFreeTag(observed, static_cast<UInt32>(slot + 1));
            if (free_head.compare_exchange_weak(observed, replacement, std::memory_order_release, std::memory_order_relaxed))
                break;
        }
        /// This release is the lease's final domain access. Shutdown observes
        /// zero only after the slot has been completely returned to the list.
        active_count.fetch_sub(1, std::memory_order_release);
    }

    bool isHazarded(const TypeCatalogRoot * root) const noexcept
    {
        for (std::size_t index = 0; index < slot_count; ++index)
        {
            if (slots[index].hazard.load(std::memory_order_seq_cst) == root)
                return true;
        }
        return false;
    }

    UInt64 activeSlots() const noexcept { return active_count.load(std::memory_order_acquire); }

    void close() noexcept
    {
        claim_gate.fetch_or(closed_mask, std::memory_order_acq_rel);
        while ((claim_gate.load(std::memory_order_acquire) & claim_count_mask) != 0)
            std::this_thread::yield();
    }

    bool isAccepting() const noexcept { return (claim_gate.load(std::memory_order_acquire) & closed_mask) == 0; }

private:
    struct alignas(CH_CACHE_LINE_SIZE) Slot final
    {
        std::atomic<const TypeCatalogRoot *> hazard{nullptr};
        std::atomic<UInt32> next_free{0};
    };

    static constexpr UInt64 free_index_bits = 17;
    static constexpr UInt64 free_index_mask = (1ULL << free_index_bits) - 1;
    static constexpr UInt64 free_tag_increment = 1ULL << free_index_bits;
    static constexpr UInt64 closed_mask = 1ULL << 63;
    static constexpr UInt64 claim_count_mask = ~closed_mask;

    static UInt64 advanceFreeTag(UInt64 observed, UInt32 next) noexcept
    {
        return ((observed + free_tag_increment) & ~free_index_mask) | next;
    }

    bool enterClaim() noexcept
    {
        UInt64 observed = claim_gate.load(std::memory_order_acquire);
        for (;;)
        {
            if ((observed & closed_mask) != 0)
                return false;
            if ((observed & claim_count_mask) == claim_count_mask)
                std::terminate();
            if (claim_gate.compare_exchange_weak(observed, observed + 1, std::memory_order_acq_rel, std::memory_order_acquire))
                return true;
        }
    }

    void leaveClaim() noexcept { claim_gate.fetch_sub(1, std::memory_order_release); }

    const std::size_t slot_count;
    std::unique_ptr<Slot[]> slots;
    /// Low 17 bits are slot index+1 (zero means empty); upper bits are an ABA
    /// tag, making pop/push independent of the number of configured slots.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt64> free_head{0};
    alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt64> active_count{0};
    /// The top bit closes admission; the remaining bits count in-flight claims
    /// so shutdown cannot outrun a successful free-list pop.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<UInt64> claim_gate{0};
};

class CatalogRootLease final
{
public:
    CatalogRootLease(CatalogHazardDomain & domain_, std::size_t slot_, const TypeCatalogRoot * root_)
        : domain(&domain_)
        , slot(slot_)
        , root(root_)
    {
    }

    CatalogRootLease(const CatalogRootLease &) = delete;
    CatalogRootLease & operator=(const CatalogRootLease &) = delete;

    CatalogRootLease & operator=(CatalogRootLease &&) = delete;

    ~CatalogRootLease()
    {
        if (root)
            domain->release(slot);
    }

    CatalogHazardDomain * getDomain() const noexcept { return domain; }
    std::size_t getSlot() const noexcept { return slot; }
    const TypeCatalogRoot * getRoot() const noexcept { return root; }
    void disarm() noexcept { root = nullptr; }

private:
    /// The catalog destructor drains every claimed slot before destroying its
    /// UDT-local domain. Decrementing active_count is this lease's final domain
    /// access, so a raw pointer avoids refcount RMWs without weakening lifetime.
    CatalogHazardDomain * domain;
    std::size_t slot;
    const TypeCatalogRoot * root;
};
}

class Catalog::Impl final
{
public:
    Impl(TypeCatalogRoot::Ptr initial_root, const TypeCatalogPublicationLimits & limits_)
        : limits(limits_)
    {
        if (!initial_root)
            catalogError(CatalogError::Code::InvalidConfiguration, "catalog initial root is null");
        if (limits.hazard_slot_count == 0 || limits.hazard_slot_count > maximum_supported_hazard_slots)
            catalogError(CatalogError::Code::InvalidConfiguration, "catalog hazard-slot count is unsupported");
        if (limits.maximum_retired_root_count > maximum_supported_retired_roots
            || !std::in_range<std::size_t>(limits.maximum_retired_root_count))
            catalogError(CatalogError::Code::InvalidConfiguration, "catalog retired-root count is unsupported");
        if (limits.maximum_retired_root_bytes == 0 || initial_root->getAccountedBytes() > limits.maximum_retired_root_bytes)
            catalogError(CatalogError::Code::InvalidConfiguration, "catalog root cannot fit the retirement byte budget");

        fixed_shard_count = initial_root->getShardCount();
        fixed_database_uuid = initial_root->getDatabaseUUID();
        fixed_hash_key0 = initial_root->getShardHashKey0();
        fixed_hash_key1 = initial_root->getShardHashKey1();
        fixed_maximum_identity_shard_entries = initial_root->getMaximumIdentityShardEntries();
        fixed_maximum_name_shard_entries = initial_root->getMaximumNameShardEntries();
        fixed_maximum_shard_accounted_bytes = initial_root->getMaximumShardAccountedBytes();
        fixed_maximum_normalized_name_length = initial_root->getMaximumNormalizedNameLength();
        hazard_domain = std::make_unique<CatalogHazardDomain>(limits.hazard_slot_count);
        retired.reserve(static_cast<std::size_t>(limits.maximum_retired_root_count));
        current_generation.store(initial_root->getGeneration(), std::memory_order_relaxed);
        current.store(initial_root.get(), std::memory_order_release);
        current_owner = std::move(initial_root);
    }

    CatalogRootLease acquireRoot() const
    {
        auto * domain = hazard_domain.get();
        const auto claimed = domain->tryClaim();
        if (!claimed)
        {
            if (!domain->isAccepting())
                catalogError(CatalogError::Code::Shutdown, "user-defined type catalog is shut down");
            catalogError(CatalogError::Code::HazardSlotsExhausted, "user-defined type catalog hazard slots are exhausted");
        }

        const std::size_t slot = *claimed;
        for (;;)
        {
            const TypeCatalogRoot * observed = current.load(std::memory_order_acquire);
            if (!observed)
            {
                domain->release(slot);
                catalogError(CatalogError::Code::Shutdown, "user-defined type catalog is shut down");
            }
            domain->publish(slot, observed);
            if (observed == current.load(std::memory_order_acquire))
                return CatalogRootLease(*domain, slot, observed);
            domain->clear(slot);
        }
    }

    void publish(TypeCatalogRoot::Ptr next_root)
    {
        if (!next_root)
            catalogError(CatalogError::Code::InvalidDefinition, "catalog replacement root is null");
        std::lock_guard lock(writer_mutex);
        if (shutdown.load(std::memory_order_acquire))
            catalogError(CatalogError::Code::Shutdown, "cannot publish to a shut-down catalog");
        if (next_root->getShardCount() != fixed_shard_count || next_root->getShardHashKey0() != fixed_hash_key0
            || next_root->getShardHashKey1() != fixed_hash_key1
            || next_root->getMaximumIdentityShardEntries() != fixed_maximum_identity_shard_entries
            || next_root->getMaximumNameShardEntries() != fixed_maximum_name_shard_entries
            || next_root->getMaximumShardAccountedBytes() != fixed_maximum_shard_accounted_bytes
            || next_root->getMaximumNormalizedNameLength() != fixed_maximum_normalized_name_length)
            catalogError(CatalogError::Code::InvalidConfiguration, "catalog replacement changes fixed shard configuration");
        if (next_root->getDatabaseUUID() != fixed_database_uuid)
            catalogError(CatalogError::Code::InvalidConfiguration, "catalog replacement changes database authority");
        if (next_root->getGeneration() <= current_owner->getGeneration())
            catalogError(CatalogError::Code::GenerationMismatch, "catalog replacement generation is not increasing");
        if (next_root->getAccountedBytes() > limits.maximum_retired_root_bytes)
            catalogError(CatalogError::Code::LimitExceeded, "catalog replacement cannot fit the retirement byte budget");

        const UInt64 projected_count
            = checkedAdd(toUInt64(retired.size(), "retired-root count does not fit UInt64"), 1, "retired-root count overflow");
        const UInt64 projected_bytes = checkedAdd(retired_bytes, current_owner->getAccountedBytes(), "retired-root byte count overflow");
        if (projected_count > limits.maximum_retired_root_count || projected_bytes > limits.maximum_retired_root_bytes)
            catalogError(CatalogError::Code::LimitExceeded, "catalog retired-root backlog is full");
        if (retired.size() == retired.capacity())
            std::terminate();

        const TypeCatalogRoot * expected_old = current_owner.get();
        const TypeCatalogRoot * exchanged = current.exchange(next_root.get(), std::memory_order_acq_rel);
        if (exchanged != expected_old)
            std::terminate();
        retired.emplace_back(std::move(current_owner));
        retired_bytes = projected_bytes;
        current_owner = std::move(next_root);
        current_generation.store(current_owner->getGeneration(), std::memory_order_release);
    }

    TypeCatalogRetirementState scanRetired()
    {
        for (;;)
        {
            TypeCatalogRoot::Ptr reclaim;
            TypeCatalogRetirementState state;
            {
                std::lock_guard lock(writer_mutex);
                for (std::size_t index = 0; index < retired.size(); ++index)
                {
                    if (hazard_domain->isHazarded(retired[index].get()))
                        continue;
                    retired_bytes -= retired[index]->getAccountedBytes();
                    reclaim = std::move(retired[index]);
                    if (index + 1 != retired.size())
                        retired[index] = std::move(retired.back());
                    retired.pop_back();
                    break;
                }
                if (!reclaim && shutdown.load(std::memory_order_acquire) && current_owner
                    && !hazard_domain->isHazarded(current_owner.get()))
                    reclaim = std::move(current_owner);

                if (!reclaim)
                {
                    state.retired_root_count = toUInt64(retired.size(), "retired-root count does not fit UInt64");
                    state.retired_root_bytes = retired_bytes;
                    if (shutdown.load(std::memory_order_acquire) && current_owner)
                    {
                        ++state.retired_root_count;
                        state.retired_root_bytes
                            = checkedAdd(state.retired_root_bytes, current_owner->getAccountedBytes(), "shutdown root byte count overflow");
                    }
                    state.active_hazard_slots = hazard_domain->activeSlots();
                    state.shutdown = shutdown.load(std::memory_order_acquire);
                }
            }
            if (!reclaim)
                return state;
            /// Root and potentially O(N) flat-map/definition releases occur on
            /// this explicit writer/admin path, after dropping writer_mutex.
            reclaim.reset();
        }
    }

    TypeCatalogRetirementState retirementState() const
    {
        std::lock_guard lock(writer_mutex);
        TypeCatalogRetirementState state{
            .retired_root_count = toUInt64(retired.size(), "retired-root count does not fit UInt64"),
            .retired_root_bytes = retired_bytes,
            .active_hazard_slots = hazard_domain->activeSlots(),
            .shutdown = shutdown.load(std::memory_order_acquire),
        };
        if (state.shutdown && current_owner)
        {
            ++state.retired_root_count;
            state.retired_root_bytes
                = checkedAdd(state.retired_root_bytes, current_owner->getAccountedBytes(), "shutdown root byte count overflow");
        }
        return state;
    }

    void beginShutdown() noexcept
    {
        std::lock_guard lock(writer_mutex);
        if (shutdown.exchange(true, std::memory_order_acq_rel))
            return;
        hazard_domain->close();
        const TypeCatalogRoot * expected_old = current_owner.get();
        const TypeCatalogRoot * exchanged = current.exchange(nullptr, std::memory_order_acq_rel);
        current_generation.store(0, std::memory_order_release);
        if (exchanged != expected_old)
            std::terminate();
    }

    void shutdownAndDrain()
    {
        beginShutdown();
        for (;;)
        {
            const auto state = scanRetired();
            if (state.retired_root_count == 0 && state.active_hazard_slots == 0)
                return;
            std::this_thread::yield();
        }
    }

    TypeCatalogPublicationLimits limits;
    UUID fixed_database_uuid = UUIDHelpers::Nil;
    std::size_t fixed_shard_count = 0;
    UInt64 fixed_hash_key0 = 0;
    UInt64 fixed_hash_key1 = 0;
    UInt64 fixed_maximum_identity_shard_entries = 0;
    UInt64 fixed_maximum_name_shard_entries = 0;
    UInt64 fixed_maximum_shard_accounted_bytes = 0;
    UInt64 fixed_maximum_normalized_name_length = 0;
    std::unique_ptr<CatalogHazardDomain> hazard_domain;
    std::atomic<const TypeCatalogRoot *> current{nullptr};
    std::atomic<UInt64> current_generation{0};
    std::atomic<bool> shutdown{false};
    mutable std::mutex writer_mutex;
    TypeCatalogRoot::Ptr current_owner;
    std::vector<TypeCatalogRoot::Ptr> retired;
    UInt64 retired_bytes = 0;
};

Catalog::ResolutionSession::ResolutionSession(
    void * hazard_domain_, std::size_t hazard_slot_, const TypeCatalogRoot * root_) noexcept
    : hazard_domain(hazard_domain_)
    , hazard_slot(hazard_slot_)
    , root(root_)
{
}

Catalog::ResolutionSession::ResolutionSession(ResolutionSession && other) noexcept
    : hazard_domain(std::exchange(other.hazard_domain, nullptr))
    , hazard_slot(other.hazard_slot)
    , root(std::exchange(other.root, nullptr))
{
}

Catalog::ResolutionSession::~ResolutionSession()
{
    if (root)
        static_cast<CatalogHazardDomain *>(hazard_domain)->release(hazard_slot);
}

Definition::Ptr Catalog::ResolutionSession::findByIdentity(const DefinitionIdentity & identity) const
{
    return root->findByIdentity(identity);
}

Definition::Ptr Catalog::ResolutionSession::findByName(std::string_view normalized_local_name) const
{
    return root->findByName(normalized_local_name);
}

UInt64 Catalog::ResolutionSession::getGeneration() const noexcept
{
    return root->getGeneration();
}

Catalog::Catalog(TypeCatalogRoot::Ptr initial_root, const TypeCatalogPublicationLimits & limits)
    : impl(std::make_unique<Impl>(std::move(initial_root), limits))
{
}

Catalog::~Catalog()
{
    impl->shutdownAndDrain();
}

Definition::Ptr Catalog::findByIdentity(const DefinitionIdentity & identity) const
{
    const auto session = beginResolutionSession();
    return session.findByIdentity(identity);
}

Definition::Ptr Catalog::findByName(std::string_view normalized_local_name) const
{
    const auto session = beginResolutionSession();
    return session.findByName(normalized_local_name);
}

Catalog::ResolutionSession Catalog::beginResolutionSession() const
{
    auto lease = impl->acquireRoot();
    ResolutionSession result(lease.getDomain(), lease.getSlot(), lease.getRoot());
    lease.disarm();
    return result;
}

void Catalog::publish(TypeCatalogRoot::Ptr next_root)
{
    impl->publish(std::move(next_root));
}

TypeCatalogRetirementState Catalog::scanRetired()
{
    return impl->scanRetired();
}

TypeCatalogRetirementState Catalog::getRetirementState() const
{
    return impl->retirementState();
}

void Catalog::shutdownAndDrain()
{
    impl->shutdownAndDrain();
}

bool Catalog::isShutdown() const noexcept
{
    return impl->shutdown.load(std::memory_order_acquire);
}

UInt64 Catalog::currentGeneration() const noexcept
{
    return impl->current_generation.load(std::memory_order_acquire);
}

}
