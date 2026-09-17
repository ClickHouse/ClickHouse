#include <DataTypes/UDT/AuthorityState.h>

#include <algorithm>
#include <array>
#include <limits>

namespace DB::UDT
{
namespace
{

constexpr std::string_view authority_state_anchor_domain = "ClickHouse UDT authority inventory anchor V1";
constexpr size_t fixed_state_bytes_without_count
    = sizeof(UInt16) + sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(UInt64) + sizeof(Digest) + sizeof(Digest);

[[noreturn]] void fail(AuthorityStateError::Code code, std::string_view message)
{
    throw AuthorityStateError(code, message);
}

void appendUInt16LE(String & output, UInt16 value)
{
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
}

void appendUInt64LE(String & output, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendVarUInt(String & output, UInt64 value)
{
    while (value >= 0x80)
    {
        output.push_back(static_cast<char>(static_cast<UInt8>(value) | 0x80));
        value >>= 7;
    }
    output.push_back(static_cast<char>(value));
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 result = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++result;
    }
    return result;
}

void appendBytes(String & output, std::span<const CanonicalByte> bytes)
{
    output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void validateLimits(const AuthorityStateLimits & limits)
{
    constexpr UInt64 maximum_implementation_leaves = 1ULL << 24;
    constexpr UInt64 maximum_implementation_bytes = 1ULL << 20;
    if (limits.maximum_leaves == 0 || limits.maximum_encoded_bytes == 0)
        fail(AuthorityStateError::Code::InvalidValue, "every authority-state limit must be nonzero");
    if (limits.maximum_leaves > maximum_implementation_leaves || limits.maximum_encoded_bytes > maximum_implementation_bytes)
        fail(AuthorityStateError::Code::InvalidValue, "an authority-state limit exceeds the implementation maximum");
}

void validateStateFields(const AuthorityState & state, const AuthorityStateLimits & limits)
{
    validateLimits(limits);
    if (state.format_version != authority_state_format_version)
        fail(AuthorityStateError::Code::UnsupportedVersion, "unsupported authority-state version");
    if (state.database_uuid == UUIDHelpers::Nil)
        fail(AuthorityStateError::Code::InvalidValue, "authority-state database UUID is nil");
    if (state.database_catalog_epoch == 0)
        fail(AuthorityStateError::Code::InvalidValue, "authority-state database catalog epoch is zero");
    if (state.persistent_capability_mask != definition_authority_capability_mask
        && state.persistent_capability_mask != dependent_object_authority_capability_mask)
        fail(AuthorityStateError::Code::InvalidValue, "authority-state capability set is incomplete");
    if (state.leaf_count > limits.maximum_leaves)
        fail(AuthorityStateError::Code::LimitExceeded, "authority-state leaf count exceeds its limit");
}

String encodeStatePrefix(const AuthorityState & state, const AuthorityStateLimits & limits)
{
    validateStateFields(state, limits);
    const UInt64 encoded_size = fixed_state_bytes_without_count + varUIntSize(state.leaf_count);
    if (encoded_size > limits.maximum_encoded_bytes || sizeof(Digest) > limits.maximum_encoded_bytes - encoded_size)
        fail(AuthorityStateError::Code::LimitExceeded, "authority-state record exceeds its size limit");

    String output;
    output.reserve(encoded_size);
    appendUInt16LE(output, state.format_version);
    appendBytes(output, uuidToCanonicalBytes(state.database_uuid));
    appendUInt64LE(output, state.database_catalog_epoch);
    appendUInt64LE(output, state.persistent_capability_mask);
    appendVarUInt(output, state.leaf_count);
    appendBytes(output, state.inventory_root);
    appendBytes(output, state.schema_graph_root);
    return output;
}

class Reader final
{
public:
    explicit Reader(std::string_view bytes_)
        : bytes(bytes_)
    {
    }

    UInt8 readByte()
    {
        require(sizeof(UInt8));
        return static_cast<UInt8>(bytes[position++]);
    }

    UInt16 readUInt16LE()
    {
        require(sizeof(UInt16));
        const UInt16 result = static_cast<UInt16>(
            static_cast<UInt8>(bytes[position]) | (static_cast<UInt16>(static_cast<UInt8>(bytes[position + 1])) << 8));
        position += sizeof(UInt16);
        return result;
    }

    UInt64 readUInt64LE()
    {
        require(sizeof(UInt64));
        UInt64 result = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            result |= static_cast<UInt64>(static_cast<UInt8>(bytes[position + index])) << (8 * index);
        position += sizeof(UInt64);
        return result;
    }

    UInt64 readMinimalVarUInt(UInt64 maximum)
    {
        UInt64 result = 0;
        UInt8 shift = 0;
        size_t encoded_size = 0;
        while (true)
        {
            const UInt8 byte = readByte();
            ++encoded_size;
            if (shift == 63 && (byte & 0xfe) != 0)
                fail(AuthorityStateError::Code::InvalidValue, "authority-state VarUInt overflows UInt64");
            result |= static_cast<UInt64>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0)
                break;
            if (shift >= 63)
                fail(AuthorityStateError::Code::InvalidValue, "authority-state VarUInt overflows UInt64");
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(result))
            fail(AuthorityStateError::Code::NonCanonical, "authority-state VarUInt is not minimally encoded");
        if (result > maximum)
            fail(AuthorityStateError::Code::LimitExceeded, "authority-state count exceeds its limit");
        return result;
    }

    template <size_t size>
    std::array<CanonicalByte, size> readArray()
    {
        require(size);
        std::array<CanonicalByte, size> result{};
        std::copy_n(reinterpret_cast<const CanonicalByte *>(bytes.data() + position), size, result.begin());
        position += size;
        return result;
    }

    void requireEnd() const
    {
        if (position != bytes.size())
            fail(AuthorityStateError::Code::TrailingData, "authority-state record has trailing data");
    }

private:
    void require(size_t count) const
    {
        if (count > bytes.size() - position)
            fail(AuthorityStateError::Code::Truncated, "authority-state record is truncated");
    }

    std::string_view bytes;
    size_t position = 0;
};

}

AuthorityStateError::AuthorityStateError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

Digest computeAuthorityStateAnchor(const AuthorityState & state, const AuthorityStateLimits & limits)
{
    return hashFramedDomainSeparated(authority_state_anchor_domain, encodeStatePrefix(state, limits));
}

AuthorityState makeAuthorityState(
    UUID database_uuid,
    UInt64 database_catalog_epoch,
    UInt64 persistent_capability_mask,
    UInt64 leaf_count,
    Digest inventory_root,
    Digest schema_graph_root,
    const AuthorityStateLimits & limits)
{
    AuthorityState state{
        .format_version = authority_state_format_version,
        .database_uuid = database_uuid,
        .database_catalog_epoch = database_catalog_epoch,
        .persistent_capability_mask = persistent_capability_mask,
        .leaf_count = leaf_count,
        .inventory_root = inventory_root,
        .schema_graph_root = schema_graph_root,
    };
    state.anchor_hash = computeAuthorityStateAnchor(state, limits);
    return state;
}

AuthorityState activateDependentObjectAuthority(const AuthorityState & definition_state, const AuthorityStateLimits & limits)
{
    validateStateFields(definition_state, limits);
    if (definition_state.anchor_hash != computeAuthorityStateAnchor(definition_state, limits))
        fail(AuthorityStateError::Code::InvalidValue, "dependent-object activation received an invalid authority-state anchor");
    if (definition_state.persistent_capability_mask != definition_authority_capability_mask)
        fail(AuthorityStateError::Code::InvalidValue, "dependent-object activation requires the definition-only capability set");
    if (definition_state.database_catalog_epoch == std::numeric_limits<UInt64>::max())
        fail(AuthorityStateError::Code::LimitExceeded, "dependent-object activation would overflow the database catalog epoch");

    return makeAuthorityState(
        definition_state.database_uuid,
        definition_state.database_catalog_epoch + 1,
        dependent_object_authority_capability_mask,
        definition_state.leaf_count,
        definition_state.inventory_root,
        definition_state.schema_graph_root,
        limits);
}

String encodeAuthorityState(const AuthorityState & state, const AuthorityStateLimits & limits)
{
    String output = encodeStatePrefix(state, limits);
    if (state.anchor_hash != hashFramedDomainSeparated(authority_state_anchor_domain, output))
        fail(AuthorityStateError::Code::InvalidValue, "authority-state anchor does not match its fields");
    appendBytes(output, state.anchor_hash);
    return output;
}

AuthorityState decodeAuthorityState(std::string_view bytes, const AuthorityStateLimits & limits)
{
    validateLimits(limits);
    if (bytes.size() > limits.maximum_encoded_bytes)
        fail(AuthorityStateError::Code::LimitExceeded, "authority-state record exceeds its size limit");

    Reader reader(bytes);
    AuthorityState state;
    state.format_version = reader.readUInt16LE();
    state.database_uuid = uuidFromCanonicalBytes(reader.readArray<sizeof(CanonicalUUID)>());
    state.database_catalog_epoch = reader.readUInt64LE();
    state.persistent_capability_mask = reader.readUInt64LE();
    state.leaf_count = reader.readMinimalVarUInt(limits.maximum_leaves);
    state.inventory_root = reader.readArray<sizeof(Digest)>();
    state.schema_graph_root = reader.readArray<sizeof(Digest)>();
    state.anchor_hash = reader.readArray<sizeof(Digest)>();
    reader.requireEnd();

    validateStateFields(state, limits);
    if (state.anchor_hash != computeAuthorityStateAnchor(state, limits))
        fail(AuthorityStateError::Code::InvalidValue, "authority-state anchor does not match its fields");
    return state;
}

}
