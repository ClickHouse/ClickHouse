#include <DataTypes/UDT/CanonicalHash.h>

#include <algorithm>
#include <stdexcept>

namespace DB::UDT
{
namespace
{

Digest convertDigest(const boost::hash2::sha2_256::result_type & source)
{
    Digest result{};
    std::copy(source.begin(), source.end(), result.begin());
    return result;
}

void writeUInt64BE(UInt64 value, CanonicalByte * output) noexcept
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output[index] = static_cast<CanonicalByte>(value >> (8 * (sizeof(value) - index - 1)));
}

UInt64 readUInt64BE(const CanonicalByte * input) noexcept
{
    UInt64 result = 0;
    for (size_t index = 0; index < sizeof(result); ++index)
        result = (result << 8) | input[index];
    return result;
}

void validateDomain(std::string_view domain)
{
    if (domain.empty())
        throw std::invalid_argument("canonical hash domain is empty");
    if (domain.find('\0') != std::string_view::npos)
        throw std::invalid_argument("canonical hash domain contains its NUL terminator");
}

void updateVarUInt(CanonicalHasher & hash, UInt64 value)
{
    std::array<CanonicalByte, 10> encoded{};
    size_t size = 0;
    do
    {
        CanonicalByte byte = static_cast<CanonicalByte>(value & 0x7f);
        value >>= 7;
        if (value)
            byte = static_cast<CanonicalByte>(byte | 0x80);
        encoded[size++] = byte;
    } while (value);
    hash.update(std::span<const CanonicalByte>(encoded.data(), size));
}

}

CanonicalUUID uuidToCanonicalBytes(const UUID & uuid) noexcept
{
    CanonicalUUID result{};
    writeUInt64BE(UUIDHelpers::getHighBytes(uuid), result.data());
    writeUInt64BE(UUIDHelpers::getLowBytes(uuid), result.data() + sizeof(UInt64));
    return result;
}

UUID uuidFromCanonicalBytes(const CanonicalUUID & bytes) noexcept
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = readUInt64BE(bytes.data());
    UUIDHelpers::getLowBytes(result) = readUInt64BE(bytes.data() + sizeof(UInt64));
    return result;
}

CanonicalHasher::CanonicalHasher(std::string_view domain)
{
    validateDomain(domain);
    hash.update(domain.data(), domain.size());
    constexpr CanonicalByte terminator = 0;
    hash.update(&terminator, 1);
}

void CanonicalHasher::checkNotFinalized() const
{
    if (finalized)
        throw std::logic_error("canonical hash has already been finalized");
}

void CanonicalHasher::update(std::span<const CanonicalByte> bytes)
{
    checkNotFinalized();
    if (!bytes.empty())
        hash.update(bytes.data(), bytes.size());
}

void CanonicalHasher::update(std::string_view bytes)
{
    checkNotFinalized();
    if (!bytes.empty())
        hash.update(bytes.data(), bytes.size());
}

void CanonicalHasher::updateUUID(const UUID & uuid)
{
    const auto bytes = uuidToCanonicalBytes(uuid);
    update(bytes);
}

Digest CanonicalHasher::finalize()
{
    checkNotFinalized();
    finalized = true;
    return convertDigest(hash.result());
}

Digest sha256(std::span<const CanonicalByte> bytes)
{
    boost::hash2::sha2_256 hash;
    if (!bytes.empty())
        hash.update(bytes.data(), bytes.size());
    return convertDigest(hash.result());
}

Digest sha256(std::string_view bytes)
{
    boost::hash2::sha2_256 hash;
    if (!bytes.empty())
        hash.update(bytes.data(), bytes.size());
    return convertDigest(hash.result());
}

Digest hashDomainSeparated(std::string_view domain, std::span<const CanonicalByte> payload)
{
    CanonicalHasher hash(domain);
    hash.update(payload);
    return hash.finalize();
}

Digest hashDomainSeparated(std::string_view domain, std::string_view payload)
{
    CanonicalHasher hash(domain);
    hash.update(payload);
    return hash.finalize();
}

Digest hashFramedDomainSeparated(std::string_view domain, std::span<const CanonicalByte> payload)
{
    CanonicalHasher hash(domain);
    updateVarUInt(hash, payload.size());
    hash.update(payload);
    return hash.finalize();
}

Digest hashFramedDomainSeparated(std::string_view domain, std::string_view payload)
{
    return hashFramedDomainSeparated(
        domain, std::span<const CanonicalByte>(reinterpret_cast<const CanonicalByte *>(payload.data()), payload.size()));
}

}
