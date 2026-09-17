#pragma once

#include <Core/UUID.h>

#include <boost/hash2/sha2.hpp>

#include <array>
#include <cstdint>
#include <span>
#include <string_view>

namespace DB::UDT
{

using CanonicalByte = std::uint8_t;
using Digest = std::array<CanonicalByte, 32>;
using CanonicalUUID = std::array<CanonicalByte, 16>;

static_assert(sizeof(Digest) == 32);
static_assert(sizeof(CanonicalUUID) == 16);

/// Returns the RFC textual-order bytes of a ClickHouse UUID. This encoding is
/// independent of the host byte order and of UInt128's in-memory layout.
CanonicalUUID uuidToCanonicalBytes(const UUID & uuid) noexcept;
UUID uuidFromCanonicalBytes(const CanonicalUUID & bytes) noexcept;

/// A single-use SHA-256 builder that prefixes every message with
/// `domain || NUL`. Domains are checked so two (domain, payload) pairs cannot
/// become ambiguous through an embedded terminator.
class CanonicalHasher final
{
public:
    explicit CanonicalHasher(std::string_view domain);

    CanonicalHasher(const CanonicalHasher &) = delete;
    CanonicalHasher & operator=(const CanonicalHasher &) = delete;
    CanonicalHasher(CanonicalHasher &&) = delete;
    CanonicalHasher & operator=(CanonicalHasher &&) = delete;

    void update(std::span<const CanonicalByte> bytes);
    void update(std::string_view bytes);
    void updateUUID(const UUID & uuid);

    /// Finalizes exactly once. update()/finalize() after finalization are
    /// rejected instead of invoking Boost.Hash2's extendable-result behavior.
    Digest finalize();

private:
    void checkNotFinalized() const;

    boost::hash2::sha2_256 hash;
    bool finalized = false;
};

/// Raw SHA-256 is intentionally separate from CanonicalHasher. Use it only
/// for a preimage that already contains its canonical domain separator.
Digest sha256(std::span<const CanonicalByte> bytes);
Digest sha256(std::string_view bytes);

/// Convenience for the complete `domain || NUL || payload` construction.
Digest hashDomainSeparated(std::string_view domain, std::span<const CanonicalByte> payload);
Digest hashDomainSeparated(std::string_view domain, std::string_view payload);

/// Hashes `domain || NUL || minimal-VarUInt(payload.size()) || payload`.
/// Permanent authority records use this framed form so concatenation with a
/// later field can never reinterpret the payload boundary.
Digest hashFramedDomainSeparated(std::string_view domain, std::span<const CanonicalByte> payload);
Digest hashFramedDomainSeparated(std::string_view domain, std::string_view payload);

}
