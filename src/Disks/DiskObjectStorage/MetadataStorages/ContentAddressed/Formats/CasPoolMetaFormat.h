#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <string_view>
#include <vector>

namespace DB::Cas
{

class Backend;
class Layout;

/// `_pool_meta` — the pool identity and the pool-wide constants that every reader and writer must
/// agree on. The v3 text representation is a header line followed by one JSON body object:
/// {"pid":"<32hex>","hln":<blob_header_len>,"mrg":<min_reader_generation>,"alg":"<algo-words>"}.
///
/// The persisted object is authoritative after creation. On reopen, `createOrValidate` uses its
/// `blob_header_len` and reader-generation floor rather than replacing them with local configuration;
/// the configuration's hash algorithm may only be admitted through the explicit opt-in path. The
/// `pool_id` is also the envelope `domain_id`, so it remains stable for the entire pool lifetime.
struct PoolMeta
{
    UInt128 pool_id{};
    uint64_t blob_header_len = 0;
    uint64_t gc_shards = 1;
    uint64_t min_reader_generation = 0;
    /// Every hash algorithm ever admitted, encoded as `static_cast<uint8_t>(BlobHashAlgo)`, in strictly
    /// increasing order. Admission only appends a new algorithm to this durable set.
    std::vector<uint8_t> algos_used;

    /// Creates the pool metadata if `_pool_meta` is absent, or validates and possibly admits the
    /// configured hash algorithm if it already exists. Initial creation validates the supplied header
    /// size and records this build's reader-generation floor. Reopen ignores the supplied header size,
    /// because changing it would move the blob payload offset for existing objects; a new hash algorithm
    /// is rejected unless `allow_new` is set, and concurrent admission is retried from fresh metadata.
    ///
    /// `allow_mint` (spec §2 [C4][D2]) gates the create-if-absent path: minting a fresh `_pool_meta` is a
    /// consequential write that establishes a brand-new pool identity, so it is permitted ONLY on the
    /// writable startup path that has just passed the zero-write residual proof (`Pool::open`). Every
    /// non-bootstrap caller — a read-only/observe open, `openForDecommission` — passes `false`; an absent
    /// `_pool_meta` then fails closed with `INVALID_STATE` instead of silently minting (which, on an
    /// observe scan over a partially-erased pool, would poison the next writable mount). The validate path
    /// (meta already present) never consults it.
    ///
    /// Defaults to `false` — a safety gate must fail CLOSED when a caller leaves it unstated, so a future
    /// pool-lifecycle entry point cannot silently re-arm the observe-mint footgun by omission. The two
    /// production callers pass it explicitly; only test minting sites opt in with `allow_mint=true`.
    static PoolMeta createOrValidate(
        Backend &, const Layout &, uint64_t blob_header_len, uint64_t gc_shards,
        BlobHashAlgo blob_hash_algo = BlobHashAlgo::CityHash128, bool allow_new = false,
        bool allow_mint = false);

    /// Convenience for single-shard callers. Production pool opening passes the configured value to
    /// the explicit overload above; this preserves compact single-shard codec/unit fixtures.
    static PoolMeta createOrValidate(
        Backend & backend, const Layout & layout, uint64_t blob_header_len,
        BlobHashAlgo blob_hash_algo = BlobHashAlgo::CityHash128, bool allow_new = false,
        bool allow_mint = false)
    {
        return createOrValidate(
            backend, layout, blob_header_len, /*gc_shards=*/1, blob_hash_algo, allow_new, allow_mint);
    }
};

/// Serializes valid pool metadata as the versioned `_pool_meta` text object. The output includes the
/// format header, one JSON body line, and its terminating newline; it is suitable for a conditional
/// backend write and preserves the sorted algorithm set as comma-separated vocabulary words.
String encodePoolMeta(const PoolMeta &);

/// Parses and validates a persisted `_pool_meta` object. Unknown JSON keys are tolerated for additive
/// evolution, while missing required data, malformed values, invariant violations, an unsupported
/// ref-state generation, or a pool requiring a newer reader produce an exception with the appropriate
/// corruption or compatibility error code.
PoolMeta decodePoolMeta(std::string_view);

/// Checks the fixed blob-envelope size invariant. The length must be 8-byte aligned, at most 16 KiB,
/// and at least 240 bytes: v3's mandatory envelope fields, framing, and newline consume 225 bytes at
/// type maxima, while 240 leaves room for a diagnostic `ref`. The caller supplies the error code so
/// persisted violations can be reported as `CORRUPTED_DATA` and bad creation arguments as
/// `BAD_ARGUMENTS`.
void validatePoolBlobHeaderLen(uint64_t blob_header_len, int error_code, std::string_view what);

/// Checks that every admitted hash algorithm is known, that the set is non-empty, and that its numeric
/// representation is strictly increasing with no duplicates. The caller supplies the error code to
/// distinguish invalid persisted metadata from invalid creation or admission input.
void validatePoolAlgosUsed(const std::vector<uint8_t> & algos_used, int error_code, std::string_view what);

}
