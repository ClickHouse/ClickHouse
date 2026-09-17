#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>
#include <base/types.h>

#include <optional>

namespace DB
{

/// Process-global cache of the raw encryption-header bytes at the front of an encrypted file,
/// keyed by its storage path, so the experimental `ReaderExecutor` can skip re-reading the header
/// on repeated opens of the same file. Only disk reads populate it (see
/// `ReadPipeline::needEncryptionHeaderCache`); url / external reads pass a null cache.
///
/// Stores opaque bytes rather than parsed `FileEncryption::Header`s, so it has no SSL dependency;
/// the consumer parses them on a hit.
///
/// Populated only for backends with random object keys (metadata-based object storage; see
/// `DiskEncrypted::prepareRead`). There every write produces a new `remote_path`, so a
/// rewrite / replace / rename never rebinds an existing key to different ciphertext -- the cache is
/// self-invalidating (a changed file has a new key -> a miss) and needs no explicit invalidation.
/// Deterministic-path backends (plain / plain-rewritable, local, web), where the key is reused on
/// rewrite, are excluded.
class EncryptionHeaderCache
{
public:
    /// The raw serialized header bytes (`N * FileEncryption::Header::kSize`).
    using HeaderBytes = String;

private:
    /// A hash of the storage path.
    using Key = UInt128;

    struct Entry
    {
        HeaderBytes bytes;
        explicit Entry(HeaderBytes bytes_) : bytes(std::move(bytes_)) {}
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const
        {
            /// Charge the payload plus the fixed per-entry overhead (the path-hash key, the map
            /// node, the shared_ptr control block), so the byte budget bounds real memory rather
            /// than just the header payload.
            static constexpr size_t APPROXIMATE_ENTRY_OVERHEAD = 256;
            return sizeof(Key) + sizeof(Entry) + entry.bytes.capacity() + APPROXIMATE_ENTRY_OVERHEAD;
        }
    };

public:
    using Cache = CacheBase<Key, Entry, UInt128TrivialHash, EntryWeight>;

    static Key makeKey(const String & storage_path);

    EncryptionHeaderCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Cache the header bytes for `storage_path` (no-op if already present).
    void write(const String & storage_path, HeaderBytes bytes);

    /// Return the cached header bytes for `storage_path`, or nullopt on a miss.
    std::optional<HeaderBytes> read(const String & storage_path);

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    size_t maxSizeInBytes() const;

private:
    Cache cache;
    LoggerPtr logger = getLogger("EncryptionHeaderCache");
};

using EncryptionHeaderCachePtr = std::shared_ptr<EncryptionHeaderCache>;

}
