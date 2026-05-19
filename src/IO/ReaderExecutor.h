#pragma once

#include <IO/Rope.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/ISourceReader.h>
#include <IO/SourceBufferLimit.h>

#include <Common/Logger.h>
#include <base/types.h>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <vector>

#include "config.h"
#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#endif

namespace DB
{

class PrefetchThreadPool;

class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_WINDOW_SIZE = 8 * 1024 * 1024; /// 8 MiB
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 8 * 1024 * 1024; /// 8 MiB
    static constexpr size_t ROPE_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB per Rope node

    ReaderExecutor(
        std::shared_ptr<ISourceReader> source,
        const StoredObjects & objects,
        std::vector<std::shared_ptr<ICacheProvider>> caches,
        size_t window_size = DEFAULT_WINDOW_SIZE,
        size_t min_bytes_for_seek = DEFAULT_MIN_BYTES_FOR_SEEK,
        CacheKey cache_key = {});

    /// Destructor must be out-of-line because LiveBuffer holds unique_ptr<ReadBufferFromFileBase>.
    ~ReaderExecutor();

    /// Read the next window starting at the current position.
    /// Returns an empty Rope at EOF.
    Rope readNextWindow();

    /// Seek to a new position. Discards any prefetched data.
    void seek(size_t new_position);

    void setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool);
    void setBufferLimit(std::shared_ptr<SourceBufferLimit> limit);

#if USE_SSL
    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    /// Add a decryption layer. Can be called multiple times for layered encryption.
    /// Headers are read lazily on the first readNextWindow call.
    void addDecryptionLayer(String path, size_t buffer_size, KeyFinderFunc key_finder);
#endif

    size_t getPosition() const { return position; }

    /// Logical file size (physical size minus encryption headers).
    size_t totalSize() const { return offset_map.totalSize() - data_start_offset; }

    /// Merge close-together ranges to reduce source request count.
    /// Ranges separated by less than min_gap are combined.
    static std::vector<ByteRange> mergeRanges(const std::vector<ByteRange> & ranges, size_t min_gap);

private:
    /// Read a specific physical range through the cache chain and source.
    Rope readPhysicalWindow(ByteRange physical_window);

    /// Read from source, trying live buffer first, falling back to stateless read.
    size_t readFromSource(const StoredObject & object, size_t offset, size_t size, char * buffer);

    /// Read from the live buffer into caller-provided memory.
    /// Uses external buffer to avoid copying — data goes directly from the
    /// network/disk into the target memory.
    size_t readFromLiveBuffer(char * buffer, size_t size);

    void maybeTriggerPrefetch();
    void discardPrefetch();

#if USE_SSL
    /// Read encryption headers from physical offset 0, resolve keys.
    void initDecryption();

    /// Decrypt rope data in-place. Returns a new rope with decrypted content.
    Rope decryptRope(Rope rope, size_t logical_offset);
#endif

    std::shared_ptr<ISourceReader> source;
    OffsetMap offset_map;
    std::vector<std::shared_ptr<ICacheProvider>> caches;
    CacheKey cache_key;
    size_t window_size;
    size_t min_bytes_for_seek;
    size_t position = 0;

    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    std::future<Rope> prefetch_future;
    ByteRange prefetch_range;      /// range the in-flight prefetch covers
    bool prefetch_valid = false;

    /// Live buffer: keeps a connection open for sequential reads.
    struct LiveBuffer
    {
        String object_path;
        size_t current_position = 0;
        std::unique_ptr<ReadBufferFromFileBase> buffer;
        SourceBufferSlot slot;
    };

    std::optional<LiveBuffer> live_buffer;
    std::shared_ptr<SourceBufferLimit> buffer_limit;

#if USE_SSL
    /// Decryption
    struct DecryptionLayer
    {
        String path;
        size_t buffer_size;
        KeyFinderFunc key_finder;
        /// Populated by initDecryption
        String key;
    };

    std::vector<DecryptionLayer> decryption_layers;
    std::vector<FileEncryption::Header> decryption_headers;
    bool decryption_initialized = false;
#endif
    size_t data_start_offset = 0;  /// N * Header::kSize (0 when no encryption)

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
