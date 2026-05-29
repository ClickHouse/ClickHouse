#include <IO/ReaderExecutor.h>
#include <IO/BufferSourceReader.h>
#include <IO/ISourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/SourceBufferLimit.h>
#include <IO/ReadSettings.h>
#include <IO/Rope.h>
#include <IO/PageCacheProvider.h>
#include <Common/PageCache.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <IO/DiskCacheProvider.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Core/ServerUUID.h>
#include <Common/QueryScope.h>
#include <Common/scope_guard_safe.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
}

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
    extern const FileCacheSettingsFileCachePolicy cache_policy;
}

using namespace DB;

namespace
{

/// In-memory source reader for testing.
/// open() materializes the requested object into a temp file and returns a
/// file-backed ReadBufferFromFileBase. Temp files are cleaned up on destruction.
class MemorySourceReader : public ISourceReader
{
public:
    explicit MemorySourceReader(std::unordered_map<String, String> data_)
        : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return nullptr;
        auto path = std::filesystem::temp_directory_path() / ("test_memory_source_" + std::to_string(file_counter++));
        {
            std::ofstream f(path, std::ios::binary);
            f.write(it->second.data(), it->second.size());
        }
        temp_files.push_back(path);
        return createReadBufferFromFileBase(path.string(), ReadSettings{});
    }

    String name() const override { return "MemorySourceReader"; }

    ~MemorySourceReader() override
    {
        for (const auto & p : temp_files)
            std::filesystem::remove(p);
    }

private:
    std::unordered_map<String, String> data;
    size_t file_counter = 0;
    std::vector<std::filesystem::path> temp_files;
};

}

TEST(ReaderExecutor, ReadSingleObjectNoCaches)
{
    String content(1000, 'A');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj_a", content}});

    StoredObjects objects;
    objects.emplace_back("obj_a", "", 1000);

    ReaderExecutor executor(source, objects, {}, 512);

    auto rope = executor.readNextWindow();
    EXPECT_FALSE(rope.empty());
    EXPECT_EQ(rope.range().offset, 0);
    EXPECT_EQ(rope.range().size, 512);

    size_t total = 0;
    for (const auto & node : rope.getNodes())
    {
        for (size_t i = 0; i < node.size; ++i)
            EXPECT_EQ(node.data()[i], 'A');
        total += node.size;
    }
    EXPECT_EQ(total, 512);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 512);
    EXPECT_EQ(rope2.range().size, 488);

    auto rope3 = executor.readNextWindow();
    EXPECT_TRUE(rope3.empty());
}

TEST(ReaderExecutor, ReadMultiObject)
{
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"blob_0", String(300, 'X')},
            {"blob_1", String(200, 'Y')},
        });

    StoredObjects objects;
    objects.emplace_back("blob_0", "", 300);
    objects.emplace_back("blob_1", "", 200);

    ReaderExecutor executor(source, objects, {}, 400);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 0);
    EXPECT_EQ(rope.range().size, 400);

    size_t pos = 0;
    for (const auto & node : rope.getNodes())
    {
        for (size_t i = 0; i < node.size; ++i)
        {
            char expected = (pos + i < 300) ? 'X' : 'Y';
            EXPECT_EQ(node.data()[i], expected)
                << "at logical offset " << (pos + i);
        }
        pos += node.size;
    }
}

TEST(ReaderExecutor, Seek)
{
    String content(1000, 'B');
    content[500] = 'Z';
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 1000);

    ReaderExecutor executor(source, objects, {}, 100);

    executor.seek(500);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 500);
    EXPECT_EQ(rope.range().size, 100);
    EXPECT_EQ(rope.getNodes()[0].data()[0], 'Z');
}

namespace
{

/// Mock cache that stores data in memory, organized by blocks.
class MockCacheHandle : public ICacheHandle
{
public:
    MockCacheHandle(
        ByteRange range,
        std::unordered_map<size_t, String> & storage_,
        size_t block_size_)
        : storage(storage_), block_size(block_size_)
    {
        size_t start_block = range.offset / block_size;
        size_t end_block = (range.end() + block_size - 1) / block_size;

        for (size_t b = start_block; b < end_block; ++b)
        {
            size_t block_start = b * block_size;
            size_t block_end = std::min(block_start + block_size, range.end());
            block_start = std::max(block_start, range.offset);
            ByteRange block_range{block_start, block_end - block_start};

            if (storage.contains(b))
                result.hit_ranges.push_back(block_range);
            else
                result.miss_ranges.push_back(block_range);
        }
    }

    CacheLookupResult status() const override { return result; }

    Rope get(ByteRange range) override
    {
        size_t block = range.offset / block_size;
        const auto & data = storage.at(block);
        auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
        std::memcpy(buf->data(), data.data(), data.size());

        Rope rope;
        rope.append(RopeNode{buf, 0, data.size(), block * block_size});
        return rope.slice(range);
    }

    size_t put(ByteRange range, Rope data) override
    {
        size_t block = range.offset / block_size;
        if (storage.contains(block))
            return 0;

        String content;
        for (const auto & node : data.getNodes())
            content.append(node.data(), node.size);
        size_t bytes = content.size();
        storage[block] = std::move(content);
        return bytes;
    }

private:
    std::unordered_map<size_t, String> & storage;
    size_t block_size;
    CacheLookupResult result;
};

class MockCacheProvider : public ICacheProvider
{
public:
    explicit MockCacheProvider(size_t block_size_)
        : block_size(block_size_) {}

    std::unique_ptr<ICacheHandle> lookup(const StoredObject &, size_t, ByteRange range) override
    {
        return std::make_unique<MockCacheHandle>(range, storage, block_size);
    }

    String name() const override { return "MockCache"; }

    bool hasBlock(size_t block_index) const { return storage.contains(block_index) > 0; }

private:
    std::unordered_map<size_t, String> storage;
    size_t block_size;
};

}

TEST(ReaderExecutor, CacheMissPopulatesCache)
{
    String content(1024, 'C');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 1024);

    auto cache = std::make_shared<MockCacheProvider>(512);

    ReaderExecutor executor(source, objects, {cache}, 512);

    /// First read: miss, fetches from source, populates cache
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 512);
    EXPECT_TRUE(cache->hasBlock(0));

    /// Second read
    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().size, 512);
    EXPECT_TRUE(cache->hasBlock(1));
}

TEST(ReaderExecutor, CacheHitSkipsSource)
{
    String source_content(512, 'S');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", source_content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 512);

    auto cache = std::make_shared<MockCacheProvider>(512);

    /// Warm up cache
    {
        ReaderExecutor warmup(source, objects, {cache}, 512);
        warmup.readNextWindow();
    }

    /// Replace source with different content
    String alt_content(512, 'Z');
    auto alt_source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", alt_content}});

    ReaderExecutor executor(alt_source, objects, {cache}, 512);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 512);

    /// Should have gotten 'S' from cache, not 'Z' from alt_source
    EXPECT_EQ(rope.getNodes()[0].data()[0], 'S');
}

TEST(ReaderExecutor, PrefetchTriggersOnReadNextWindow)
{
    String content(3000, 'P');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 3000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    ReaderExecutor executor(source, objects, {}, 1000);
    executor.setPrefetchPool(pool);

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().size, 1000);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 1000);
    EXPECT_EQ(rope2.range().size, 1000);

    auto rope3 = executor.readNextWindow();
    EXPECT_EQ(rope3.range().offset, 2000);
    EXPECT_EQ(rope3.range().size, 1000);

    auto rope4 = executor.readNextWindow();
    EXPECT_TRUE(rope4.empty());
}

TEST(ReaderExecutor, SeekInsidePrefetchedWindow)
{
    /// After the first window read, a prefetch is in flight for [500, 1000).
    /// Seeking to 750 (inside the prefetched range) must:
    ///   - leave executor.getPosition() == 750 (not 500), and
    ///   - cause the next readNextWindow to return a rope starting at logical 750
    ///     with content matching the source at offset 750.
    ///
    /// The returned size depends on which branch the executor takes:
    ///   - Wait branch (worker already running): rope is the prefetched [500, 1000)
    ///     sliced to [750, 1000), size 250.
    ///   - Cancel branch (worker hadn't started): a fresh window from position 750
    ///     of size min(window_size, file_size - 750), so the rope spans [750, 1250).
    /// Both are valid outcomes and the test accepts either.

    String content(2000, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
    executor.setPrefetchPool(pool);

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().offset, 0u);
    EXPECT_EQ(rope1.range().size, 500u);

    executor.seek(750);
    EXPECT_EQ(executor.getPosition(), 750u);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 750u);
    EXPECT_TRUE(rope2.range().size == 250u || rope2.range().size == 500u)
        << "got size " << rope2.range().size;
    ASSERT_FALSE(rope2.getNodes().empty());
    EXPECT_EQ(rope2.getNodes().front().data()[0], content[750]);
}

TEST(ReaderExecutor, SeekDiscardsPrefetch)
{
    String content(2000, 'Q');
    content[1500] = 'Z';
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    ReaderExecutor executor(source, objects, {}, 500);
    executor.setPrefetchPool(pool);

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().offset, 0);

    executor.seek(1500);
    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 1500);
    EXPECT_EQ(rope2.getNodes()[0].data()[0], 'Z');
}

TEST(ReaderExecutor, SeekTriggersPrefetch)
{
    /// After `seek` lands outside the previously-prefetched range, the old
    /// prefetch is discarded AND a new one for the new position must be
    /// queued — without that, the next `readNextWindow` would pay full
    /// source-read latency synchronously.
    String content(4000, 'S');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 4000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
    executor.setPrefetchPool(pool);

    /// Before the first readNextWindow nothing has been prefetched yet.
    EXPECT_FALSE(executor.hasInflightPrefetch());

    /// Seek to a position outside any existing prefetch range. Must queue
    /// a fresh prefetch for the new position right away.
    executor.seek(2500);
    EXPECT_TRUE(executor.hasInflightPrefetch())
        << "seek must trigger a new prefetch when prefetch_pool is set";

    /// And the prefetched data is the one we actually consume next.
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 2500u);
    EXPECT_EQ(rope.range().size, 500u);
}

TEST(ReaderExecutor, SeekWithoutPoolDoesNotCrash)
{
    /// Transient `readBigAt` executors have no `prefetch_pool` — the
    /// post-seek `maybeTriggerPrefetch` call must be a clean no-op there.
    String content(1000, 'T');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 1000);

    ReaderExecutor executor(source, objects, {}, 200);
    /// No setPrefetchPool — leave prefetch_pool null.

    executor.seek(400);
    EXPECT_FALSE(executor.hasInflightPrefetch());

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 400u);
}

TEST(ReaderExecutor, MergeRangesNoGap)
{
    /// Adjacent ranges — should merge into one
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {100, 100}, {200, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 300);
}

TEST(ReaderExecutor, MergeRangesSmallGap)
{
    /// Small gap (10 bytes) < min_gap (100) — merge
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {110, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 210);
}

TEST(ReaderExecutor, MergeRangesLargeGap)
{
    /// Large gap (500 bytes) > min_gap (100) — don't merge
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {600, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 100);
    EXPECT_EQ(merged[1].offset, 600);
    EXPECT_EQ(merged[1].size, 100);
}

TEST(ReaderExecutor, MergeRangesMixed)
{
    /// Three ranges: first two close, third far away
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {120, 100}, {1000, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 220);
    EXPECT_EQ(merged[1].offset, 1000);
    EXPECT_EQ(merged[1].size, 100);
}

TEST(ReaderExecutor, MergeRangesZeroMinGap)
{
    /// min_gap=0 — no merging
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {100, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 0);
    ASSERT_EQ(merged.size(), 2);
}

#if USE_SSL
TEST(ReaderExecutor, TotalSizeSaturatesOnUndersizedEncryptedFile)
{
    /// File is 10 bytes; two encryption layers expect 128 bytes of headers
    /// (offset_map.totalSize() < data_start_offset). Pre-fix: unsigned
    /// subtraction underflowed to ~SIZE_MAX, making the executor think the
    /// logical file was enormous. Post-fix: totalSize() saturates to 0;
    /// the next read (or initDecryption) will throw CANNOT_READ_ALL_DATA.
    String content(10, 'A');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/512);
    executor.addDecryptionLayer("layer0", 64, [](UInt128, const String &) { return String{}; });
    executor.addDecryptionLayer("layer1", 64, [](UInt128, const String &) { return String{}; });

    EXPECT_EQ(executor.totalSize(), 0u);
}

#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromString.h>

namespace
{
    /// Encrypt `plaintext` with the given key/iv at stream offset 0 using
    /// AES_128_CTR. CTR is symmetric — encryption and decryption are the
    /// same operation.
    String aesCtrEncrypt(const String & key, FileEncryption::InitVector iv, const String & plaintext)
    {
        FileEncryption::Encryptor enc(FileEncryption::Algorithm::AES_128_CTR, key, iv);
        enc.setOffset(0);
        String out(plaintext.size(), '\0');
        enc.decrypt(plaintext.data(), plaintext.size(), out.data());
        return out;
    }

    /// Same as `aesCtrEncrypt` but keyed at an arbitrary stream offset —
    /// needed when reproducing a legacy stacked-encryption write where an
    /// outer layer's keystream covers `[inner_header (64), inner_ciphertext]`
    /// at offsets `[0, inner_ciphertext_size + 64)`. CTR is position-
    /// addressable, so encrypting two contiguous chunks at adjacent offsets
    /// produces the same ciphertext as encrypting the concatenation.
    String aesCtrEncryptAt(const String & key, FileEncryption::InitVector iv,
        size_t stream_offset, const char * data, size_t size)
    {
        FileEncryption::Encryptor enc(FileEncryption::Algorithm::AES_128_CTR, key, iv);
        enc.setOffset(stream_offset);
        String out(size, '\0');
        enc.decrypt(data, size, out.data());
        return out;
    }

    /// Build the on-disk encrypted byte stream:
    ///   Header(64 bytes) + ciphertext
    String makeEncryptedFile(const String & key, FileEncryption::InitVector iv, const String & plaintext)
    {
        String file_bytes;
        {
            WriteBufferFromString wb(file_bytes);
            FileEncryption::Header header;
            header.algorithm = FileEncryption::Algorithm::AES_128_CTR;
            header.key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
            header.init_vector = iv;
            header.write(wb);
            wb.finalize();
        }
        file_bytes += aesCtrEncrypt(key, iv, plaintext);
        return file_bytes;
    }
}

TEST(ReaderExecutor, DecryptRopeStreamsBlockByBlock)
{
    /// End-to-end exercise of the block-by-block iteration in `decryptRope`.
    /// Plaintext is intentionally larger than ROPE_BLOCK_SIZE so the
    /// function must allocate and decrypt multiple output blocks. A
    /// non-multiple total ensures the final partial block is handled.

    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0x0123456789abcdefULL});

    const size_t plaintext_size = ReaderExecutor::ROPE_BLOCK_SIZE * 3 + 12345;
    String plaintext(plaintext_size, '\0');
    for (size_t i = 0; i < plaintext_size; ++i)
        plaintext[i] = static_cast<char>((i * 31 + 7) & 0xFF);  /// distinguishable

    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    /// Window larger than the plaintext so the entire file is read in one
    /// readNextWindow call — decryptRope sees a rope of >3 MiB and must
    /// produce 4 output blocks (3 full + 1 partial).
    ReaderExecutor executor(source, objects, {},
        /*window_size=*/plaintext_size + ReaderExecutor::ROPE_BLOCK_SIZE);
    executor.addDecryptionLayer(
        "/test", 0,
        [&](UInt128 got_fp, const String &)
        {
            EXPECT_EQ(got_fp, FileEncryption::calculateKeyFingerprint(key));
            return key;
        });
    executor.initDecryption();

    String result;
    while (true)
    {
        auto w = executor.readNextWindow();
        if (w.empty())
            break;
        for (const auto & n : w.getNodes())
            result.append(n.data(), n.size);
    }

    ASSERT_EQ(result.size(), plaintext.size());
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutor, EncryptedEofReleasesBufferLimitSlot)
{
    /// Regression: `atEnd` used to compare the logical `position` against
    /// the physical `offset_map.totalSize()`. For an encrypted file the
    /// physical size is larger by `data_start_offset` bytes, so after the
    /// last plaintext byte `position` is strictly less than
    /// `offset_map.totalSize()` and `atEnd` stays false. That skipped the
    /// EOF branch in `readNextWindow` and left the `SourceBufferLimit`
    /// slot pinned past EOF.
    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0xfeedfacecafeULL});
    String plaintext(2048, 'E');
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    auto buffer_limit = std::make_shared<SourceBufferLimit>(4);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/512);
    executor.setBufferLimit(buffer_limit);
    executor.addDecryptionLayer(
        "/test", 0,
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    while (true)
    {
        auto w = executor.readNextWindow();
        if (w.empty())
            break;
    }

    EXPECT_EQ(buffer_limit->getActive().size(), 0u);
}

TEST(ReaderExecutor, DecryptRopeRoundtripsSmallPayload)
{
    /// Same path but payload smaller than ROPE_BLOCK_SIZE — exercises the
    /// single-iteration loop.

    String key(16, 'q');
    FileEncryption::InitVector iv(UInt128{42});
    const String plaintext = "Hello, encrypted world!";
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    ReaderExecutor executor(source, objects, {}, /*window_size=*/4096);
    executor.addDecryptionLayer("/t", 0,
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    auto w = executor.readNextWindow();
    String result;
    for (const auto & n : w.getNodes())
        result.append(n.data(), n.size);
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutor, DecryptRopeMultiLayer)
{
    /// Two encryption layers stacked, in the layout that a legacy
    /// `DiskEncrypted`-over-`DiskEncrypted` configuration actually
    /// produces on write:
    ///   [outer_h_plain]            -- 64 bytes, in clear
    ///   [outer.encrypt(inner_h)]   -- 64 bytes, ciphertext (NOT plaintext)
    ///   [outer.encrypt(inner.encrypt(plaintext))]
    /// The outer encryption keystream covers the inner header AND payload
    /// — i.e. outer's keystream offset for user-byte P is `P + 64`, while
    /// inner's is `P`. `initDecryption` must peel the outer layer off the
    /// inner header bytes before parsing them; `decryptRope` must apply
    /// per-layer keystream offsets to recover the plaintext.

    String key_inner(16, 'i');
    String key_outer(16, 'o');
    FileEncryption::InitVector iv_inner(UInt128{1});
    FileEncryption::InitVector iv_outer(UInt128{2});

    const String plaintext(ReaderExecutor::ROPE_BLOCK_SIZE + 500, 'X');

    /// 1. Serialize the inner header bytes.
    String inner_h_bytes;
    {
        WriteBufferFromString wb(inner_h_bytes);
        FileEncryption::Header inner_h;
        inner_h.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        inner_h.key_fingerprint = FileEncryption::calculateKeyFingerprint(key_inner);
        inner_h.init_vector = iv_inner;
        inner_h.write(wb);
        wb.finalize();
    }
    ASSERT_EQ(inner_h_bytes.size(), FileEncryption::Header::kSize);

    /// 2. Inner-encrypt the plaintext at inner keystream offset 0.
    const String inner_ciphertext = aesCtrEncrypt(key_inner, iv_inner, plaintext);

    /// 3. Outer-encrypt `inner_h_bytes` and `inner_ciphertext` as one
    ///    contiguous stream — `inner_h_bytes` at outer-keystream offset 0,
    ///    `inner_ciphertext` at outer-keystream offset 64. CTR is
    ///    position-addressable so this matches the result of outer-
    ///    encrypting the concatenation in one shot.
    const String outer_h_ciphertext = aesCtrEncryptAt(
        key_outer, iv_outer,
        /*stream_offset=*/0,
        inner_h_bytes.data(), inner_h_bytes.size());
    const String outer_payload_ciphertext = aesCtrEncryptAt(
        key_outer, iv_outer,
        /*stream_offset=*/FileEncryption::Header::kSize,
        inner_ciphertext.data(), inner_ciphertext.size());

    /// 4. Assemble the file: plaintext outer header, ciphertext inner
    ///    header, ciphertext payload.
    String file_bytes;
    {
        WriteBufferFromString wb(file_bytes);
        FileEncryption::Header outer_h;
        outer_h.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        outer_h.key_fingerprint = FileEncryption::calculateKeyFingerprint(key_outer);
        outer_h.init_vector = iv_outer;
        outer_h.write(wb);
        wb.finalize();
    }
    file_bytes += outer_h_ciphertext;
    file_bytes += outer_payload_ciphertext;

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    ReaderExecutor executor(source, objects, {},
        /*window_size=*/plaintext.size() + 2048);
    /// Layers are added outermost-first, innermost-last — same order the
    /// stacked-disk prepareRead chain produces (each layer recurses into
    /// its delegate before appending its own `needDecryption`).
    executor.addDecryptionLayer("/outer", 0,
        [&](UInt128, const String &) { return key_outer; });
    executor.addDecryptionLayer("/inner", 0,
        [&](UInt128, const String &) { return key_inner; });
    executor.initDecryption();

    String result;
    while (true)
    {
        auto w = executor.readNextWindow();
        if (w.empty())
            break;
        for (const auto & n : w.getNodes())
            result.append(n.data(), n.size);
    }
    ASSERT_EQ(result.size(), plaintext.size());
    EXPECT_EQ(result, plaintext);
}

#endif

TEST(ReaderExecutor, MergeRangesOverlapping)
{
    /// Overlapping ranges merge into their union regardless of min_gap > 0.
    /// Without the saturating-subtraction fix, gap = sorted[i].offset - prev.end()
    /// underflows on overlap and the merge branch is skipped, leaving overlapping
    /// ranges in the output.
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {50, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 10);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0u);
    EXPECT_EQ(merged[0].size, 150u);  /// [0, 100) ∪ [50, 150) = [0, 150)
}

TEST(ReaderExecutor, MergeRangesContained)
{
    /// One range fully contained in another. The union is the wider range;
    /// without the fix the underflow path emits both ranges.
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 200}, {50, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 10);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0u);
    EXPECT_EQ(merged[0].size, 200u);  /// [0, 200) ∪ [50, 150) = [0, 200)
}

TEST(ReaderExecutor, ShortReadThrows)
{
    /// offset_map sees obj_a as 1000 bytes but the source has only 300.
    /// readFromSource short-reads pr1, which is non-terminal (obj_b follows).
    /// Pre-fix: obj_b's data would silently land at a shifted logical offset.
    /// Fix: throw CANNOT_READ_ALL_DATA with a clear message.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_a", String(300, 'A')},
            {"obj_b", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_a", "", 1000);  /// claims 1000 but actual is 300
    objects.emplace_back("obj_b", "", 500);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/1500);
    EXPECT_THROW(executor.readNextWindow(), Exception);
}

TEST(ReaderExecutor, MergeAcrossCacheHitDropsCachedNode)
{
    /// When mergeRanges combines two miss ranges across a cached hit, the
    /// source fetches the merged range — which now covers the cached block.
    /// The cache hit must be dropped from the result, otherwise it appears
    /// alongside source data for the same logical offsets (duplicate coverage).
    ///
    /// Layout: cache block [100, 200), miss ranges [0, 100) and [200, 300)
    /// merged into [0, 300) with a large min_bytes_for_seek.

    StoredObjects objects;
    objects.emplace_back("obj", "", 300);

    auto cache = std::make_shared<MockCacheProvider>(100);

    /// Warm cache block 1 (offsets [100, 200)). Content is irrelevant —
    /// the bug is about node count, not content.
    {
        String warm_content(300, 'W');
        auto warm_source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", warm_content}});
        ReaderExecutor warmup(warm_source, objects, {cache}, /*window_size=*/100);
        warmup.seek(100);
        warmup.readNextWindow();
        ASSERT_TRUE(cache->hasBlock(1));
    }

    /// Real read: window covers [0, 300); min_bytes_for_seek=8 MiB so the
    /// two miss ranges around the cached block get merged into [0, 300).
    String real_content(300, 'S');
    auto real_source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", real_content}});

    ReaderExecutor executor(
        real_source, objects, {cache},
        /*window_size=*/300,
        /*min_bytes_for_seek=*/8 * 1024 * 1024);
    auto rope = executor.readNextWindow();

    EXPECT_EQ(rope.range().offset, 0u);
    EXPECT_EQ(rope.range().size, 300u);
    /// Without the fix, the surviving cache hit at [100, 200) adds 100 bytes
    /// of duplicate coverage, so totalBytes() reports 400 instead of 300.
    EXPECT_EQ(rope.totalBytes(), 300u);
}

namespace ProfileEvents
{
    extern const Event LiveSourceBufferCreated;
    extern const Event LiveSourceBufferHits;
    extern const Event LiveSourceBufferFallbacks;
    extern const Event LiveSourceBufferBytes;
}

namespace
{

/// Succeeds on the first open() and throws on every subsequent one.
/// Used to drive an asynchronous failure into the prefetch lambda so the
/// future held by ReaderExecutor ends up holding an exception when the
/// destructor calls future.get() via discardPrefetch.
class ThrowOnSecondOpenSourceReader : public ISourceReader
{
public:
    explicit ThrowOnSecondOpenSourceReader(String data_)
        : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        if (call_count.fetch_add(1) > 0)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "ThrowOnSecondOpenSourceReader: synthetic failure (would abort in debug if LOGICAL_ERROR)");

        auto path = std::filesystem::temp_directory_path() / ("test_throw_on_second_open_" + std::to_string(file_counter++));
        {
            std::ofstream f(path, std::ios::binary);
            f.write(data.data(), data.size());
        }
        temp_files.push_back(path);
        return createReadBufferFromFileBase(path.string(), ReadSettings{});
    }

    String name() const override { return "ThrowOnSecondOpenSourceReader"; }

    ~ThrowOnSecondOpenSourceReader() override
    {
        for (const auto & p : temp_files)
            std::filesystem::remove(p);
    }

private:
    String data;
    std::atomic<size_t> call_count{0};
    size_t file_counter = 0;
    std::vector<std::filesystem::path> temp_files;
};

/// RAII helper: creates a ThreadGroup with its own ProfileEvents counters,
/// attaches the current thread to it, detaches in destructor.
/// Lets us read per-test ProfileEvents without interference from other tests.
struct TestThreadGroup
{
    /// Create ThreadStatus if none exists (debug build has one, ASan may not).
    std::optional<DB::ThreadStatus> thread_status_holder{current_thread ? std::nullopt : std::optional<DB::ThreadStatus>(std::in_place)};
    DB::ThreadGroupPtr thread_group = DB::ThreadGroup::createForQuery(getContext().context);
    DB::ThreadGroupSwitcher switcher{thread_group, ThreadName::UNKNOWN};

    ProfileEvents::Count get(ProfileEvents::Event event) const
    {
        return thread_group->performance_counters[event].load(std::memory_order_relaxed);
    }
};

}

TEST(ReaderExecutor, DestructorTolerantOfThrowingPrefetch)
{
    /// Pre-fix: ~ReaderExecutor calls discardPrefetch → future.get(), which
    /// re-throws the prefetch lambda's exception. Because ~ReaderExecutor is
    /// implicitly noexcept, this terminates the process.
    /// Post-fix: discardPrefetch catches and logs, scope exit is clean.

    auto source = std::make_shared<ThrowOnSecondOpenSourceReader>(String(2000, 'A'));
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    {
        ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
        executor.setPrefetchPool(pool);

        /// First sync read consumes the 1st open() and primes maybeTriggerPrefetch,
        /// which submits a task whose 2nd open() will throw on the pool thread.
        auto rope = executor.readNextWindow();
        ASSERT_FALSE(rope.empty());

        /// executor's destructor must drain the throwing future without terminating.
    }
    SUCCEED();
}

TEST(ReaderExecutor, DestructorAfterThrownReadNextWindowDoesNotSegfault)
{
    /// Reproduces the production segfault observed in stress tests:
    ///   1. First `readNextWindow` succeeds and queues a prefetch.
    ///   2. Second `readNextWindow` waits on the prefetch via `future.get()`,
    ///      which re-throws the worker's exception. `future.get()` detaches
    ///      the future's `__state_` as its very first step, so afterwards the
    ///      future is unusable.
    ///   3. Pre-fix: the throw skipped `prefetch_handle.reset()`, leaving the
    ///      executor with a non-null `prefetch_handle` whose future has already
    ///      been consumed. `~ReaderExecutor → discardPrefetch → get()` then
    ///      segfaulted dereferencing a null `__assoc_state*` at offset 0x28
    ///      (the `__mut_` slot) inside `pthread_mutex_lock`.
    ///   4. Post-fix: `readNextWindow` takes local ownership of the handle and
    ///      clears `prefetch_handle` BEFORE calling `get`,
    ///      so the destructor never re-touches a half-consumed future.

    auto source = std::make_shared<ThrowOnSecondOpenSourceReader>(String(2000, 'B'));
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    {
        ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
        executor.setPrefetchPool(pool);

        /// 1st readNextWindow: synchronous open (success) + queues a prefetch
        /// whose worker will call `open()` again and throw.
        auto rope1 = executor.readNextWindow();
        ASSERT_FALSE(rope1.empty());

        /// 2nd readNextWindow: waits on the prefetch, which re-throws the
        /// worker's exception. This is the path that previously left the
        /// executor in a poisoned state.
        EXPECT_ANY_THROW(executor.readNextWindow());

        /// Now let the executor go out of scope. Pre-fix this segfaulted in
        /// `discardPrefetch`. Post-fix the destructor finishes cleanly because
        /// `prefetch_handle` was already cleared inside `readNextWindow` before
        /// the throw.
    }
    SUCCEED();
}

TEST(ReaderExecutor, ConsumePathCancelledPrefetchIsStashedForDrain)
{
    /// When a queued prefetch is cancelled on the readNextWindow consume path
    /// (the next read arrives before the worker starts it), the handle must be
    /// stashed in `abandoned_prefetches` so ~ReaderExecutor waits for the pool
    /// worker to take the cancellation path before the executor's state (and
    /// the enclosing query's memory-tracker chain) is freed. The worker
    /// attaches a ThreadGroupSwitcher to the submitter's group BEFORE checking
    /// cancellation, so dropping the handle here risked a use-after-free.
    String content(2000, 'Z');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    /// Real single-worker pool. Occupy its one worker with a blocking task so
    /// the executor's prefetch stays Queued (the worker can't pull it), which
    /// makes the consume-path tryCancel succeed deterministically.
    auto pool = std::make_shared<PrefetchThreadPool>(1);

    std::promise<void> worker_started;
    std::promise<void> release_worker;
    auto blocker = pool->submit([&]() -> Rope
    {
        worker_started.set_value();
        release_worker.get_future().wait();
        return Rope{};
    });
    ASSERT_TRUE(blocker != nullptr);
    worker_started.get_future().wait();   /// the one worker is now busy in `blocker`

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setPrefetchPool(pool);

    /// Window 1: synchronous read, then maybeTriggerPrefetch submits a prefetch
    /// for window 2 that queues behind the blocked worker.
    auto w1 = executor.readNextWindow();
    ASSERT_FALSE(w1.empty());
    ASSERT_TRUE(executor.hasInflightPrefetch());

    /// Window 2: the prefetch is still Queued, so tryCancel succeeds on the
    /// consume path and the cancelled handle must be stashed for the drain.
    auto w2 = executor.readNextWindow();
    ASSERT_FALSE(w2.empty());
    EXPECT_EQ(executor.abandonedPrefetchCount(), 1u)
        << "consume-path cancelled prefetch must be stashed for ~ReaderExecutor to drain";

    /// Release the worker so it finishes `blocker`, then pulls the cancelled
    /// prefetch (sets the cancellation exception). ~ReaderExecutor's drain then
    /// get()s it (throws, caught) and returns cleanly.
    release_worker.set_value();
}

TEST(ReaderExecutor, LiveBufferReusesConnection)
{
    TestThreadGroup tg;

    /// 2000 bytes, window=500 → 4 sequential readNextWindow calls.
    String content(2000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    String result;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
    }

    EXPECT_EQ(result.size(), 2000);
    EXPECT_EQ(result, content);
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 1);   /// Opened once.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferHits), 3);      /// Reused for 3 subsequent reads.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 0); /// Never fell back.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferBytes), 2000);  /// All bytes through live buffer.
}

TEST(ReaderExecutor, LiveBufferFallbackWhenFull)
{
    TestThreadGroup tg;

    /// Semaphore with 0 capacity — all reads go through stateless path.
    String content(1000, 'R');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 1000);

    auto limit = std::make_shared<SourceBufferLimit>(0);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    String result;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
    }

    EXPECT_EQ(result.size(), 1000);
    EXPECT_EQ(result, content);
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 0);   /// Never opened — no slots.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferHits), 0);      /// No live buffer to hit.
    EXPECT_GE(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 2); /// All reads fell back.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferBytes), 0);     /// No bytes through live buffer.
}

namespace
{

/// FileCache-style cache with fixed-size segments that the test can evict
/// between windows. Mirrors DiskCacheHandle::status miss-head behavior:
///   - partially downloaded segment -> miss head at current write offset,
///   - empty/evicted segment        -> miss head at segment start.
/// Honors pinSegmentAt using FileCache's releasable() rule (use_count()==1).
class EvictableSegmentMockCache : public ICacheProvider
{
public:
    explicit EvictableSegmentMockCache(size_t segment_size_)
        : segment_size(segment_size_) {}

    std::unique_ptr<ICacheHandle> lookup(const StoredObject &, size_t, ByteRange range) override;

    String name() const override { return "EvictableSegmentMock"; }

    /// Evict every segment not currently pinned by a caller.
    void evictUnpinned()
    {
        for (auto & [idx, live] : liveness)
            if (live.use_count() == 1)
                downloaded[idx] = 0;
    }

    size_t segmentSize() const { return segment_size; }

    std::shared_ptr<int> & livenessFor(size_t idx)
    {
        auto & p = liveness[idx];
        if (!p)
            p = std::make_shared<int>(0);
        return p;
    }

    /// idx -> bytes downloaded from segment start (0 == empty).
    std::unordered_map<size_t, size_t> downloaded;
    /// idx -> liveness token; an extra ref (held by the executor's pin) makes
    /// the segment non-evictable.
    std::unordered_map<size_t, std::shared_ptr<int>> liveness;
    std::vector<std::pair<ByteRange, size_t>> put_log;
    /// idx -> true means put() returns 0 (simulates a cache that refuses writes).
    std::unordered_map<size_t, bool> reject_put;

private:
    size_t segment_size;
};

class EvictableSegmentMockCacheHandle : public ICacheHandle
{
public:
    EvictableSegmentMockCacheHandle(ByteRange requested_, EvictableSegmentMockCache & cache_)
        : requested(requested_), cache(cache_)
    {
        const size_t seg = cache.segmentSize();
        const size_t first = requested.offset / seg;
        const size_t last = requested.end() == 0 ? 0 : (requested.end() - 1) / seg;
        for (size_t idx = first; idx <= last; ++idx)
        {
            const size_t seg_start = idx * seg;
            const size_t seg_end = seg_start + seg;
            const size_t dl = cache.downloaded.contains(idx) ? cache.downloaded[idx] : 0;
            if (dl >= seg)
            {
                result.hit_ranges.push_back(ByteRange{seg_start, seg});
            }
            else if (dl > 0)
            {
                result.hit_ranges.push_back(ByteRange{seg_start, dl});
                const size_t miss_head = seg_start + dl;
                const size_t miss_end = std::min(seg_end, requested.end());
                if (miss_head < miss_end)
                    result.miss_ranges.push_back(ByteRange{miss_head, miss_end - miss_head});
            }
            else
            {
                const size_t miss_end = std::min(seg_end, requested.end());
                if (seg_start < miss_end)
                    result.miss_ranges.push_back(ByteRange{seg_start, miss_end - seg_start});
            }
        }
    }

    CacheLookupResult status() const override { return result; }

    Rope get(ByteRange range) override
    {
        auto buf = std::make_shared<OwnedRopeBuffer>(range.size);
        std::memset(buf->data(), 'D', range.size);
        Rope rope;
        rope.append(RopeNode{buf, 0, range.size, range.offset});
        return rope;
    }

    size_t put(ByteRange range, Rope data) override
    {
        cache.put_log.emplace_back(range, data.totalBytes());
        const size_t seg = cache.segmentSize();
        const size_t idx = range.offset / seg;
        if (auto it = cache.reject_put.find(idx); it != cache.reject_put.end() && it->second)
            return 0;
        const size_t seg_start = idx * seg;
        const size_t cwo = seg_start + (cache.downloaded.contains(idx) ? cache.downloaded[idx] : 0);
        if (range.offset != cwo)   // append-only, like FileSegment::write
            return 0;
        cache.downloaded[idx] = std::min(seg, (range.offset + range.size) - seg_start);
        cache.livenessFor(idx);
        return range.size;
    }

    CacheSegmentPin pinSegmentAt(size_t file_offset) const override
    {
        const size_t seg = cache.segmentSize();
        const size_t idx = file_offset / seg;
        const size_t dl = cache.downloaded.contains(idx) ? cache.downloaded[idx] : 0;
        if (dl == 0 || dl >= seg)
            return nullptr;   // nothing partial to pin
        return std::static_pointer_cast<void>(cache.livenessFor(idx));
    }

private:
    ByteRange requested;
    EvictableSegmentMockCache & cache;
    CacheLookupResult result;
};

inline std::unique_ptr<ICacheHandle> EvictableSegmentMockCache::lookup(const StoredObject &, size_t, ByteRange range)
{
    return std::make_unique<EvictableSegmentMockCacheHandle>(range, *this);
}

} // anonymous namespace

TEST(ReaderExecutor, SequentialMidReadEvictionDoesNotResetConnection)
{
    TestThreadGroup tg;

    /// One 4000-byte object = one 4000-byte cache segment, window 1000.
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    String result;
    auto consume = [&](Rope rope)
    {
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
    };

    /// Window 1: [0,1000). Fills the segment to cwo=1000; the executor pins it.
    auto w1 = executor.readNextWindow();
    ASSERT_FALSE(w1.empty());
    consume(std::move(w1));

    /// Eviction pressure. The pinned segment must survive (use_count()==2).
    cache->evictUnpinned();
    ASSERT_EQ(cache->downloaded[0], 1000u) << "pinned in-flight segment was evicted";

    /// Drain the rest sequentially.
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        consume(std::move(rope));
    }

    EXPECT_EQ(result, content);                       /// no corruption / no missing bytes
    EXPECT_EQ(executor.getSourceRequestsCount(), 1u); /// connection opened exactly once
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 1);
}

TEST(ReaderExecutor, MultipleEvictionsKeepSingleConnection)
{
    TestThreadGroup tg;
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto limit = std::make_shared<SourceBufferLimit>(10);
    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    String result;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
        EXPECT_LE(limit->getActive().size(), 1u);   /// no slot churn
        cache->evictUnpinned();                      /// eviction pressure before next window
    }

    EXPECT_EQ(result, content);
    EXPECT_EQ(executor.getSourceRequestsCount(), 1u);
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 1);
}

TEST(ReaderExecutor, PinReleasedOnSeek)
{
    String content(8000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 8000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);  /// two segments
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));

    ASSERT_FALSE(executor.readNextWindow().empty());      /// [0,1000) fills + pins segment 0
    ASSERT_EQ(cache->downloaded[0], 1000u);
    cache->evictUnpinned();
    ASSERT_EQ(cache->downloaded[0], 1000u) << "segment 0 should be pinned before the seek";

    executor.seek(5000);                                  /// continuity breaks -> pin released
    cache->evictUnpinned();
    EXPECT_EQ((cache->downloaded.contains(0) ? cache->downloaded[0] : 0u), 0u)
        << "pin should be released on seek, allowing eviction of segment 0";

    auto rope = executor.readNextWindow();                /// [5000,6000)
    ASSERT_FALSE(rope.empty());
    String got;
    for (const auto & node : rope.getNodes())
        got.append(node.data(), node.size);
    EXPECT_EQ(got, content.substr(5000, got.size()));
}

TEST(ReaderExecutor, PutFailedTakesNoPin)
{
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    cache->reject_put[0] = true;            /// segment 0 never accepts writes
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));

    auto rope = executor.readNextWindow();   /// [0,1000)
    ASSERT_FALSE(rope.empty());
    String got;
    for (const auto & node : rope.getNodes())
        got.append(node.data(), node.size);
    EXPECT_EQ(got, content.substr(0, got.size()));   /// data still correct from source
    EXPECT_FALSE(cache->liveness.contains(0));        /// nothing downloaded -> no pin token
}

TEST(ReaderExecutor, TransientReadDoesNotPin)
{
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    auto transient = executor.makeTransientForReadAt(0);
    ASSERT_TRUE(transient != nullptr);
    auto rope = transient->readNextWindow();
    ASSERT_FALSE(rope.empty());

    /// A transient one-shot read has no buffer_limit -> no live buffer -> the
    /// pin is cleared each window. Nothing should survive an eviction sweep.
    cache->evictUnpinned();
    EXPECT_EQ((cache->downloaded.contains(0) ? cache->downloaded[0] : 0u), 0u);
}

TEST(SourceBufferLimit, MoveAssignReleasesPreviousSlot)
{
    /// Defaulted move-assignment used to overwrite `limit` / `slot_id`
    /// without calling `release` on the previously held slot, permanently
    /// pinning one unit of capacity. The explicit move-assignment must
    /// release the old slot first.
    auto limit = std::make_shared<SourceBufferLimit>(2);

    auto a = limit->tryAcquire(limit, "obj-a");
    auto b = limit->tryAcquire(limit, "obj-b");
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(limit->getActive().size(), 2u);

    /// Move-assign `b` into `a` — slot for `obj-a` must come back.
    *a = std::move(*b);
    EXPECT_EQ(limit->getActive().size(), 1u);

    /// The remaining active slot is the one moved from `b` (obj-b).
    EXPECT_EQ(limit->getActive().front().object_path, "obj-b");

    a.reset();
    b.reset();
    EXPECT_EQ(limit->getActive().size(), 0u);
}

TEST(SourceBufferLimit, MoveAssignFromEmptySlotReleasesCurrent)
{
    /// Edge case: assigning a moved-from / empty `SourceBufferSlot` into a
    /// holding one should still release the current slot.
    auto limit = std::make_shared<SourceBufferLimit>(2);

    auto a = limit->tryAcquire(limit, "obj-a");
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(limit->getActive().size(), 1u);

    auto empty = limit->tryAcquire(limit, "obj-tmp");
    ASSERT_TRUE(empty.has_value());
    auto moved_from = std::move(*empty);  // `*empty` is now empty (no slot)
    (void)moved_from;                      // moved_from owns the slot
    EXPECT_EQ(limit->getActive().size(), 2u);

    *a = std::move(*empty);   // assign an empty slot — must drop a's slot
    EXPECT_EQ(limit->getActive().size(), 1u);
}

TEST(SourceBufferLimit, SelfMoveAssignIsNoOp)
{
    /// Self-assignment (e.g. `*s = std::move(*s)`) must not double-release.
    auto limit = std::make_shared<SourceBufferLimit>(1);
    auto s = limit->tryAcquire(limit, "obj");
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(limit->getActive().size(), 1u);

    *s = std::move(*s);   // not a real-world pattern, but must be safe
    EXPECT_EQ(limit->getActive().size(), 1u);
}

TEST(ReaderExecutor, LiveBufferReleasedAtEof)
{
    /// Once the caller reads to EOF, the per-stream `SourceBufferLimit`
    /// slot (and the associated open connection) must be returned even if
    /// the `ReaderExecutor` itself is not yet destroyed. Otherwise a
    /// finished-but-still-held reader pins capacity from the global
    /// budget.
    String content(2000, 'E');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read every window — last call returns an empty rope (EOF).
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().size, 500u);
    EXPECT_EQ(limit->getActive().size(), 1u) << "expected one open slot during streaming";

    auto r2 = executor.readNextWindow();
    auto r3 = executor.readNextWindow();
    auto r4 = executor.readNextWindow();
    EXPECT_EQ(r2.range().size, 500u);
    EXPECT_EQ(r3.range().size, 500u);
    EXPECT_EQ(r4.range().size, 500u);
    EXPECT_EQ(limit->getActive().size(), 1u);

    /// EOF — slot must be released by readNextWindow itself.
    auto r5 = executor.readNextWindow();
    EXPECT_TRUE(r5.empty());
    EXPECT_EQ(limit->getActive().size(), 0u)
        << "live buffer slot must be released when EOF is reached";

    /// Idempotent: calling readNextWindow again at EOF is still EOF and
    /// keeps the slot count at zero.
    auto r6 = executor.readNextWindow();
    EXPECT_TRUE(r6.empty());
    EXPECT_EQ(limit->getActive().size(), 0u);
}

TEST(PageCacheProvider, BypassMissDoesNotPolluteCache)
{
    /// `read_from_page_cache_if_exists_otherwise_bypass_cache = true`
    /// (e.g. background merges/mutations via `MergeTreeSequentialSource`)
    /// must not populate the page cache on miss. Confirm by writing data
    /// through a bypass-mode provider and verifying that a fresh handle
    /// from a non-bypass provider on the same `PageCacheFile` sees a
    /// MISS — the bytes never made it into the registered cache.
    using namespace DB;
    /// `min_size_in_bytes` is the initial per-shard capacity. Tests don't
    /// call `autoResize`, so we set it equal to `max_size_in_bytes` so the
    /// shard can actually store entries from the get-go.
    constexpr size_t cache_capacity = 1ull << 20;
    auto cache = std::make_shared<PageCache>(
        std::chrono::milliseconds(2000), "LRU", 0.5,
        /*min_size_in_bytes=*/cache_capacity,
        /*max_size_in_bytes=*/cache_capacity,
        /*free_memory_ratio=*/0.0,
        /*num_shards=*/1);

    PageCacheFile file;
    file.path = "test-bypass";
    file.file_version = "v1";

    constexpr size_t block_size = 4096;
    constexpr size_t inject_eviction = false;

    /// Step 1: bypass=true. Put bytes into block [0, 4096).
    PageCacheProvider bypass_provider(cache, file, block_size, inject_eviction, /*bypass_if_missing=*/true, /*file_size_in_bytes=*/block_size);
    {
        auto handle = bypass_provider.lookup(StoredObject{}, /*object_file_offset=*/0, ByteRange{0, block_size});
        auto status = handle->status();
        ASSERT_EQ(status.hit_ranges.size(), 0u);
        ASSERT_EQ(status.miss_ranges.size(), 1u);

        Rope data;
        auto buf = std::make_shared<OwnedRopeBuffer>(block_size);
        std::memset(buf->data(), 'B', block_size);
        data.append(RopeNode{buf, 0, block_size, 0});

        size_t written = handle->put(ByteRange{0, block_size}, std::move(data));
        EXPECT_EQ(written, block_size);
    }   /// bypass handle drops here; if the cell was detached, the data is
        /// reclaimed; the cache stays empty.

    /// Step 2: lookup via a NON-bypass provider on the same file/block.
    /// Should be a MISS — bypass mode did not register the bytes.
    PageCacheProvider observer_provider(cache, file, block_size, inject_eviction, /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);
    {
        auto handle = observer_provider.lookup(StoredObject{}, /*object_file_offset=*/0, ByteRange{0, block_size});
        auto status = handle->status();
        EXPECT_EQ(status.hit_ranges.size(), 0u)
            << "bypass-mode put must NOT register the cell with the cache; "
               "subsequent lookups must miss";
        EXPECT_EQ(status.miss_ranges.size(), 1u);
    }
}

TEST(PageCacheProvider, NonBypassMissPopulatesCache)
{
    /// Symmetry check: when bypass is OFF, put DOES populate the cache,
    /// and a later lookup hits.
    using namespace DB;
    /// `min_size_in_bytes` is the initial per-shard capacity. Tests don't
    /// call `autoResize`, so we set it equal to `max_size_in_bytes` so the
    /// shard can actually store entries from the get-go.
    constexpr size_t cache_capacity = 1ull << 20;
    auto cache = std::make_shared<PageCache>(
        std::chrono::milliseconds(2000), "LRU", 0.5,
        /*min_size_in_bytes=*/cache_capacity,
        /*max_size_in_bytes=*/cache_capacity,
        /*free_memory_ratio=*/0.0,
        /*num_shards=*/1);

    PageCacheFile file;
    file.path = "test-populate";
    file.file_version = "v1";

    constexpr size_t block_size = 4096;
    constexpr size_t inject_eviction = false;

    PageCacheProvider provider(cache, file, block_size, inject_eviction, /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    {
        auto handle = provider.lookup(StoredObject{}, 0, ByteRange{0, block_size});
        EXPECT_EQ(handle->status().miss_ranges.size(), 1u);

        Rope data;
        auto buf = std::make_shared<OwnedRopeBuffer>(block_size);
        std::memset(buf->data(), 'P', block_size);
        data.append(RopeNode{buf, 0, block_size, 0});
        handle->put(ByteRange{0, block_size}, std::move(data));
    }

    {
        auto handle = provider.lookup(StoredObject{}, 0, ByteRange{0, block_size});
        EXPECT_EQ(handle->status().hit_ranges.size(), 1u)
            << "non-bypass put must register the cell so subsequent lookups hit";
    }
}

TEST(PageCacheProvider, TailBlockSizedToFile)
{
    /// File size 1500, block size 1024. The tail block must be sized to the
    /// 476 real bytes, NOT to a full 1024-byte block. With the cell sized to
    /// real data, `get` has no past-EOF region to ever serve back (regardless
    /// of what range the caller asks for), and `put` cannot leave a zero-filled
    /// trailing gap that a subsequent reader could mistake for file content.
    using namespace DB;
    constexpr size_t cache_capacity = 1ull << 20;
    auto cache = std::make_shared<PageCache>(
        std::chrono::milliseconds(2000), "LRU", 0.5,
        /*min_size_in_bytes=*/cache_capacity,
        /*max_size_in_bytes=*/cache_capacity,
        /*free_memory_ratio=*/0.0,
        /*num_shards=*/1);

    PageCacheFile file;
    file.path = "test-tail-clamp";
    file.file_version = "v1";

    constexpr size_t file_size = 1500;
    constexpr size_t block_size = 1024;
    constexpr bool inject_eviction = false;
    PageCacheProvider provider(cache, file, block_size, inject_eviction, /*bypass_if_missing=*/false, file_size);

    /// Populate both blocks.
    {
        auto handle = provider.lookup(StoredObject{}, 0, ByteRange{0, file_size});
        auto status = handle->status();
        ASSERT_EQ(status.miss_ranges.size(), 2u);
        EXPECT_EQ(status.miss_ranges[0].size, 1024u);
        EXPECT_EQ(status.miss_ranges[1].offset, 1024u);
        EXPECT_EQ(status.miss_ranges[1].size, 476u) << "tail block must be sized to valid bytes";

        Rope data;
        auto buf0 = std::make_shared<OwnedRopeBuffer>(1024);
        std::memset(buf0->data(), '0', 1024);
        data.append(RopeNode{buf0, 0, 1024, 0});
        auto buf1 = std::make_shared<OwnedRopeBuffer>(476);
        std::memset(buf1->data(), '1', 476);
        data.append(RopeNode{buf1, 0, 476, 1024});
        handle->put(ByteRange{0, 1024}, data.slice(ByteRange{0, 1024}));
        handle->put(ByteRange{1024, 476}, data.slice(ByteRange{1024, 476}));
    }

    /// Now read across the tail block with a range that would extend past EOF
    /// if the cell were full block_size. Even an explicit attempt to read
    /// [1024, 2048) returns only 476 bytes — the cell physically has no more.
    {
        auto handle = provider.lookup(StoredObject{}, 0, ByteRange{1024, block_size});
        auto status = handle->status();
        ASSERT_EQ(status.hit_ranges.size(), 1u);
        EXPECT_EQ(status.hit_ranges[0].size, 476u);

        Rope rope = handle->get(ByteRange{1024, block_size});
        EXPECT_EQ(rope.totalBytes(), 476u);
    }
}

TEST(ReaderExecutor, UnknownSizeStreamsToEof)
{
    /// When `StoredObject::bytes_size == UnknownSize`,
    /// `OffsetMap::hasUnknownSize` is true and the executor switches to
    /// streaming-until-EOF: it reads `window_size` bytes at a time from
    /// the source and detects EOF when the source returns short. The
    /// source itself (`MemorySourceReader` backed by a temp file) knows
    /// the real size; only the executor's view is unknown.
    String content(1500, 'U');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);

    String collected;
    while (true)
    {
        Rope w = executor.readNextWindow();
        if (w.empty())
            break;
        for (const auto & node : w.getNodes())
            collected.append(node.data(), node.size);
    }
    EXPECT_EQ(collected.size(), content.size());
    EXPECT_EQ(collected, content);
}

TEST(ReaderExecutor, UnknownSizeEofIsLatchedUntilSeek)
{
    /// After the source returns short and EOF is latched, subsequent
    /// `readNextWindow` calls keep returning empty without re-hitting
    /// the source. A backward `seek` clears the latch so reads resume.
    String content(600, 'L');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/1000);

    auto r1 = executor.readNextWindow();   /// reads all 600 bytes, latches EOF
    EXPECT_EQ(r1.range().size, 600u);

    /// Latched: stays empty without re-reading.
    EXPECT_TRUE(executor.readNextWindow().empty());
    EXPECT_TRUE(executor.readNextWindow().empty());

    /// Seek back to position 0 — latch cleared, reads resume.
    executor.seek(0);
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().size, 600u);
    EXPECT_EQ(r2.range().offset, 0u);
}

TEST(ReaderExecutor, UnknownSizeZeroByteTerminalReleasesLiveSlot)
{
    /// Unknown-size source whose content is an exact multiple of the window
    /// size, so the terminal live read returns 0 bytes: readNextWindow returns
    /// an empty rope and the caller stops, never making the follow-up call that
    /// would hit the pre-read EOF gate. The live buffer + its SourceBufferLimit
    /// slot must still be released as soon as EOF is latched, not leaked until
    /// the executor is destroyed.
    String content(1000, 'E');   /// exactly 2 * window
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    auto limit = std::make_shared<SourceBufferLimit>(10);
    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
    executor.setBufferLimit(limit);

    String collected;
    while (true)
    {
        Rope w = executor.readNextWindow();
        if (w.empty())
            break;
        for (const auto & node : w.getNodes())
            collected.append(node.data(), node.size);
    }
    EXPECT_EQ(collected, content);
    EXPECT_EQ(limit->getActive().size(), 0u)
        << "live buffer + slot must be released when EOF is latched on a zero-byte terminal read";
}

TEST(ReaderExecutor, UnknownSizeMultiObjectRejected)
{
    /// Multi-object pipelines need each object's `bytes_size` to compute
    /// the cumulative `logical_offset`. With an unknown size we can't.
    /// `OffsetMap::build` rejects the combination outright.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"a", "AA"},
            {"b", "BB"},
        });

    StoredObjects objects;
    objects.emplace_back("a", "", StoredObject::UnknownSize);
    objects.emplace_back("b", "", 2);

    EXPECT_ANY_THROW({
        ReaderExecutor executor(source, objects, {}, /*window_size=*/100);
    });
}

TEST(ReaderExecutor, LiveBufferReacquiredAfterSeekBackFromEof)
{
    /// After EOF released the slot, a backward seek and re-read must
    /// re-open the connection and re-acquire a slot.
    String content(1500, 'R');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 1500);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read to EOF.
    while (!executor.readNextWindow().empty()) {}
    EXPECT_EQ(limit->getActive().size(), 0u);

    /// Seek back to the start and read again — slot must come back.
    executor.seek(0);
    auto r = executor.readNextWindow();
    EXPECT_EQ(r.range().offset, 0u);
    EXPECT_EQ(r.range().size, 500u);
    EXPECT_EQ(limit->getActive().size(), 1u)
        << "slot must be re-acquired after backward seek + read";
}

TEST(ReaderExecutor, CacheOnlyWindowClosesStaleLiveBuffer)
{
    /// Window 1 misses -> opens a live connection at the frontier. Window 2 is
    /// served entirely from cache (no source read), leaving the live buffer
    /// parked behind the new position where it can no longer continue the
    /// stream. It must be closed (slot freed) at the end of the cache-only
    /// window, not held idle until the next source miss / EOF.
    constexpr size_t window_bytes = 1000;
    String content(3 * window_bytes, 'X');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 3 * window_bytes);

    /// Segment 1 ([window, 2*window)) pre-marked fully downloaded so window 2
    /// is a pure cache hit; segments 0 and 2 are empty, so windows 1 and 3
    /// would read from source.
    auto cache = std::make_shared<EvictableSegmentMockCache>(window_bytes);
    cache->downloaded[1] = window_bytes;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto limit = std::make_shared<SourceBufferLimit>(10);
    ReaderExecutor executor(source, objects, caches, /*window_size=*/window_bytes, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Window 1 [0, window): miss -> source read -> live connection open.
    auto w1 = executor.readNextWindow();
    ASSERT_FALSE(w1.empty());
    EXPECT_EQ(limit->getActive().size(), 1u) << "window 1 opens a live connection";

    /// Window 2 [window, 2*window): full cache hit, no source read.
    auto w2 = executor.readNextWindow();
    ASSERT_FALSE(w2.empty());
    EXPECT_EQ(limit->getActive().size(), 0u)
        << "cache-only window must close the now-stale live connection";
}

TEST(ReaderExecutor, SeekClosesStaleLiveBufferEvenWithoutReadFromSource)
{
    /// Regression: `seek` used to defer closing a stale `live_buffer` to
    /// `readFromSource`. If the next window was fully cache-served (or just
    /// nothing was read after seek), the old connection — and its
    /// `SourceBufferLimit` slot — stayed open until EOF or executor
    /// destruction, burning `max_remote_read_connections` capacity.
    String content_a(2000, 'A');
    String content_b(2000, 'B');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"a", content_a}, {"b", content_b}});

    StoredObjects objects;
    objects.emplace_back("a", "", 2000);
    objects.emplace_back("b", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read from object "a" — opens a live buffer + acquires a slot.
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 500u);
    EXPECT_EQ(limit->getActive().size(), 1u);

    /// Seek into object "b". No read afterwards — the stale connection to
    /// "a" must be closed by `seek` itself, not by a future `readFromSource`.
    executor.seek(2500);
    EXPECT_EQ(limit->getActive().size(), 0u)
        << "stale live buffer + slot must be released by seek when the "
           "target is in a different object";
}

TEST(ReaderExecutor, LiveBufferClosedOnSeek)
{
    TestThreadGroup tg;

    /// Sequential read opens live buffer, seek closes it and opens a new one.
    String content(2000, 'S');
    content[1000] = 'T';
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read first window (opens live buffer).
    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().size, 500);

    /// Seek (closes live buffer, next read opens a new one).
    executor.seek(1000);
    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 1000);
    EXPECT_EQ(rope2.getNodes()[0].data()[0], 'T');

    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 2);   /// Opened twice: initial + after seek.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferHits), 0);      /// Seek broke the chain.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 0); /// Slots were available.
}

namespace
{

/// Mock cache that reports hits/misses at FULL block granularity, matching the
/// production behaviour of PageCacheHandle and DiskCacheHandle. The existing
/// MockCacheHandle clips ranges to the request which hides the cache-vs-cache
/// overlap problem these tests target.
///
/// `put` records its arguments in `put_log` and refuses non-disjoint ropes
/// (totalBytes != range.size) — production caches assume disjoint coverage,
/// and DiskCacheHandle::put's flat_buf memcpy would overflow otherwise.
class WideGranularityMockCacheHandle : public ICacheHandle
{
public:
    WideGranularityMockCacheHandle(
        ByteRange requested_,
        std::unordered_map<size_t, String> & storage_,
        std::vector<std::pair<ByteRange, size_t>> & put_log_,
        size_t block_size_)
        : storage(storage_), put_log(put_log_), block_size(block_size_)
    {
        size_t start_block = requested_.offset / block_size;
        size_t end_block = (requested_.end() + block_size - 1) / block_size;
        for (size_t b = start_block; b < end_block; ++b)
        {
            /// FULL block, NOT clipped to requested_.
            ByteRange block_range{b * block_size, block_size};
            if (storage.contains(b))
                result.hit_ranges.push_back(block_range);
            else
                result.miss_ranges.push_back(block_range);
        }
    }

    CacheLookupResult status() const override { return result; }

    Rope get(ByteRange range) override
    {
        Rope rope;
        size_t start_block = range.offset / block_size;
        size_t end_block = (range.end() + block_size - 1) / block_size;
        for (size_t b = start_block; b < end_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            rope.append(RopeNode{buf, 0, data.size(), b * block_size});
        }
        return rope.slice(range);
    }

    size_t put(ByteRange range, Rope data) override
    {
        put_log.emplace_back(range, data.totalBytes());

        /// Production caches assume disjoint coverage. If totalBytes doesn't
        /// match range.size, the chain handed us duplicate coverage — refuse.
        if (data.totalBytes() != range.size)
            return 0;

        size_t bytes_written = 0;
        size_t start_block = range.offset / block_size;
        size_t end_block = (range.end() + block_size - 1) / block_size;
        for (size_t b = start_block; b < end_block; ++b)
        {
            if (storage.contains(b))
                continue;
            ByteRange block_range{b * block_size, block_size};
            Rope slice = data.slice(block_range);
            String content;
            content.resize(slice.totalBytes());
            size_t pos = 0;
            for (const auto & node : slice.getNodes())
            {
                std::memcpy(content.data() + pos, node.data(), node.size);
                pos += node.size;
            }
            bytes_written += content.size();
            storage[b] = std::move(content);
        }
        return bytes_written;
    }

private:
    std::unordered_map<size_t, String> & storage;
    std::vector<std::pair<ByteRange, size_t>> & put_log;
    size_t block_size;
    CacheLookupResult result;
};

class WideGranularityMockCache : public ICacheProvider
{
public:
    WideGranularityMockCache(size_t block_size_, String name_)
        : block_size(block_size_), provider_name(std::move(name_)) {}

    std::unique_ptr<ICacheHandle> lookup(const StoredObject &, size_t, ByteRange range) override
    {
        return std::make_unique<WideGranularityMockCacheHandle>(
            range, storage, put_log, block_size);
    }

    String name() const override { return provider_name; }

    bool hasBlock(size_t block_index) const { return storage.contains(block_index); }

    /// (range argument to put, totalBytes of rope argument to put)
    const std::vector<std::pair<ByteRange, size_t>> & putLog() const { return put_log; }

    /// Pre-fill a block; used to seed cache state before a test.
    void seedBlock(size_t block_index, char fill)
    {
        storage[block_index] = String(block_size, fill);
    }

private:
    std::unordered_map<size_t, String> storage;
    std::vector<std::pair<ByteRange, size_t>> put_log;
    size_t block_size;
    String provider_name;
};

}

/// Sanity: two caches with the SAME granularity, complementary hits. No
/// overlap by construction. Expected to pass even pre-fix.
TEST(ReaderExecutor, ChainTwoTierDisjointHits)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');
    disk_cache->seedBlock(1, 'D');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 128u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u);
}

/// THE BUG: PageCache (64K blocks) hits block 0, misses block 1. DiskCache
/// (4M blocks) holds the whole 4M segment, so for the chain's lookup of
/// [64K, 128K) it reports a hit at [0, 4M) — covering bytes already in
/// PageCache. Returned rope today has duplicate coverage.
TEST(ReaderExecutor, ChainLowerCacheHitCoversUpperHit)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');
    disk_cache->seedBlock(0, 'D');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 128u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u)
        << "Returned rope must not contain duplicate coverage from cache-vs-cache overlap";
}

/// Three caches with 64K / 1M / 4M granularities. PageCache hits, MidCache
/// hit covers PageCache, DiskCache not reached. Returned rope must be
/// disjoint and equal to window size.
TEST(ReaderExecutor, ChainThreeTierCascading)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto mid_cache  = std::make_shared<WideGranularityMockCache>(1024 * 1024, "MidMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');
    mid_cache->seedBlock(0, 'M');
    disk_cache->seedBlock(0, 'D');

    ReaderExecutor executor(src, objects, {page_cache, mid_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 128u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u);
}

/// After a read where PageCache hits but DiskCache is cold, the DiskCache
/// must be filled with its full segment range (cached hit bytes + source
/// bytes combined into one disjoint rope for the put).
TEST(ReaderExecutor, ChainLowerCacheFilledFullyAfterRead)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u);
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "DiskCache must be filled with the full segment after the read";
}

/// Every put across the chain must receive a rope with disjoint coverage —
/// totalBytes == range.size. A rope with duplicate nodes would overflow
/// DiskCacheHandle::put's flat_buf.
TEST(ReaderExecutor, ChainPutReceivesDisjointRope)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    executor.readNextWindow();

    ASSERT_FALSE(disk_cache->putLog().empty()) << "DiskCache put must be called";
    for (const auto & [range, total] : disk_cache->putLog())
        EXPECT_EQ(total, range.size)
            << "put received non-disjoint rope: range=[" << range.offset
            << ", " << range.end() << "), totalBytes=" << total;
}

/// Cache block extends past the window's tail. Returned rope must end at
/// the window boundary, not at the block boundary.
TEST(ReaderExecutor, ChainHitExtendsBeyondWindowEnd)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache},
                             /*window_size=*/50 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 0u);
    EXPECT_EQ(rope.range().size, 50u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 50u * 1024u);
}

/// Window starts inside a cache block. Bytes before window.offset must not
/// appear in the returned rope.
TEST(ReaderExecutor, ChainHitExtendsBeforeWindowStart)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache},
                             /*window_size=*/40 * 1024,
                             /*min_bytes_for_seek=*/0);

    executor.seek(10 * 1024);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 10u * 1024u);
    EXPECT_EQ(rope.range().size, 40u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 40u * 1024u);
}

/// Cache miss range extends past the window end (cache block is larger than
/// the window's tail). Source fetch goes past the window to fill the block;
/// cache stores the full block; user rope is exactly window size.
TEST(ReaderExecutor, ChainWindowEndCacheMissExtendsPast)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto disk_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "DiskMock");

    ReaderExecutor executor(src, objects, {disk_cache},
                             /*window_size=*/50 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 50u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 50u * 1024u);
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "Cache block must be filled past the window end (intentional read-ahead)";
}

namespace
{
    /// Records every `lookup` call so tests can assert which `StoredObject`
    /// and `object_file_offset` the cache provider received per piece.
    /// `status()` always reports the whole range as a miss so the executor
    /// falls through to the source — keeps the data path simple.
    struct TrackedLookup
    {
        String remote_path;
        size_t object_file_offset;
        ByteRange range_in_file;
    };

    class TrackingCacheHandle : public ICacheHandle
    {
    public:
        explicit TrackingCacheHandle(ByteRange range_) : range(range_) {}
        CacheLookupResult status() const override
        {
            CacheLookupResult r;
            r.miss_ranges.push_back(range);
            return r;
        }
        Rope get(ByteRange) override { return {}; }
        size_t put(ByteRange, Rope) override { return 0; }
    private:
        ByteRange range;
    };

    class TrackingCacheProvider : public ICacheProvider
    {
    public:
        std::unique_ptr<ICacheHandle> lookup(
            const StoredObject & object,
            size_t object_file_offset,
            ByteRange range_in_file) override
        {
            log.push_back(TrackedLookup{object.remote_path, object_file_offset, range_in_file});
            return std::make_unique<TrackingCacheHandle>(range_in_file);
        }
        String name() const override { return "Tracking"; }

        std::vector<TrackedLookup> log;
    };
}

TEST(ReaderExecutor, CacheLookupSplitByObjectBoundary)
{
    /// A single physical request that spans two objects must be issued to
    /// the cache as TWO `lookup` calls — one per object — each carrying
    /// the right `StoredObject` and `object_file_offset`. Previously the
    /// executor handed the cache a single file-level range with one
    /// (executor-wide) cache key, so caches that key per object (the new
    /// `DiskCacheProvider`) couldn't tell the bytes apart.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"blob_A", String(300, 'A')},
            {"blob_B", String(200, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("blob_A", "", 300);
    objects.emplace_back("blob_B", "", 200);

    auto tracker = std::make_shared<TrackingCacheProvider>();

    ReaderExecutor executor(
        source, objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{tracker},
        /*window_size=*/500);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 500u);

    ASSERT_EQ(tracker->log.size(), 2u);

    EXPECT_EQ(tracker->log[0].remote_path, "blob_A");
    EXPECT_EQ(tracker->log[0].object_file_offset, 0u);
    EXPECT_EQ(tracker->log[0].range_in_file.offset, 0u);
    EXPECT_EQ(tracker->log[0].range_in_file.size, 300u);

    EXPECT_EQ(tracker->log[1].remote_path, "blob_B");
    EXPECT_EQ(tracker->log[1].object_file_offset, 300u);
    EXPECT_EQ(tracker->log[1].range_in_file.offset, 300u);
    EXPECT_EQ(tracker->log[1].range_in_file.size, 200u);
}

TEST(ReaderExecutor, PreAcquiredSlotMatchesObjectAtCursor)
{
    /// Previously `ensurePreAcquiredSlot` blindly pre-acquired for the
    /// FIRST object's path. A `seek` into a later object would acquire a
    /// slot for object A that the next source read (against object B)
    /// couldn't consume, then re-acquire another slot — and the first
    /// slot stayed pinned. The fix: pre-acquire for the object that
    /// covers the cursor's current `position`.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_A", String(500, 'A')},
            {"obj_B", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_A", "", 500);
    objects.emplace_back("obj_B", "", 500);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/200, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Seek past the first object before any reads.
    executor.seek(700);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 700u);

    /// Exactly one active slot, and its path is obj_B (not obj_A).
    const auto active = limit->getActive();
    ASSERT_EQ(active.size(), 1u);
    EXPECT_EQ(active.front().object_path, "obj_B")
        << "pre-acquired slot must match the object at the cursor, not the first object in the file";
}

TEST(ReaderExecutor, SlotReleasedOnSeekToDifferentObject)
{
    /// After reading from object A, a seek into object B must release
    /// A's slot (live buffer for A is now stale) so total active slots
    /// stay at 1 — not grow with each cross-object seek.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_A", String(500, 'A')},
            {"obj_B", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_A", "", 500);
    objects.emplace_back("obj_B", "", 500);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/200, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// First read from object A.
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().offset, 0u);
    EXPECT_EQ(limit->getActive().size(), 1u);

    /// Seek into object B and read.
    executor.seek(750);
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().offset, 750u);

    const auto active = limit->getActive();
    ASSERT_EQ(active.size(), 1u) << "must not accumulate slots across cross-object seeks";
    EXPECT_EQ(active.front().object_path, "obj_B");
}

namespace
{

/// Mock prefetch pool whose `submit` always returns nullptr ("queue full"
/// fallback). The executor still calls `ensurePreAcquiredSlot` before the
/// submit attempt, so the pre-acquired slot DOES get set — exposing the
/// leak path we want to exercise without needing real worker threads.
class FakePrefetchPool : public PrefetchThreadPool
{
public:
    FakePrefetchPool() : PrefetchThreadPool(NoWorkers{}) {}
    std::unique_ptr<PrefetchHandle> submit(std::function<Rope()> /*task*/) override
    {
        return nullptr;
    }
};

/// Mock pool that runs every submitted task synchronously on the calling
/// thread and returns a `Done`-state handle holding the produced rope.
/// Eliminates worker-thread timing from prefetch-related tests.
class SyncPrefetchPool : public PrefetchThreadPool
{
public:
    SyncPrefetchPool() : PrefetchThreadPool(NoWorkers{}) {}
    std::unique_ptr<PrefetchHandle> submit(std::function<Rope()> task) override
    {
        return makeCompletedHandleForTest(task());
    }
};

}

TEST(ReaderExecutor, PreAcquiredSlotReleasedOnCrossObjectSeek)
{
    /// Pre-fix: `seek` cleared `prefetch_handle` but left `pre_acquired_slot`
    /// alone. After seeking across objects, the
    /// next `readFromSource` saw a path mismatch, fell through to acquire
    /// a fresh slot for the new object, and silently held the stale slot
    /// until executor destruction — a slow leak of
    /// `max_remote_read_connections` capacity under cross-object random
    /// reads.
    ///
    /// The fake prefetch pool returns nullptr from `submit`, which leaves
    /// `prefetch_handle` null but lets `ensurePreAcquiredSlot` fire first
    /// — exactly the state needed to reproduce the leak.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_A", String(500, 'A')},
            {"obj_B", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_A", "", 500);
    objects.emplace_back("obj_B", "", 500);

    auto limit = std::make_shared<SourceBufferLimit>(10);
    auto pool = std::make_shared<FakePrefetchPool>();

    ReaderExecutor executor(source, objects, {}, /*window_size=*/200, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);
    executor.setPrefetchPool(pool);

    /// Trigger `maybeTriggerPrefetch` at the tail of `seek(0)` — pre-acquires
    /// a slot for obj_A (cursor lies in A), submit returns nullptr from the
    /// fake. State: `pre_acquired_slot` holds an obj_A slot, no prefetch.
    executor.seek(0);
    {
        const auto active = limit->getActive();
        ASSERT_EQ(active.size(), 1u);
        EXPECT_EQ(active.front().object_path, "obj_A")
            << "post-seek prefetch path must pre-acquire for the cursor's object";
    }

    /// Seek into obj_B. The fix's reset drops the obj_A slot before the
    /// tail `maybeTriggerPrefetch` re-acquires for obj_B.
    executor.seek(700);

    /// Drive the read, which consumes the (now obj_B) pre-acquired slot.
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 700u);

    /// Without the fix: 2 active slots (stale obj_A + live obj_B).
    /// With the fix: exactly 1, for obj_B.
    const auto active = limit->getActive();
    ASSERT_EQ(active.size(), 1u)
        << "cross-object seek must release the stale pre-acquired slot";
    EXPECT_EQ(active.front().object_path, "obj_B");
}

/// Pre-fix: for an unknown-size source, the prefetch worker can set
/// `reached_eof = true` mid-flight while still producing a partial rope
/// with the real final bytes. `readNextWindow`'s EOF gate ran BEFORE the
/// `if (prefetch_handle)` branch, so the foreground call short-circuited
/// to `discardPrefetch()` + `return {}` and dropped those bytes.
///
/// With the fix, the prefetch is consumed first; only the no-prefetch
/// branch applies the EOF gate.
TEST(ReaderExecutor, UnknownSizePrefetchedFinalBytesAreServed)
{
    /// 30 bytes "ABAB...". The source has the real bytes; the executor is
    /// told the size is unknown, so it discovers EOF only via a short
    /// return from the source.
    constexpr size_t total = 30;
    String content(total, 0);
    for (size_t i = 0; i < total; ++i)
        content[i] = static_cast<char>('A' + (i % 2));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    auto pool = std::make_shared<SyncPrefetchPool>();

    constexpr size_t window = 16;
    ReaderExecutor executor(source, objects, {}, window, /*min_bytes_for_seek=*/0);
    executor.setPrefetchPool(pool);

    /// First call: sync-read [0, 16). At the end of the call,
    /// `maybeTriggerPrefetch` submits P1 for [16, 32). The synchronous
    /// pool runs P1 inline: the source short-returns 14 bytes (EOF at 30),
    /// the worker sets `reached_eof = true`, and the future ends up
    /// holding the 14-byte rope.
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().offset, 0u);
    EXPECT_EQ(r1.range().size, window);

    /// Pre-fix: returns {} (EOF gate fires; prefetch dropped). Post-fix:
    /// the prefetched final bytes are served.
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().offset, window) << "prefetched final bytes lost";
    EXPECT_EQ(r2.range().size, total - window);

    /// Third call: no pending prefetch, `reached_eof` still set → real EOF.
    auto r3 = executor.readNextWindow();
    EXPECT_TRUE(r3.empty());
}

/// `ReadBufferFromOwnMemoryFile` (used by `BackupInMemory::readFile`, and
/// anywhere a fully-buffered in-memory blob is exposed as a file-shaped
/// buffer) pre-loads its content into `working_buffer` at construction;
/// its `nextImpl` returns false at first call. With the default
/// `supportsExternalBufferMode() = true` from `ReadBuffer`, the executor's
/// `readIntoBlock` would call `set(dest, chunk)` + `next()`, observe
/// `next() == false`, return 0 — and the executor would treat the source
/// as truncated (throw `CANNOT_READ_ALL_DATA` for known-size, latch
/// `reached_eof` for unknown-size), silently dropping the in-memory bytes.
///
/// `ReadBufferFromMemoryFileBase` overrides `supportsExternalBufferMode()`
/// to `false` so `readIntoBlock` falls back to `buf.read(dest, n)`, which
/// copies from `working_buffer`. This test exercises the full path:
/// `BufferSourceReader` whose factory hands back a `ReadBufferFromOwnMemoryFile`,
/// driven through `ReaderExecutor`, expects every byte through.
TEST(ReaderExecutor, MemoryBackedFileBufferIsReadFully)
{
    constexpr size_t total = 128;
    String content(total, 0);
    for (size_t i = 0; i < total; ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<BufferSourceReader>(
        [content](const StoredObject &) -> std::unique_ptr<ReadBufferFromFileBase>
        {
            return std::make_unique<ReadBufferFromOwnMemoryFile>("memfile", content);
        },
        "MemorySource");

    StoredObjects objects;
    objects.emplace_back("memfile", "", total);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/32, /*min_bytes_for_seek=*/0);

    /// Drive multiple windows to make sure subsequent reads also work,
    /// not just the first. Pre-fix the very first read would throw
    /// `CANNOT_READ_ALL_DATA` because `total_read == 0 < pr.size`.
    String collected;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        size_t base = collected.size();
        collected.resize(base + rope.range().size);
        rope.copyTo(collected.data() + base, rope.range());
    }
    EXPECT_EQ(collected, content) << "memory-backed file buffer must deliver all bytes through the executor";
}

/// End-to-end: drive the REAL `ReaderExecutor` over a REAL `DiskCacheProvider`
/// backed by a REAL `FileCache`, force real eviction between windows, and
/// assert the source connection is opened exactly once (no reset, no re-read).
///
/// The other pin tests use a MOCK cache (`EvictableSegmentMockCache`) or test
/// `DiskCacheHandle::pinSegmentAt` in isolation. This test closes the gap: it
/// proves the executor's in-flight pin keeps the partially-downloaded segment
/// non-releasable through an eviction flood that targets the REAL FileCache
/// LRU/reserve machinery. A prior bug — `pinSegmentAt` reading the holder that
/// `put` empties — was invisible to the mock but is caught here.
TEST(ReaderExecutor, RealDiskCacheSequentialEvictionKeepsConnection)
{
    DB::ServerUUID::setRandomForUnitTests();

    /// `FileCache::reserve` charges the per-query budget via
    /// `CurrentThread::getQueryId()`, so a real `ThreadStatus` + `QueryScope`
    /// (with a query context) must be in scope — same setup as
    /// `gtest_filecache.cpp`'s `DiskCacheHandlePinSurvivesEviction`.
    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_real_disk_cache");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_pin_it_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    /// Sized so the streamed object's single segment plus a little headroom
    /// fits, and a flood of other keys forces eviction of anything releasable.
    settings[DB::FileCacheSetting::max_size] = 24 * 1024;
    settings[DB::FileCacheSetting::max_elements] = 4;
    settings[DB::FileCacheSetting::max_file_segment_size] = 8 * 1024;
    /// Alignment == segment size keeps the streamed segment PARTIALLY_DOWNLOADED
    /// across windows (a smaller alignment would shrink it to DOWNLOADED on
    /// complete, removing the state the pin protects).
    settings[DB::FileCacheSetting::boundary_alignment] = 8 * 1024;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_pin_it", settings);
    cache->initialize();
    const auto & origin = DB::FileCache::getCommonOrigin();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DB::DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});

    /// One object that is a single 8 KiB-aligned segment, streamed in 2 KiB
    /// windows (4 windows) so the segment is PARTIALLY_DOWNLOADED between
    /// windows.
    String content(8000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"stream_obj", content}});
    StoredObjects objects;
    objects.emplace_back("stream_obj", "stream_obj", 8000);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(provider);

    auto limit = std::make_shared<SourceBufferLimit>(10);
    /// NOTE: no prefetch pool — keep reads synchronous so the flood between
    /// windows is deterministic.
    ReaderExecutor executor(source, objects, caches, /*window_size=*/2000, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Flood the cache with unrelated keys to force eviction of any releasable
    /// segment. The streamed segment must survive because it is pinned.
    auto flood = [&](int round)
    {
        for (int i = 0; i < 6; ++i)
        {
            auto key = DB::FileCacheKey::fromPath("flood_" + std::to_string(round) + "_" + std::to_string(i));
            auto h = cache->getOrSet(key, 0, 8 * 1024, 8 * 1024, DB::CreateFileSegmentSettings{}, 0, origin);
            for (auto & seg : *h)
            {
                if (seg->state() != DB::FileSegment::State::EMPTY)
                    continue;
                if (seg->getOrSetDownloader() != DB::FileSegment::getCallerId())
                    continue;
                std::string failure_reason;
                if (!seg->reserve(8 * 1024, 1000, failure_reason))
                {
                    seg->completePartAndResetDownloader();
                    continue;
                }
                /// `FileSegment::write` requires the key's on-disk directory.
                auto key_str = key.toString();
                auto subdir = fs::path(cache_path) / key_str.substr(0, 3) / key_str;
                if (!fs::exists(subdir))
                    fs::create_directories(subdir);
                std::string payload(8 * 1024, 'Z');
                seg->write(payload.data(), payload.size(), seg->getCurrentWriteOffset());
                seg->completePartAndResetDownloader();
            }
        }
    };

    auto & profile_events = DB::CurrentThread::getProfileEvents();
    const auto created_before = profile_events[ProfileEvents::LiveSourceBufferCreated].load();

    String result;
    int round = 0;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
        flood(round++);   // eviction pressure before the next window
    }

    EXPECT_EQ(result, content);
    /// The streamed segment stayed pinned through every flood, so the live
    /// connection was never reset and the left bytes were never re-read.
    EXPECT_EQ(executor.getSourceRequestsCount(), 1u);
    EXPECT_EQ(profile_events[ProfileEvents::LiveSourceBufferCreated].load() - created_before, 1);
}

