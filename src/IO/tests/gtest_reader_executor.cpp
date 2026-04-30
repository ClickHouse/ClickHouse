#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/Rope.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <cstring>
#include <unordered_map>

using namespace DB;

namespace
{

/// In-memory source reader for testing.
class MemorySourceReader : public ISourceReader
{
public:
    explicit MemorySourceReader(std::unordered_map<String, String> data_)
        : data(std::move(data_)) {}

    size_t read(const StoredObject & object, size_t offset, size_t size, char * buffer) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return 0;
        const auto & content = it->second;
        if (offset >= content.size())
            return 0;
        size_t to_read = std::min(size, content.size() - offset);
        std::memcpy(buffer, content.data() + offset, to_read);
        return to_read;
    }

    String name() const override { return "MemorySourceReader"; }

private:
    std::unordered_map<String, String> data;
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
        Range range,
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
            Range block_range{block_start, block_end - block_start};

            if (storage.count(b))
                result.hit_ranges.push_back(block_range);
            else
                result.miss_ranges.push_back(block_range);
        }
    }

    CacheLookupResult status() const override { return result; }

    Rope get(Range range) override
    {
        size_t block = range.offset / block_size;
        const auto & data = storage.at(block);
        auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
        std::memcpy(buf->data(), data.data(), data.size());

        Rope rope;
        rope.append(RopeNode{buf, 0, data.size(), block * block_size});
        return rope.slice(range);
    }

    bool put(Range range, Rope data) override
    {
        size_t block = range.offset / block_size;
        if (storage.count(block))
            return false;

        String content;
        for (const auto & node : data.getNodes())
            content.append(node.data(), node.size);
        storage[block] = std::move(content);
        return true;
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

    std::unique_ptr<ICacheHandle> lookup(CacheKey, Range range) override
    {
        return std::make_unique<MockCacheHandle>(range, storage, block_size);
    }

    String name() const override { return "MockCache"; }

    bool hasBlock(size_t block_index) const { return storage.count(block_index) > 0; }

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
