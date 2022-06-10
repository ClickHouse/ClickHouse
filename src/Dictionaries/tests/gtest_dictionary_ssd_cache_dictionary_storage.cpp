#if defined(OS_LINUX) || defined(OS_FREEBSD)

#include <gtest/gtest.h>

#include <Dictionaries/SSDCacheDictionaryStorage.h>

using namespace DB;

TEST(SSDCacheDictionaryStorage, SSDCacheBlockWithSSDCacheSimpleKey)
{
    static constexpr size_t block_data_size = 4096;
    std::unique_ptr<char[]> block_data(new char[block_data_size]);
    memset(block_data.get(), 0, block_data_size);

    {
        memset(block_data.get(), 0, block_data_size);

        SSDCacheBlock block(block_data_size);

        block.reset(block_data.get());

        std::unique_ptr<char[]> data_to_insert(new char[4000]);
        memset(data_to_insert.get(), 1, 4000);

        SSDCacheSimpleKey key(0, 200, data_to_insert.get());
        ASSERT_EQ(block.getKeysSize(), 0);

        bool write_result = false;
        size_t offset_in_block = 0;

        ASSERT_TRUE(block.enoughtPlaceToWriteKey(key));
        write_result = block.writeKey(key, offset_in_block);
        ASSERT_TRUE(write_result);
        ASSERT_EQ(block.getKeysSize(), 1);

        key.key = 1;
        ASSERT_TRUE(block.enoughtPlaceToWriteKey(key));
        write_result = block.writeKey(key, offset_in_block);
        ASSERT_TRUE(write_result);
        ASSERT_EQ(block.getKeysSize(), 2);

        key.key = 2;
        ASSERT_TRUE(block.enoughtPlaceToWriteKey(key));
        write_result = block.writeKey(key, offset_in_block);
        ASSERT_TRUE(write_result);
        ASSERT_EQ(block.getKeysSize(), 3);

        PaddedPODArray<UInt64> expected = {0,1,2};
        PaddedPODArray<UInt64> actual;
        block.readSimpleKeys(actual);
        ASSERT_EQ(actual, expected);
    }
    {
        memset(block_data.get(), 0, block_data_size);
        SSDCacheBlock block(block_data_size);
        block.reset(block_data.get());

        static constexpr size_t block_header_size = SSDCacheBlock::block_header_size;
        static constexpr size_t key_metadata_size = sizeof(size_t) * 2;

        std::unique_ptr<char[]> data_to_insert(new char[4080]);
        memset(data_to_insert.get(), 1, 4000);

        SSDCacheSimpleKey key {0, 4064, data_to_insert.get()};

        ASSERT_TRUE(SSDCacheBlock::canBeWrittenInEmptyBlock(key, block_data_size));
        key.size = 4065;
        ASSERT_FALSE(SSDCacheBlock::canBeWrittenInEmptyBlock(key, block_data_size));
        key.size = 4064;

        size_t offset_in_block;
        ASSERT_TRUE(block.enoughtPlaceToWriteKey(key));
        ASSERT_TRUE(block.writeKey(key, offset_in_block));
        ASSERT_EQ(offset_in_block, block_header_size + key_metadata_size);

        ASSERT_FALSE(block.enoughtPlaceToWriteKey({1, 4065, data_to_insert.get()}));
        offset_in_block = 0;
        ASSERT_FALSE(block.writeKey({1, 4065, data_to_insert.get()}, offset_in_block));
        ASSERT_EQ(offset_in_block, 0);

        PaddedPODArray<UInt64> expected = {0};
        PaddedPODArray<UInt64> actual;
        block.readSimpleKeys(actual);
        ASSERT_EQ(actual, expected);
    }
    {
        memset(block_data.get(), 0, block_data_size);
        SSDCacheBlock block(block_data_size);
        block.reset(block_data.get());

        PaddedPODArray<UInt64> expected = {};
        PaddedPODArray<UInt64> actual;
        block.readSimpleKeys(actual);
        ASSERT_EQ(actual, expected);
    }
    {
        memset(block_data.get(), 0, block_data_size);
        SSDCacheBlock block(block_data_size);
        block.reset(block_data.get());

        std::unique_ptr<char[]> data_to_insert(new char[4000]);
        memset(data_to_insert.get(), 1, 4000);
        size_t offset_in_block;
        SSDCacheSimpleKey key {0, 200, data_to_insert.get()};
        block.writeKey({1, 200, data_to_insert.get()}, offset_in_block);
        ASSERT_EQ(block.getKeysSize(), 1);

        ASSERT_FALSE(block.checkCheckSum());
        block.writeCheckSum();
        ASSERT_TRUE(block.checkCheckSum());

        SSDCacheBlock other_block(block_data_size);
        other_block.reset(block_data.get());
        bool write_result = other_block.writeKey({2, 200, data_to_insert.get()}, offset_in_block);
        ASSERT_TRUE(write_result);

        ASSERT_FALSE(block.checkCheckSum());
        block.writeCheckSum();
        ASSERT_TRUE(block.checkCheckSum());
    }
}

TEST(SSDCacheDictionaryStorage, SSDCacheBlockWithSSDCachComplexKey)
{
    static constexpr size_t block_data_size = 4096;
    std::unique_ptr<char[]> block_data(new char[block_data_size]);
    memset(block_data.get(), 0, block_data_size);

    {
        memset(block_data.get(), 0, block_data_size);

        SSDCacheBlock block(block_data_size);

        block.reset(block_data.get());

        std::unique_ptr<char[]> data_to_insert(new char[4000]);
        memset(data_to_insert.get(), 1, 4000);

        String key = "0";

        SSDCacheComplexKey ssd_cache_key(key, 200, data_to_insert.get());
        ASSERT_EQ(block.getKeysSize(), 0);

        bool write_result = false;
        size_t offset_in_block = 0;

        ASSERT_TRUE(block.enoughtPlaceToWriteKey(ssd_cache_key));
        write_result = block.writeKey(ssd_cache_key, offset_in_block);
        ASSERT_TRUE(write_result);
        ASSERT_EQ(block.getKeysSize(), 1);

        ssd_cache_key.key = "1";
        ASSERT_TRUE(block.enoughtPlaceToWriteKey(ssd_cache_key));
        write_result = block.writeKey(ssd_cache_key, offset_in_block);
        ASSERT_TRUE(write_result);
        ASSERT_EQ(block.getKeysSize(), 2);

        ssd_cache_key.key = "2";
        ASSERT_TRUE(block.enoughtPlaceToWriteKey(ssd_cache_key));
        write_result = block.writeKey(ssd_cache_key, offset_in_block);
        ASSERT_TRUE(write_result);
        ASSERT_EQ(block.getKeysSize(), 3);

        PaddedPODArray<StringRef> expected = {"0","1","2"};
        PaddedPODArray<StringRef> actual;

        block.readComplexKeys(actual);
        ASSERT_EQ(actual, expected);
    }
    {
        memset(block_data.get(), 0, block_data_size);
        SSDCacheBlock block(block_data_size);
        block.reset(block_data.get());

        static constexpr size_t block_header_size = SSDCacheBlock::block_header_size;
        static constexpr size_t key_metadata_size = sizeof(size_t) * 2;

        std::unique_ptr<char[]> data_to_insert(new char[4080]);
        memset(data_to_insert.get(), 1, 4000);

        SSDCacheComplexKey key {"", 4064, data_to_insert.get()};

        ASSERT_TRUE(SSDCacheBlock::canBeWrittenInEmptyBlock(key, block_data_size));
        key.size = 4065;
        ASSERT_FALSE(SSDCacheBlock::canBeWrittenInEmptyBlock(key, block_data_size));
        key.size = 4064;

        size_t offset_in_block;
        ASSERT_TRUE(block.enoughtPlaceToWriteKey(key));
        ASSERT_TRUE(block.writeKey(key, offset_in_block));
        ASSERT_EQ(offset_in_block, block_header_size + key_metadata_size);

        ASSERT_FALSE(block.enoughtPlaceToWriteKey({1, 4065, data_to_insert.get()}));
        offset_in_block = 0;
        ASSERT_FALSE(block.writeKey({1, 4065, data_to_insert.get()}, offset_in_block));
        ASSERT_EQ(offset_in_block, 0);

        PaddedPODArray<UInt64> expected = {0};
        PaddedPODArray<UInt64> actual;
        block.readSimpleKeys(actual);
        ASSERT_EQ(actual, expected);
    }
    {
        memset(block_data.get(), 0, block_data_size);
        SSDCacheBlock block(block_data_size);
        block.reset(block_data.get());

        PaddedPODArray<StringRef> expected = {};
        PaddedPODArray<StringRef> actual;
        block.readComplexKeys(actual);
        ASSERT_EQ(actual, expected);
    }
    {
        memset(block_data.get(), 0, block_data_size);
        SSDCacheBlock block(block_data_size);
        block.reset(block_data.get());

        std::unique_ptr<char[]> data_to_insert(new char[4000]);
        memset(data_to_insert.get(), 1, 4000);
        size_t offset_in_block;
        SSDCacheComplexKey key {"0", 200, data_to_insert.get()};
        block.writeKey({1, 200, data_to_insert.get()}, offset_in_block);
        ASSERT_EQ(block.getKeysSize(), 1);

        ASSERT_FALSE(block.checkCheckSum());
        block.writeCheckSum();
        ASSERT_TRUE(block.checkCheckSum());

        SSDCacheBlock other_block(block_data_size);
        other_block.reset(block_data.get());
        bool write_result = other_block.writeKey({2, 200, data_to_insert.get()}, offset_in_block);
        ASSERT_TRUE(write_result);

        ASSERT_FALSE(block.checkCheckSum());
        block.writeCheckSum();
        ASSERT_TRUE(block.checkCheckSum());
    }
}

#endif
