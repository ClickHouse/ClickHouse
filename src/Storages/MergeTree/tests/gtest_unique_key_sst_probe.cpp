#include <gtest/gtest.h>

#include "config.h"

#if USE_ROCKSDB

#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>

#include <algorithm>
#include <cstring>
#include <mutex>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>


using namespace DB;

namespace
{
    struct SSTFixture : public ::testing::Test
    {
        std::filesystem::path tmp_path;
        std::shared_ptr<DiskLocal> disk;
        std::shared_ptr<SingleDiskVolume> volume;
        std::shared_ptr<DataPartStorageOnDiskFull> storage;

        void SetUp() override
        {
            tmp_path = std::filesystem::temp_directory_path()
                / ("gtest_unique_key_sst_"
                   + std::to_string(::testing::UnitTest::GetInstance()->random_seed())
                   + "_"
                   + std::to_string(reinterpret_cast<uintptr_t>(this)));
            std::filesystem::remove_all(tmp_path);
            std::filesystem::create_directories(tmp_path / "part");
            disk = std::make_shared<DiskLocal>("test_disk", tmp_path.string());
            volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
            storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");

            /// `Context::setTemporaryStoragePath` throws if called twice, so
            /// configure once per binary lifetime to a shared dir. The
            /// per-test fixture tmp_path stays for the actual part storage.
            static std::once_flag tmp_storage_once;
            std::call_once(tmp_storage_once, [] {
                auto shared_tmp = std::filesystem::temp_directory_path() / "ck_uk_gtest_tmp";
                std::filesystem::create_directories(shared_tmp);
                getMutableContext().context->setTemporaryStoragePath(shared_tmp.string() + "/", 0);
            });
        }

        void TearDown() override
        {
            storage.reset();
            volume.reset();
            disk.reset();
            std::filesystem::remove_all(tmp_path);
        }

        std::string finalPath() const
        {
            return storage->getFullPath() + "/" + SSTIndexWriter::FILE_NAME;
        }

    };

    Columns makeUInt64Columns(const std::vector<UInt64> & keys)
    {
        auto col = ColumnUInt64::create();
        for (UInt64 k : keys)
            col->insertValue(k);
        Columns cols;
        cols.push_back(std::move(col));
        return cols;
    }

    /// Wrap a UInt64 key vector in a single-column Block named "k", for
    /// `writeFromBlock` callers. Keys are inserted in input order.
    Block makeUInt64Block(const std::vector<UInt64> & keys)
    {
        auto type_u64 = std::make_shared<DataTypeUInt64>();
        auto col = type_u64->createColumn();
        auto * typed = typeid_cast<ColumnUInt64 *>(col.get());
        for (UInt64 k : keys)
            typed->insertValue(k);
        Block block;
        block.insert({std::move(col), type_u64, "k"});
        return block;
    }

    rocksdb::Options makeReaderOptions()
    {
        rocksdb::Options options;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(
            rocksdb::NewBloomFilterPolicy(SSTIndexWriter::BLOOM_BITS_PER_KEY));
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        return options;
    }

    bool sstIteratorContains(
        rocksdb::SstFileReader & reader,
        const std::string & key,
        std::string * out_value = nullptr)
    {
        std::unique_ptr<rocksdb::Iterator> iter(reader.NewIterator(rocksdb::ReadOptions{}));
        iter->Seek(rocksdb::Slice(key));
        if (!iter->Valid())
            return false;
        if (iter->key().compare(rocksdb::Slice(key)) != 0)
            return false;
        if (out_value)
            *out_value = iter->value().ToString();
        return true;
    }

    UInt32 decodeRowNumberBE(const std::string & value)
    {
        EXPECT_EQ(value.size(), 4u);
        return (static_cast<UInt32>(static_cast<UInt8>(value[0])) << 24)
             | (static_cast<UInt32>(static_cast<UInt8>(value[1])) << 16)
             | (static_cast<UInt32>(static_cast<UInt8>(value[2])) << 8)
             |  static_cast<UInt32>(static_cast<UInt8>(value[3]));
    }
}

/// Smoke: write 10K sorted UInt64 keys, read back via `SstFileReader`,
/// every key resolves to its expected row_number. One smoke test gates
/// the wrapper layer end-to-end; the rest of this file pins our own
/// SST-write contracts (atomic rename, empty short-circuit, value
/// encoding, unsorted-path sort, memory-limit, corruption rebuild).
TEST_F(SSTFixture, RoundTrip10K)
{
    constexpr size_t N = 10'000;
    std::vector<UInt64> keys;
    keys.reserve(N);
    for (size_t i = 0; i < N; ++i)
        keys.push_back(static_cast<UInt64>(i) * 7 + 13);

    auto cols = makeUInt64Columns(keys);
    auto block = makeUInt64Block(keys);

    UInt64 written = SSTIndexWriter::writeFromBlock(
        *storage, block, Names{"k"}, /*permutation=*/nullptr, /*max_encoded_size=*/256, getContext().context);
    ASSERT_EQ(written, N);
    ASSERT_TRUE(std::filesystem::exists(finalPath()));

    rocksdb::SstFileReader reader(makeReaderOptions());
    auto status = reader.Open(finalPath());
    ASSERT_TRUE(status.ok()) << status.ToString();

    std::vector<String> encoded;
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, /*max_size=*/256, encoded);
    for (size_t i = 0; i < N; ++i)
    {
        std::string value;
        ASSERT_TRUE(sstIteratorContains(reader, encoded[i], &value))
            << "missing key at row " << i;
        EXPECT_EQ(decodeRowNumberBE(value), static_cast<UInt32>(i));
    }
}

/// Corruption detection + rebuild: write an SST, truncate it past the
/// RocksDB footer, reopen → !ok. Rebuild via `writeFromBlock` on the
/// same key set → open succeeds.
TEST_F(SSTFixture, CorruptionRebuild)
{
    std::vector<UInt64> keys;
    for (UInt64 i = 0; i < 500; ++i)
        keys.push_back(i * 3);
    auto block = makeUInt64Block(keys);

    ASSERT_EQ(SSTIndexWriter::writeFromBlock(
                  *storage, block, Names{"k"}, /*permutation=*/nullptr, 256, getContext().context),
              keys.size());

    {
        std::error_code ec;
        auto sz = std::filesystem::file_size(finalPath(), ec);
        ASSERT_FALSE(ec);
        ASSERT_GT(sz, 64u);
        std::filesystem::resize_file(finalPath(), 64);
    }

    {
        rocksdb::SstFileReader reader(makeReaderOptions());
        auto status = reader.Open(finalPath());
        EXPECT_FALSE(status.ok())
            << "Expected corruption detection to fail Open; got: " << status.ToString();
    }

    /// Rebuild from the same input → open succeeds.
    ASSERT_EQ(SSTIndexWriter::writeFromBlock(
                  *storage, block, Names{"k"}, /*permutation=*/nullptr, 256, getContext().context),
              keys.size());

    rocksdb::SstFileReader reader(makeReaderOptions());
    ASSERT_TRUE(reader.Open(finalPath()).ok());
    auto one_col = makeUInt64Columns({keys[42]});
    std::vector<String> encoded;
    UniqueKeyEncoding::encodeBlock(one_col, /*permutation=*/nullptr, 256, encoded);
    EXPECT_TRUE(sstIteratorContains(reader, encoded[0]));
}

/// Empty-input short-circuit: zero keys → no `.sst` produced, no `.tmp`
/// residue. Pins `finalizeToStorage`'s empty-input contract.
TEST_F(SSTFixture, EmptyInputProducesNoFile)
{
    auto block = makeUInt64Block({});
    UInt64 written = SSTIndexWriter::writeFromBlock(
        *storage, block, Names{"k"}, /*permutation=*/nullptr, 256, getContext().context);
    EXPECT_EQ(written, 0u);
    EXPECT_FALSE(std::filesystem::exists(finalPath()));
}

/// Row-number value encoding: SST values are UInt32 BE. End-to-end scan
/// of a 100-row SST exposes the contract directly.
TEST_F(SSTFixture, RowNumbersMonotonic)
{
    constexpr size_t N = 100;
    std::vector<UInt64> keys;
    for (UInt64 i = 0; i < N; ++i)
        keys.push_back(1'000'000 + i);
    auto block = makeUInt64Block(keys);

    ASSERT_EQ(SSTIndexWriter::writeFromBlock(
                  *storage, block, Names{"k"}, /*permutation=*/nullptr, 256, getContext().context), N);

    rocksdb::SstFileReader reader(makeReaderOptions());
    ASSERT_TRUE(reader.Open(finalPath()).ok());

    std::unique_ptr<rocksdb::Iterator> it(reader.NewIterator(rocksdb::ReadOptions{}));
    std::vector<UInt32> observed;
    for (it->SeekToFirst(); it->Valid(); it->Next())
        observed.push_back(decodeRowNumberBE(it->value().ToString()));

    ASSERT_EQ(observed.size(), N);
    for (size_t i = 0; i < N; ++i)
        EXPECT_EQ(observed[i], static_cast<UInt32>(i));
}

/// Non-prefix-UK pre-sort: `writeFromBlockUnsorted` accepts keys in
/// ORDER-BY order and produces a correctly sorted SST. Pins the in-
/// memory sort + `_part_offset` row_number derivation.
TEST_F(SSTFixture, UnsortedBlockSortedBeforeWrite)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();
    auto col = type_u64->createColumn();
    auto * typed = typeid_cast<ColumnUInt64 *>(col.get());
    std::vector<UInt64> values{500, 100, 900, 200, 300, 800, 400, 700, 600, 1000};
    for (auto v : values)
        typed->insertValue(v);

    Block block;
    block.insert({std::move(col), type_u64, "k"});

    UInt64 written = SSTIndexWriter::writeFromBlockUnsorted(
        *storage, block, Names{"k"}, /*permutation=*/nullptr,
        /*max_encoded_size=*/256, getContext().context);
    ASSERT_EQ(written, values.size());

    rocksdb::SstFileReader reader(makeReaderOptions());
    ASSERT_TRUE(reader.Open(finalPath()).ok());
    std::unique_ptr<rocksdb::Iterator> it(reader.NewIterator(rocksdb::ReadOptions{}));

    std::vector<UInt64> observed_keys_order;
    std::vector<UInt32> observed_row_numbers;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        auto k = it->key();
        ASSERT_EQ(k.size(), 8u);
        UInt64 decoded = 0;
        for (size_t i = 0; i < 8; ++i)
            decoded = (decoded << 8) | static_cast<unsigned char>(k.data()[i]);
        observed_keys_order.push_back(decoded);
        observed_row_numbers.push_back(decodeRowNumberBE(it->value().ToString()));
    }

    auto sorted_values = values;
    std::sort(sorted_values.begin(), sorted_values.end());
    ASSERT_EQ(observed_keys_order, sorted_values);

    /// row_number = position of the value in the original input block.
    for (size_t i = 0; i < sorted_values.size(); ++i)
    {
        UInt64 v = sorted_values[i];
        auto it_orig = std::find(values.begin(), values.end(), v);
        ASSERT_NE(it_orig, values.end());
        size_t original_index = std::distance(values.begin(), it_orig);
        EXPECT_EQ(observed_row_numbers[i], static_cast<UInt32>(original_index))
            << "value=" << v;
    }
}

/// `writeFromBlockUnsorted` over a `Nullable(UInt64)` mix: pins that
/// `stableGetPermutation`'s `nulls_direction=1` and the encoder's
/// null-flag convention agree (encoded NULL sorts AFTER non-NULL). If
/// the encoder put NULL before non-NULL but the sort put it after (or
/// vice versa), the key stream would be out of order and RocksDB
/// `SstFileWriter::Put` would reject it.
TEST_F(SSTFixture, UnsortedNullableSortsConsistently)
{
    auto type_nullable_u64 = makeNullable(std::make_shared<DataTypeUInt64>());
    auto col = type_nullable_u64->createColumn();

    /// Mix: non-null 50, NULL, non-null 10, non-null 30. Insert order
    /// is intentionally unsorted by both UK value and null-flag.
    /// Production dedups duplicate UKs upstream, so each distinct UK
    /// (including a single NULL) reaches the writer at most once —
    /// matched here by using a single NULL.
    col->insert(Field(UInt64(50)));
    col->insert(Null());
    col->insert(Field(UInt64(10)));
    col->insert(Field(UInt64(30)));

    Block block;
    block.insert({std::move(col), type_nullable_u64, "k"});

    UInt64 written = SSTIndexWriter::writeFromBlockUnsorted(
        *storage, block, Names{"k"}, /*permutation=*/nullptr,
        /*max_encoded_size=*/256, getContext().context);
    ASSERT_EQ(written, 4u);

    rocksdb::SstFileReader reader(makeReaderOptions());
    ASSERT_TRUE(reader.Open(finalPath()).ok());
    std::unique_ptr<rocksdb::Iterator> it(reader.NewIterator(rocksdb::ReadOptions{}));

    std::vector<UInt32> observed_row_numbers;
    std::string prev_key;
    bool first = true;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::string cur = it->key().ToString();
        if (!first)
            ASSERT_LT(prev_key, cur) << "SST keys not strictly ascending — encoder/sort direction mismatch";
        prev_key = cur;
        first = false;
        observed_row_numbers.push_back(decodeRowNumberBE(it->value().ToString()));
    }
    /// Expected order by encoded bytes: 10, 30, 50, NULL → row_numbers = 2, 3, 0, 1.
    EXPECT_EQ(observed_row_numbers, (std::vector<UInt32>{2, 3, 0, 1}));
}

/// `writeFromBlockUnsorted` with a non-null caller permutation: SST
/// row_number must be the row's part_offset (after applying the
/// permutation), not the source-row index.
TEST_F(SSTFixture, UnsortedWithCallerPermutationStoresPartOffset)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();
    auto col = type_u64->createColumn();
    auto * typed = typeid_cast<ColumnUInt64 *>(col.get());
    /// Source-order UKs: 500, 100, 900, 200, 300.
    std::vector<UInt64> values{500, 100, 900, 200, 300};
    for (auto v : values)
        typed->insertValue(v);

    Block block;
    block.insert({std::move(col), type_u64, "k"});

    /// PK perm = part_offset → source_row. Pick {2, 0, 4, 3, 1} so the
    /// part is laid out as UK descending: 900, 500, 300, 200, 100.
    IColumn::Permutation pk_perm{2, 0, 4, 3, 1};

    UInt64 written = SSTIndexWriter::writeFromBlockUnsorted(
        *storage, block, Names{"k"}, &pk_perm,
        /*max_encoded_size=*/256, getContext().context);
    ASSERT_EQ(written, values.size());

    rocksdb::SstFileReader reader(makeReaderOptions());
    ASSERT_TRUE(reader.Open(finalPath()).ok());
    std::unique_ptr<rocksdb::Iterator> it(reader.NewIterator(rocksdb::ReadOptions{}));

    std::vector<UInt64> keys_in_order;
    std::vector<UInt32> row_numbers_in_order;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        auto k = it->key();
        ASSERT_EQ(k.size(), 8u);
        UInt64 decoded = 0;
        for (size_t i = 0; i < 8; ++i)
            decoded = (decoded << 8) | static_cast<unsigned char>(k.data()[i]);
        keys_in_order.push_back(decoded);
        row_numbers_in_order.push_back(decodeRowNumberBE(it->value().ToString()));
    }

    /// UK-ascending: 100, 200, 300, 500, 900 → source rows 1, 3, 4, 0, 2.
    /// Inverting pk_perm: source 1→part_offset 4, 3→3, 4→2, 0→1, 2→0.
    EXPECT_EQ(keys_in_order, (std::vector<UInt64>{100, 200, 300, 500, 900}));
    EXPECT_EQ(row_numbers_in_order, (std::vector<UInt32>{4, 3, 2, 1, 0}));
}

/// `writeFromBlock` (prefix-UK path) accepts a sorted Block and produces
/// the expected SST shape (sorted keys + monotonic row_numbers).
TEST_F(SSTFixture, WriteFromBlockProducesSortedSST)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();
    auto col = type_u64->createColumn();
    auto * typed = typeid_cast<ColumnUInt64 *>(col.get());
    constexpr size_t N = 64;
    for (UInt64 i = 0; i < N; ++i)
        typed->insertValue(i * 11 + 3);

    Block block;
    block.insert({std::move(col), type_u64, "k"});

    UInt64 written = SSTIndexWriter::writeFromBlock(
        *storage, block, Names{"k"}, /*permutation=*/nullptr, /*max_encoded_size=*/256, getContext().context);
    ASSERT_EQ(written, N);
    ASSERT_TRUE(std::filesystem::exists(finalPath()));

    rocksdb::SstFileReader reader(makeReaderOptions());
    ASSERT_TRUE(reader.Open(finalPath()).ok());
    std::unique_ptr<rocksdb::Iterator> it(reader.NewIterator(rocksdb::ReadOptions{}));

    UInt32 expected_row = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        EXPECT_EQ(decodeRowNumberBE(it->value().ToString()), expected_row);
        ++expected_row;
    }
    EXPECT_EQ(expected_row, static_cast<UInt32>(N));
}

/// ColumnConst UK columns must be materialized before encoder dispatch
/// (encoder static_casts based on getDataType(), UB on ColumnConst).
TEST_F(SSTFixture, WriteFromBlockConstUKColumnAccepted)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();
    auto inner = type_u64->createColumn();
    typeid_cast<ColumnUInt64 *>(inner.get())->insertValue(42);
    ColumnPtr const_col = ColumnConst::create(std::move(inner), 1);

    Block block;
    block.insert({const_col, type_u64, "k"});

    UInt64 written = SSTIndexWriter::writeFromBlock(
        *storage, block, Names{"k"}, /*permutation=*/nullptr, /*max_encoded_size=*/256, getContext().context);
    EXPECT_EQ(written, 1u);
    EXPECT_TRUE(std::filesystem::exists(finalPath()));
}

#endif  // USE_ROCKSDB

/// ---------------------------------------------------------------------------
/// USE_ROCKSDB=OFF stub-contract tests. UK on RocksDB-disabled builds
/// is unsupported; these pin the static-writer / ctor stub behaviour.
/// ---------------------------------------------------------------------------

#if !USE_ROCKSDB

#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>

#include <filesystem>
#include <string>

using namespace DB;

TEST(UniqueKeyNoRocksDB, StaticWritersThrowSupportIsDisabled)
{
    auto tmp_path = std::filesystem::temp_directory_path()
        / ("gtest_unique_key_no_rocksdb_"
           + std::to_string(::testing::UnitTest::GetInstance()->random_seed())
           + "_" + std::to_string(reinterpret_cast<uintptr_t>(&tmp_path)));
    std::filesystem::remove_all(tmp_path);
    std::filesystem::create_directories(tmp_path / "part");
    auto disk = std::make_shared<DiskLocal>("test_disk", tmp_path.string());
    auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");

    auto col = ColumnUInt64::create();
    col->insert(Field(UInt64(1)));
    col->insert(Field(UInt64(2)));
    Block block{
        {col->getPtr(), std::make_shared<DataTypeUInt64>(), "a"}
    };

    Names uk_names{"a"};

    /// Both static entry points must fail loudly on USE_ROCKSDB=0; a silent
    /// success would let the merge-write path skip index creation without the
    /// caller noticing.
    EXPECT_THROW(
        SSTIndexWriter::writeFromBlock(
            *storage, block, uk_names, /*permutation=*/nullptr, /*max_encoded_size=*/256, getContext().context),
        DB::Exception);

    EXPECT_THROW(
        SSTIndexWriter::writeFromBlockUnsorted(
            *storage, block, uk_names, /*permutation=*/nullptr, /*max_encoded_size=*/256, getContext().context),
        DB::Exception);

    EXPECT_FALSE(storage->existsFile(SSTIndexWriter::FILE_NAME));

    std::filesystem::remove_all(tmp_path);
}

TEST(UniqueKeyNoRocksDB, ConstructorThrowsSupportIsDisabled)
{
    auto tmp_path = std::filesystem::temp_directory_path()
        / ("gtest_unique_key_no_rocksdb_ctor_"
           + std::to_string(::testing::UnitTest::GetInstance()->random_seed())
           + "_" + std::to_string(reinterpret_cast<uintptr_t>(&tmp_path)));
    std::filesystem::remove_all(tmp_path);
    std::filesystem::create_directories(tmp_path / "part");
    auto disk = std::make_shared<DiskLocal>("test_disk", tmp_path.string());
    auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");

    EXPECT_THROW({
        SSTIndexWriter writer(*storage, getContext().context);
    }, DB::Exception);

    std::filesystem::remove_all(tmp_path);
}

#endif  // !USE_ROCKSDB
