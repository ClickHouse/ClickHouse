#pragma once

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{
class Block;
class StorageMergeTree;
using StorageMergeTreePtr = std::shared_ptr<StorageMergeTree>;
}

namespace MergeTreeTestHarness
{

/// Create a `StorageMergeTree` backed by an in-process local disk that:
///   - has a single `Array(Float32)` vector column named `vec_column_name` with dim `dim`;
///   - has a UInt32 partition key column named `partition_key_column`;
///   - uses `ORDER BY tuple()` (no sorting key);
///   - enables `_block_number` and `_block_offset` virtual columns (persisted);
///
/// The returned storage is started up and must be shut down via `flushAndShutdown` before it
/// goes out of scope — the caller is responsible for this to keep the ownership semantics
/// obvious in each test.
///
/// `disk_root` is the root of the local disk. `base_relative_path` is the relative sub-path
/// inside the disk that holds this table's data (e.g. `store/ann_test_xxx/`).
struct TestStorage
{
    DB::DiskPtr disk;
    DB::StorageMergeTreePtr storage;
    DB::StorageID storage_id{"test_db", "test_table"};
    std::string vec_column_name;
    std::string partition_key_column;
    size_t dim;
};

TestStorage createStorageWithVectorColumn(
    const std::string & disk_root,
    const std::string & base_relative_path,
    const std::string & vec_column_name,
    size_t dim,
    const std::string & partition_key_column = "pk");

/// Insert one block of `num_rows` rows into the storage. The vector column is filled with the
/// values returned by `vec_generator(row_idx, col_idx)`; the partition key is filled by
/// `pk_generator(row_idx)`. A fresh block creates a fresh data part.
void insertVectorBlock(
    DB::StorageMergeTreePtr storage,
    const TestStorage & setup,
    size_t num_rows,
    uint32_t partition_value,
    const std::vector<std::vector<float>> & vectors);

}
