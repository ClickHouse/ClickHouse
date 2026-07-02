#pragma once

/// Minimal `StorageMergeTree` harness shared by the UNIQUE KEY storage gtests.
/// `LoadingStrictnessLevel::ATTACH` skips the sanity / DDL checks these
/// part-level primitives don't exercise; the harness is enough to drive
/// `MergeTreeData::createEmptyPart` / `createMarkerPart` and the read-side
/// snapshot filter over a real (in-memory) `StorageMergeTree`.
///
/// One column `a` UInt64. Callers parameterize the three points that differ
/// between tests: whether a UNIQUE KEY is defined on `a`, the table name /
/// relative path, and the order-by (a non-UK marker table keeps an empty
/// `tuple()` order-by with a null primary-key `definition_ast`; a UK table
/// orders by `tuple(a)`).
///
/// Heavy includes (Context.h, MergeTreeSettings.h, StorageMergeTree.h, …) live
/// in the .cpp so this header stays light for the style gate.

#include <Interpreters/Context_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <memory>
#include <string>

namespace DB
{
class StorageMergeTree;
}

namespace DB::UniqueKeyTxn::tests
{

struct UKStorageHarnessOptions
{
    bool with_unique_key = false;
    std::string table_name;
    std::string relative_path;
};

struct UKStorageHarness
{
    ContextMutablePtr context;
    std::shared_ptr<StorageMergeTree> storage;
    StorageInMemoryMetadata metadata;

    explicit UKStorageHarness(const UKStorageHarnessOptions & opts);
    ~UKStorageHarness();
};

}
