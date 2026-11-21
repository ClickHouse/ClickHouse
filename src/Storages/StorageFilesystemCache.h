#pragma once

#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Storages/IStorage.h>


namespace DB
{

/** Storage for the `filesystemCache` table function.
  * Returns the file segments of the filesystem cache `cache_name` selected by their priority,
  * see `IFileCachePriority::observeFilesystemCache`.
  */
class StorageFilesystemCache final : public IStorage
{
public:
    StorageFilesystemCache(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        String cache_name_,
        UInt64 query_max_elements_,
        UInt64 query_max_total_size_,
        IFileCachePriority::IterationOrder query_order_);

    std::string getName() const override { return "FilesystemCache"; }

    static ColumnsDescription getColumnsDescription();

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool storesDataOnDisk() const override { return false; }

private:
    const String cache_name;
    const UInt64 query_max_elements;
    const UInt64 query_max_total_size;
    const IFileCachePriority::IterationOrder query_order;
};

}
