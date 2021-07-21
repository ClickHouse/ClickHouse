#pragma once

#include <map>

#include <common/shared_ptr_helper.h>

#include <Core/Defines.h>
#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>


namespace DB
{
/** Implements a table engine that is suitable for small chunks of the log.
  * It differs from StorageLog in the absence of mark files.
  */
class StorageTinyLog final : public shared_ptr_helper<StorageTinyLog>, public IStorage
{
    friend class TinyLogSource;
    friend class TinyLogBlockOutputStream;
    friend struct shared_ptr_helper<StorageTinyLog>;

public:
    String getName() const override { return "TinyLog"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    CheckResults checkData(const ASTPtr & /* query */, ContextPtr /* context */) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {DB::fullPath(disk, table_path)}; }
    bool supportsSubcolumns() const override { return true; }

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

protected:
    StorageTinyLog(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool attach,
        size_t max_compress_block_size_);

private:
    struct ColumnData
    {
        String data_file_path;
    };
    using Files = std::map<String, ColumnData>; /// file name -> column data

    DiskPtr disk;
    String table_path;

    size_t max_compress_block_size;

    Files files;

    FileChecker file_checker;
    std::shared_timed_mutex rwlock;

    Poco::Logger * log;

    void addFiles(const NameAndTypePair & column);
};

}
