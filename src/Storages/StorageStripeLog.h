#pragma once

#include <map>
#include <shared_mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/Defines.h>
#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>


namespace DB
{
/** Implements a table engine that is suitable for small chunks of the log.
  * In doing so, stores all the columns in a single Native file, with a nearby index.
  */
class StorageStripeLog final : public ext::shared_ptr_helper<StorageStripeLog>, public IStorage
{
    friend class StripeLogSource;
    friend class StripeLogBlockOutputStream;
    friend struct ext::shared_ptr_helper<StorageStripeLog>;

public:
    String getName() const override { return "StripeLog"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    CheckResults checkData(const ASTPtr & /* query */, const Context & /* context */) override;

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {DB::fullPath(disk, table_path)}; }

    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder&) override;

protected:
    StorageStripeLog(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
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

    FileChecker file_checker;
    mutable std::shared_mutex rwlock;

    Poco::Logger * log;
};

}
