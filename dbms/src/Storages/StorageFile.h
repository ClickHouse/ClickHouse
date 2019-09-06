#pragma once

#include <Storages/IStorage.h>

#include <Poco/File.h>
#include <Poco/Path.h>

#include <common/logger_useful.h>

#include <atomic>
#include <shared_mutex>
#include <ext/shared_ptr_helper.h>


namespace DB
{

class StorageFileBlockInputStream;
class StorageFileBlockOutputStream;

class StorageFile : public ext::shared_ptr_helper<StorageFile>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageFile>;
public:
    std::string getName() const override { return "File"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(
        const ASTPtr & query,
        const Context & context) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &) override;

    String getDataPath() const override;

protected:
    friend class StorageFileBlockInputStream;
    friend class StorageFileBlockOutputStream;

    /** there are three options (ordered by priority):
    - use specified file descriptor if (fd >= 0)
    - use specified table_path if it isn't empty
    - create own table inside data/db/table/
    */
    StorageFile(
        const std::string & table_path_,
        int table_fd_,
        const std::string & db_dir_path,
        const std::string & database_name_,
        const std::string & table_name_,
        const std::string & format_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        Context & context_);

private:
    std::string table_name;
    std::string database_name;
    std::string format_name;
    Context & context_global;

    int table_fd = -1;

    std::vector<std::string> paths;

    bool is_db_table = true;                     /// Table is stored in real database, not user's file
    bool use_table_fd = false;                    /// Use table_fd instead of path
    std::atomic<bool> table_fd_was_used{false}; /// To detect repeating reads from stdin
    off_t table_fd_init_offset = -1;            /// Initial position of fd, used for repeating reads

    mutable std::shared_mutex rwlock;

    Logger * log = &Logger::get("StorageFile");
};

}
