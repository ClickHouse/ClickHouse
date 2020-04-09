#pragma once

#include <Storages/IStorage.h>
#include <Poco/URI.h>
#include <common/logger_useful.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{
/**
 * This class represents table engine for external S3 urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageS3 : public ext::shared_ptr_helper<StorageS3>, public IStorage
{
public:
    StorageS3(
        const Poco::URI & uri_,
        const std::string & database_name_,
        const std::string & table_name_,
        const String & format_name_,
        UInt64 min_upload_part_size_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        Context & context_);

    String getName() const override
    {
        return "S3";
    }

    Block getHeaderBlock(const Names & /*column_names*/) const
    {
        return getSampleBlock();
    }

    String getTableName() const override
    {
        return table_name;
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &) override;

private:
    Poco::URI uri;
    const Context & context_global;

    String format_name;
    String database_name;
    String table_name;
    UInt64 min_upload_part_size;
};

}
