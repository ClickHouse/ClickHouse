#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Storages/StorageS3Settings.h>
#include <Poco/URI.h>
#include <common/logger_useful.h>
#include <ext/shared_ptr_helper.h>

namespace Aws::S3
{
    class S3Client;
}

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
    StorageS3(const S3::URI & uri,
        const String & access_key_id,
        const String & secret_access_key,
        const StorageID & table_id_,
        const String & format_name_,
        UInt64 min_upload_part_size_,
        UInt64 max_single_part_upload_size_,
        UInt64 max_connections_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_,
        const String & compression_method_ = "");

    String getName() const override
    {
        return name;
    }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    NamesAndTypesList getVirtuals() const override;

private:
    const S3::URI uri;
    const String access_key_id;
    const String secret_access_key;
    const UInt64 max_connections;
    const Context & global_context;

    String format_name;
    size_t min_upload_part_size;
    size_t max_single_part_upload_size;
    String compression_method;
    std::shared_ptr<Aws::S3::S3Client> client;
    String name;
    S3AuthSettings auth_settings;

    void updateAuthSettings(const Context & context);
};

}

#endif
