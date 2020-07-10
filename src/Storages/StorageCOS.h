#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Storages/IStorage.h>
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
 * This class represents table engine for external COS urls.
 * It sends HTTP GET to server when select is called and
 * HTTP PUT when insert is called.
 */
class StorageCOS final : public StorageS3
{
public:
    StorageCOS(const S3::URI & uri,
        const String & access_key_id,
        const String & secret_access_key,
        const StorageID & table_id_,
        const String & format_name_,
        UInt64 min_upload_part_size_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        Context & context_,
        const String & compression_method_)
    : StorageS3(uri,
    access_key_id,
    secret_access_key,
    table_id_,
    format_name_,
    min_upload_part_size_,
    columns_,
    constraints_,
    context_,
    compression_method_)
    {
    }

    String getName() const override
    {
        return "COSN";
    }
};

}

#endif
