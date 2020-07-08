#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <IO/COSCommon.h>
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
class StorageCOS final : public ext::shared_ptr_helper<StorageCOS>, public IStorage
{
public:
    StorageCOS(const COS::URI & uri,
        const String & access_key_id,
        const String & secret_access_key,
        const StorageID & table_id_,
        const String & format_name_,
        UInt64 min_upload_part_size_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        Context & context_,
        const String & compression_method_);

    String getName() const override
    {
        return "COS";
    }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    NamesAndTypesList getVirtuals() const override;

private:
    COS::URI uri;
    const Context & context_global;

    String format_name;
    UInt64 min_upload_part_size;
    String compression_method;
    std::shared_ptr<Aws::S3::S3Client> client;
};

}

#endif
