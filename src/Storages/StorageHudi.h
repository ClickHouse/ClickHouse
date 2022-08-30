#pragma once

#include "config_core.h"

#include <Storages/IStorage.h>
#include <Storages/StorageS3.h>

namespace Poco
{
class Logger;
}

namespace Aws::S3
{
class S3Client;
}

namespace DB
{

class StorageHudi : public IStorage
{
public:
    StorageHudi(
        const S3::URI & uri_,
        const String & access_key_,
        const String & secret_access_key_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_);

    String getName() const override { return "Hudi"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    static void updateS3Configuration(ContextPtr, StorageS3::S3Configuration &);

private:
    std::vector<std::string> getKeysFromS3();
    std::string generateQueryFromKeys(std::vector<std::string> && keys);

private:
    StorageS3::S3Configuration base_configuration;
    std::shared_ptr<StorageS3> s3engine;
    Poco::Logger * log;
    String table_path;
};

}
