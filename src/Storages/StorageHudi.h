#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <Storages/IStorage.h>
#    include <Storages/StorageS3.h>

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
        const StorageS3Configuration & configuration_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_);

    String getName() const override { return "Hudi"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    std::vector<std::string> getKeysFromS3();
    static std::string generateQueryFromKeys(std::vector<std::string> && keys, String format);

    StorageS3::S3Configuration base_configuration;
    std::shared_ptr<StorageS3> s3engine;
    Poco::Logger * log;
    String table_path;
};

}

#endif
