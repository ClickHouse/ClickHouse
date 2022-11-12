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
    /// 1. Parses internal file structure of table.
    /// 2. Finds out parts with latest version.
    /// 3. Creates url for underlying StorageS3 enigne to handle reads.
    StorageHudi(
        const StorageS3Configuration & configuration_,
        const StorageID & table_id_,
        ColumnsDescription columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_);

    String getName() const override { return "Hudi"; }


    /// Reads latest version of Apache Hudi table
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

    /// Apache Hudi store parts of data in different files.
    /// Every part file has timestamp in it.
    /// Every partition(directory) in Apache Hudi has different versions of part.
    /// To find needed parts we need to find out latest part file for every partition.
    /// Part format is usually parquet, but can differ.
    static String generateQueryFromKeys(const std::vector<std::string> & keys, const String & format);

    StorageS3::S3Configuration base_configuration;
    std::shared_ptr<StorageS3> s3engine;
    Poco::Logger * log;
    String table_path;
};

}

#endif
