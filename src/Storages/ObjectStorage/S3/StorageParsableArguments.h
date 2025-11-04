
// #pragma once

// #include "config.h"

// #if USE_AWS_S3
// #include <IO/S3Settings.h>
// #include <Storages/ObjectStorage/StorageObjectStorage.h>
// #include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
// #include <Parsers/IAST_fwd.h>
// #include <Disks/ObjectStorages/IObjectStorage.h>

// namespace DB
// {


// class S3StorageParsableArguments
// {
//     String format = "auto";
//     String compression_method = "auto";
//     String structure = "auto";
//     PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
//     bool partition_columns_in_data_file = true;
//     std::shared_ptr<IPartitionStrategy> partition_strategy;
//     S3::URI url;
//     std::unique_ptr<S3Settings> s3_settings;
//     std::unique_ptr<S3Capabilities> s3_capabilities;
//     HTTPHeaderEntries headers_from_ast;

// public:
//     S3StorageParsableArguments() = default;

//     void fromNamedCollection(const NamedCollection & collection, ContextPtr context);
//     void fromAST(ASTs & args, ContextPtr context, bool with_structure);
//     void fromDisk(const String & disk_name, ASTs & args, ContextPtr context, bool with_structure);
// };

// }

// #endif
