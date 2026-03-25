#pragma once

#include <Storages/IPartitionStrategy.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

namespace DB
{

/// Simple storage metadata that does not depend on the connection type.
/// Holds format, compression, structure, partitioning, and path information.
/// No derived classes — this is a plain value type.
struct StorageObjectStorageTableOptions
{
    using Path = ObjectStorageConnectionConfiguration::Path;

    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";

    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    /// Whether partition column values are contained in the actual data.
    /// The alternative is hive partitioning, when they are contained in file path.
    bool partition_columns_in_data_file = true;
    std::shared_ptr<IPartitionStrategy> partition_strategy;

    String schema_hash;

    const Path & getPathForRead(const Path & raw_path) const;
    void setPathForRead(const Path & path);
    Path getPathForWrite(const Path & raw_path, const std::string & partition_id = "") const;

    static String computeSchemaHash(const ColumnsDescription & columns);

    using Paths = ObjectStorageConnectionConfiguration::Paths;

    void setSchemaHash(const String & hash, Paths & paths);

    void initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context, const Path & raw_path);

private:
    /// Path used for reading, by default it is the same as raw_path.
    /// When using `partition_strategy=hive`, a recursive reading pattern will be appended.
    Path read_path;
};

struct StorageParsedArguments;

/// Convert common parsed arguments into table options.
StorageObjectStorageTableOptions tableOptionsFromParsedArguments(StorageParsedArguments && parsed_arguments);

}
