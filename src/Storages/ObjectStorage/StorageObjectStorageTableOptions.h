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

    /// Returns the path used for reading. Always initialized at construction time.
    const Path & getPathForRead() const;

    Path getPathForWrite(const Path & raw_path, const std::string & partition_id = "") const;

    static String computeSchemaHash(const ColumnsDescription & columns);

    using Paths = ObjectStorageConnectionConfiguration::Paths;

    void setSchemaHash(const String & hash, Paths & paths);

    void initPartitionStrategy(ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context, const Path & raw_path);

    /// Adjust `read_path` for Queue storage: ensure it ends with a glob pattern for polling.
    void adjustReadPathForQueue();

    StorageObjectStorageTableOptions(
        Path read_path_,
        String format_,
        String compression_method_,
        String structure_,
        PartitionStrategyFactory::StrategyType partition_strategy_type_,
        bool partition_columns_in_data_file_,
        std::shared_ptr<IPartitionStrategy> partition_strategy_)
        : format(std::move(format_))
        , compression_method(std::move(compression_method_))
        , structure(std::move(structure_))
        , partition_strategy_type(partition_strategy_type_)
        , partition_columns_in_data_file(partition_columns_in_data_file_)
        , partition_strategy(std::move(partition_strategy_))
        , read_path(std::move(read_path_))
    {
    }

    StorageObjectStorageTableOptions() = default;

private:
    /// Path used for reading. Initialized at construction time to `getRawPath()`,
    /// then may be overwritten by `initPartitionStrategy` with a Hive glob pattern.
    Path read_path;
};

struct StorageParsedArguments;

/// Create table options from parsed arguments and the initial read path.
StorageObjectStorageTableOptions tableOptionsFromParsedArguments(StorageParsedArguments && parsed_arguments, const ObjectStorageConnectionConfiguration::Path & read_path);

}
