#pragma once

#include <memory>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>


namespace DB {
struct StorageParsableArguments {

    using Paths = StorageObjectStorageConfiguration::Paths;
    using Path = StorageObjectStorageConfiguration::Path;
    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    bool partition_columns_in_data_file = true;
    std::shared_ptr<IPartitionStrategy> partition_strategy;
};
}
